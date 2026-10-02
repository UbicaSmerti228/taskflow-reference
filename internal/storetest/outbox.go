package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// Outbox — методы хранилища событий.
type Outbox interface {
	Drain(ctx context.Context, limit int, publish func(context.Context, []event.Record) error) (int, error)
	Pending(ctx context.Context) (int, error)
}

// RunOutboxSuite проверяет, что хранилище задач записывает события вместе с изменениями,
// а хранилище событий отдаёт их по порядку и ровно столько, сколько опубликовано.
func RunOutboxSuite(t *testing.T, newStore func(t *testing.T) (tasks TaskStore, outbox Outbox, alice int64)) {
	ctx := context.Background()

	// collect возвращает publish, который складывает полученные события в срез.
	collect := func(got *[]event.Event) func(context.Context, []event.Record) error {
		return func(_ context.Context, batch []event.Record) error {
			for _, rec := range batch {
				e, err := event.Decode(rec.Payload)
				if err != nil {
					return err
				}
				if rec.EventID != e.ID || rec.Topic != event.Topic || rec.Key != e.Key() {
					return errors.New("запись outbox расходится с событием внутри неё")
				}
				*got = append(*got, e)
			}
			return nil
		}
	}
	pending := func(t *testing.T, o Outbox, want int) {
		t.Helper()
		if n, err := o.Pending(ctx); err != nil || n != want {
			t.Errorf("Pending() = %d, %v; want %d", n, err, want)
		}
	}

	t.Run("создание задачи записывает событие", func(t *testing.T) {
		tasks, outbox, alice := newStore(t)
		pending(t, outbox, 0)

		due := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
		created, err := tasks.Create(ctx, alice, task.NewTask{Title: "отчёт", DueAt: &due})
		if err != nil {
			t.Fatal(err)
		}
		pending(t, outbox, 1)

		var got []event.Event
		if n, err := outbox.Drain(ctx, 10, collect(&got)); err != nil || n != 1 {
			t.Fatalf("Drain() = %d, %v; want 1", n, err)
		}
		e := got[0]
		if e.ID != "task.created:1" || e.Type != event.TypeTaskCreated || e.TaskID != created.ID || e.UserID != alice ||
			e.Title != "отчёт" || e.DueAt == nil || !e.DueAt.Equal(due) || !e.OccurredAt.Equal(created.CreatedAt) {
			t.Errorf("событие = %+v", e)
		}
		pending(t, outbox, 0)

		// Опубликованное событие второй раз не отдаётся.
		if n, err := outbox.Drain(ctx, 10, collect(&got)); err != nil || n != 0 || len(got) != 1 {
			t.Errorf("повторный Drain() = %d, %v, всего событий %d; want 0 и 1", n, err, len(got))
		}
	})

	t.Run("события отдаются по порядку и не больше limit", func(t *testing.T) {
		tasks, outbox, alice := newStore(t)
		for _, title := range []string{"a", "b", "c", "d", "e"} {
			if _, err := tasks.Create(ctx, alice, task.NewTask{Title: title}); err != nil {
				t.Fatal(err)
			}
		}
		var got []event.Event
		for _, want := range []int{2, 2, 1, 0} {
			if n, err := outbox.Drain(ctx, 2, collect(&got)); err != nil || n != want {
				t.Fatalf("Drain(2) = %d, %v; want %d", n, err, want)
			}
		}
		for i, e := range got {
			if e.TaskID != int64(i+1) {
				t.Fatalf("порядок событий нарушен: на месте %d задача %d", i, e.TaskID)
			}
		}
	})

	t.Run("неудачная публикация оставляет события в очереди", func(t *testing.T) {
		tasks, outbox, alice := newStore(t)
		for _, title := range []string{"a", "b"} {
			if _, err := tasks.Create(ctx, alice, task.NewTask{Title: title}); err != nil {
				t.Fatal(err)
			}
		}
		down := errors.New("kafka is down")
		n, err := outbox.Drain(ctx, 10, func(context.Context, []event.Record) error { return down })
		if !errors.Is(err, down) || n != 0 {
			t.Fatalf("Drain() при отказе = %d, %v; want 0 и ошибку публикации", n, err)
		}
		pending(t, outbox, 2)

		var got []event.Event
		if n, err := outbox.Drain(ctx, 10, collect(&got)); err != nil || n != 2 {
			t.Errorf("Drain() после восстановления = %d, %v; want 2", n, err)
		}
	})

	t.Run("напоминание записывает событие один раз на срок", func(t *testing.T) {
		tasks, outbox, alice := newStore(t)
		due := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
		now := due.Add(time.Minute)
		if _, err := tasks.Create(ctx, alice, task.NewTask{Title: "отчёт", DueAt: &due}); err != nil {
			t.Fatal(err)
		}
		var got []event.Event
		if _, err := outbox.Drain(ctx, 10, collect(&got)); err != nil {
			t.Fatal(err)
		}

		// Отметка поставлена дважды (например, после сбоя): событие одно.
		for range 2 {
			if err := tasks.MarkReminded(ctx, 1, now); err != nil {
				t.Fatal(err)
			}
		}
		pending(t, outbox, 1)
		got = nil
		if _, err := outbox.Drain(ctx, 10, collect(&got)); err != nil || len(got) != 1 {
			t.Fatalf("Drain() вернул %d событий (%v), want 1", len(got), err)
		}
		e := got[0]
		if e.ID != "task.due:1:1777629600" || e.Type != event.TypeTaskDue || e.UserID != alice || e.TaskID != 1 ||
			e.Title != "отчёт" || e.DueAt == nil || !e.DueAt.Equal(due) || !e.OccurredAt.Equal(now) {
			t.Errorf("событие = %+v", e)
		}

		// Срок перенесён: о нём будет своё напоминание и своё событие.
		later := due.Add(24 * time.Hour)
		if _, err := tasks.Update(ctx, alice, 1, task.Patch{DueAt: &later}); err != nil {
			t.Fatal(err)
		}
		if err := tasks.MarkReminded(ctx, 1, later); err != nil {
			t.Fatal(err)
		}
		pending(t, outbox, 1)

		// У несуществующей задачи события нет.
		if err := tasks.MarkReminded(ctx, 99, now); !errors.Is(err, task.ErrNotFound) {
			t.Errorf("MarkReminded(99) error = %v, want ErrNotFound", err)
		}
		pending(t, outbox, 1)
	})

	// Два экземпляра сервиса не публикуют одновременно: иначе события ушли бы дважды или не по порядку.
	t.Run("публикует один, второй ждёт", func(t *testing.T) {
		tasks, outbox, alice := newStore(t)
		if _, err := tasks.Create(ctx, alice, task.NewTask{Title: "a"}); err != nil {
			t.Fatal(err)
		}

		started, release := make(chan struct{}), make(chan struct{})
		var (
			wg    sync.WaitGroup
			first int
		)
		wg.Go(func() {
			first, _ = outbox.Drain(ctx, 10, func(context.Context, []event.Record) error {
				close(started)
				<-release
				return nil
			})
		})
		<-started

		called := false
		n, err := outbox.Drain(ctx, 10, func(context.Context, []event.Record) error { called = true; return nil })
		if err != nil || n != 0 || called {
			t.Errorf("Drain() во время чужой публикации = %d, %v, publish вызван: %v; want 0, nil, false", n, err, called)
		}
		close(release)
		wg.Wait()
		if first != 1 {
			t.Errorf("первый Drain() опубликовал %d событий, want 1", first)
		}
		pending(t, outbox, 0)
	})
}
