package notifier

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
	"github.com/UbicaSmerti228/taskflow-reference/internal/memory"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

var ctx = context.Background()

// results — метрики, которые можно прочитать в тесте: тип события и исход → сколько раз.
type results struct {
	mu sync.Mutex
	n  map[string]int
}

func (r *results) Event(eventType, result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.n == nil {
		r.n = map[string]int{}
	}
	r.n[eventType+"/"+result]++
}

func (r *results) count(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n[key]
}

func newService(t *testing.T) (*Service, *memory.Notifications, *results) {
	t.Helper()
	store, m := memory.NewNotifications(), &results{}
	return New(store, NewHub(), slog.New(slog.DiscardHandler), m), store, m
}

func payload(t *testing.T, e event.Event) []byte {
	t.Helper()
	rec, err := e.Record()
	if err != nil {
		t.Fatal(err)
	}
	return rec.Payload
}

var (
	due     = time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	created = time.Date(2026, 4, 30, 9, 0, 0, 0, time.UTC)
	sample  = task.Task{ID: 7, Title: "сдать отчёт", DueAt: &due, CreatedAt: created}
)

func TestHandleSavesNotification(t *testing.T) {
	svc, _, m := newService(t)
	if err := svc.Handle(ctx, payload(t, event.TaskCreated(3, sample))); err != nil {
		t.Fatal(err)
	}
	if err := svc.Handle(ctx, payload(t, event.TaskDue(3, sample, due))); err != nil {
		t.Fatal(err)
	}

	got, err := svc.List(ctx, 3, 0, 0)
	if err != nil || len(got) != 2 {
		t.Fatalf("List() = %+v, %v; want два уведомления", got, err)
	}
	// Новые первыми: сначала напоминание, потом создание.
	if got[0].Kind != notification.KindTaskDue || got[1].Kind != notification.KindTaskCreated {
		t.Errorf("виды уведомлений %s, %s", got[0].Kind, got[1].Kind)
	}
	if n := got[1]; n.UserID != 3 || n.TaskID != 7 || n.Title != "сдать отчёт" || !n.CreatedAt.Equal(created) {
		t.Errorf("уведомление = %+v", n)
	}
	if m.count("task.created/saved") != 1 || m.count("task.due/saved") != 1 {
		t.Errorf("метрики = %v", m.n)
	}
}

// Kafka доставила событие трижды — уведомление одно.
func TestHandleIsIdempotent(t *testing.T) {
	svc, _, m := newService(t)
	p := payload(t, event.TaskCreated(3, sample))
	for range 3 {
		if err := svc.Handle(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := svc.List(ctx, 3, 0, 0); len(got) != 1 {
		t.Errorf("уведомлений %d, want 1", len(got))
	}
	if m.count("task.created/saved") != 1 || m.count("task.created/duplicate") != 2 {
		t.Errorf("метрики = %v, want одно сохранение и два дубликата", m.n)
	}
}

func TestHandleErrors(t *testing.T) {
	svc, store, m := newService(t)

	// Битое событие: ошибка особого вида, по которой читатель Kafka пропустит сообщение.
	if err := svc.Handle(ctx, []byte(`{не json`)); !errors.Is(err, ErrMalformed) {
		t.Errorf("битое событие: error = %v, want ErrMalformed", err)
	}
	// Незнакомый тип — не ошибка: сообщение подтверждается и пропускается.
	if err := svc.Handle(ctx, []byte(`{"event_id":"x","type":"task.archived","task_id":1,"user_id":1}`)); err != nil {
		t.Errorf("незнакомый тип: error = %v, want nil", err)
	}
	// Сбой хранилища: обычная ошибка, сообщение будет подано снова.
	down := errors.New("database is down")
	store.Err = down
	err := svc.Handle(ctx, payload(t, event.TaskCreated(3, sample)))
	if !errors.Is(err, down) || errors.Is(err, ErrMalformed) {
		t.Errorf("сбой хранилища: error = %v, want ошибку хранилища", err)
	}
	store.Err = nil
	// После восстановления то же событие обрабатывается.
	if err := svc.Handle(ctx, payload(t, event.TaskCreated(3, sample))); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"unknown/malformed", "unknown/ignored", "task.created/failed", "task.created/saved"} {
		if m.count(key) != 1 {
			t.Errorf("метрика %s = %d, want 1", key, m.count(key))
		}
	}
}

func TestListValidates(t *testing.T) {
	svc, _, _ := newService(t)
	tests := []struct {
		name           string
		userID         int64
		limit          int
		beforeID       int64
		wantBadRequest bool
	}{
		{name: "обычный запрос", userID: 1, limit: 10},
		{name: "размер по умолчанию", userID: 1, limit: 0},
		{name: "наибольший размер", userID: 1, limit: notification.MaxLimit},
		{name: "нет пользователя", userID: 0, limit: 10, wantBadRequest: true},
		{name: "отрицательный размер", userID: 1, limit: -1, wantBadRequest: true},
		{name: "слишком большой размер", userID: 1, limit: notification.MaxLimit + 1, wantBadRequest: true},
		{name: "отрицательный before_id", userID: 1, limit: 10, beforeID: -5, wantBadRequest: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.List(ctx, tt.userID, tt.limit, tt.beforeID)
			if errors.Is(err, ErrBadRequest) != tt.wantBadRequest {
				t.Errorf("List() error = %v, want ErrBadRequest: %v", err, tt.wantBadRequest)
			}
		})
	}
}

// По умолчанию отдаётся не больше DefaultLimit уведомлений.
func TestListDefaultLimit(t *testing.T) {
	svc, _, _ := newService(t)
	for id := int64(1); id <= notification.DefaultLimit+5; id++ {
		if err := svc.Handle(ctx, payload(t, event.TaskCreated(1, task.Task{ID: id, Title: "задача"}))); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := svc.List(ctx, 1, 0, 0); len(got) != notification.DefaultLimit {
		t.Errorf("List() вернул %d уведомлений, want %d", len(got), notification.DefaultLimit)
	}
}

func TestSubscribeReceivesOwnNotifications(t *testing.T) {
	svc, _, _ := newService(t)
	if _, _, err := svc.Subscribe(0); !errors.Is(err, ErrBadRequest) {
		t.Errorf("Subscribe(0) error = %v, want ErrBadRequest", err)
	}
	mine, cancelMine, err := svc.Subscribe(3)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelMine()
	other, cancelOther, _ := svc.Subscribe(4)
	defer cancelOther()

	p := payload(t, event.TaskCreated(3, sample))
	for range 2 { // дубликат подписчикам не уходит
		if err := svc.Handle(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case n := <-mine:
		if n.TaskID != 7 || n.ID == 0 {
			t.Errorf("подписчик получил %+v", n)
		}
	default:
		t.Fatal("подписчик не получил уведомление")
	}
	if len(mine) != 0 {
		t.Error("дубликат события дошёл до подписчика")
	}
	if len(other) != 0 {
		t.Error("чужое уведомление дошло до другого пользователя")
	}
}

func TestHub(t *testing.T) {
	h := NewHub()
	a, cancelA := h.Subscribe(1)
	b, cancelB := h.Subscribe(1)
	if h.Subscribers() != 2 {
		t.Fatalf("подписчиков %d, want 2", h.Subscribers())
	}

	h.Publish(notification.Notification{ID: 1, UserID: 1})
	if (<-a).ID != 1 || (<-b).ID != 1 {
		t.Error("уведомление дошло не до всех подписчиков пользователя")
	}

	// Отписка закрывает канал; повторная отписка безопасна.
	cancelA()
	cancelA()
	if _, open := <-a; open {
		t.Error("после отписки канал не закрыт")
	}

	// Подписчик b не читает: буфер заполняется, и Hub отключает его, не блокируясь.
	for i := range subscriberBuffer + 1 {
		h.Publish(notification.Notification{ID: int64(i), UserID: 1})
	}
	if h.Subscribers() != 0 {
		t.Errorf("медленный подписчик не отключён: подписчиков %d", h.Subscribers())
	}
	read := 0
	for range b {
		read++
	}
	if read != subscriberBuffer {
		t.Errorf("медленный подписчик дочитал %d уведомлений, want %d", read, subscriberBuffer)
	}
	cancelB() // отписка после отключения тоже безопасна

	h.Publish(notification.Notification{UserID: 99}) // подписчиков нет — ничего не происходит
}

func TestHubConcurrent(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			ch, cancel := h.Subscribe(int64(i % 3))
			h.Publish(notification.Notification{UserID: int64(i % 3)})
			select {
			case <-ch:
			case <-time.After(time.Second):
			}
			cancel()
		})
	}
	wg.Wait()
	if h.Subscribers() != 0 {
		t.Errorf("после отписки осталось %d подписчиков", h.Subscribers())
	}
}

func TestNewWithoutMetrics(t *testing.T) {
	svc := New(memory.NewNotifications(), NewHub(), slog.New(slog.DiscardHandler), nil)
	if err := svc.Handle(ctx, payload(t, event.TaskCreated(3, sample))); err != nil {
		t.Fatal(err)
	}
}
