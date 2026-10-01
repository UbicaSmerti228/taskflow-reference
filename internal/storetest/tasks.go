// Package storetest содержит общие наборы тестов для хранилищ.
// Один и тот же набор проходят хранилище в памяти и настоящее (PostgreSQL, Redis):
// поэтому юнит-тестам на хранилищах в памяти можно доверять.
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// TaskStore — полный набор методов хранилища задач.
type TaskStore interface {
	Create(ctx context.Context, userID int64, in task.NewTask) (task.Task, error)
	Get(ctx context.Context, userID, id int64) (task.Task, error)
	List(ctx context.Context, userID int64, f task.Filter) ([]task.Task, int, error)
	Update(ctx context.Context, userID, id int64, p task.Patch) (task.Task, error)
	Delete(ctx context.Context, userID, id int64) error
	DueForReminder(ctx context.Context, now time.Time) ([]task.Task, error)
	MarkReminded(ctx context.Context, id int64, at time.Time) error
}

func ptr[T any](v T) *T { return &v }

// RunTaskSuite проверяет поведение хранилища задач. newStore возвращает пустое хранилище
// и id двух существующих пользователей.
func RunTaskSuite(t *testing.T, newStore func(t *testing.T) (s TaskStore, alice, bob int64)) {
	ctx := context.Background()

	seed := func(t *testing.T, titles ...string) (TaskStore, int64, int64) {
		t.Helper()
		s, alice, bob := newStore(t)
		for _, title := range titles {
			if _, err := s.Create(ctx, alice, task.NewTask{Title: title}); err != nil {
				t.Fatal(err)
			}
		}
		return s, alice, bob
	}

	t.Run("Create и Get", func(t *testing.T) {
		due := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
		s, alice, _ := newStore(t)
		created, err := s.Create(ctx, alice, task.NewTask{Title: "отчёт", DueAt: &due})
		if err != nil {
			t.Fatal(err)
		}
		if created.ID != 1 || created.Title != "отчёт" || created.Done || created.CreatedAt.IsZero() {
			t.Errorf("Create() = %+v, want id 1, заголовок, done = false и заполненное created_at", created)
		}
		got, err := s.Get(ctx, alice, created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.DueAt == nil || !got.DueAt.Equal(due) || got.RemindedAt != nil {
			t.Errorf("Get() = %+v, want срок %v и пустую отметку о напоминании", got, due)
		}
		if got.DueAt != nil && got.DueAt.Location() != time.UTC || got.CreatedAt.Location() != time.UTC {
			t.Errorf("время вернулось не в UTC: due_at %v, created_at %v", got.DueAt, got.CreatedAt)
		}
		if _, err := s.Get(ctx, alice, 9); !errors.Is(err, task.ErrNotFound) {
			t.Errorf("Get(9) error = %v, want ErrNotFound", err)
		}
	})

	// Чужие задачи не видны и не меняются: для другого пользователя их как будто нет.
	t.Run("задачи другого пользователя недоступны", func(t *testing.T) {
		s, alice, bob := seed(t, "задача Алисы")
		if _, err := s.Create(ctx, bob, task.NewTask{Title: "задача Боба"}); err != nil {
			t.Fatal(err)
		}

		if _, err := s.Get(ctx, bob, 1); !errors.Is(err, task.ErrNotFound) {
			t.Errorf("Get чужой задачи: error = %v, want ErrNotFound", err)
		}
		if _, err := s.Update(ctx, bob, 1, task.Patch{Done: ptr(true)}); !errors.Is(err, task.ErrNotFound) {
			t.Errorf("Update чужой задачи: error = %v, want ErrNotFound", err)
		}
		if err := s.Delete(ctx, bob, 1); !errors.Is(err, task.ErrNotFound) {
			t.Errorf("Delete чужой задачи: error = %v, want ErrNotFound", err)
		}
		got, total, err := s.List(ctx, bob, task.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		if total != 1 || len(got) != 1 || got[0].Title != "задача Боба" {
			t.Errorf("List Боба = %+v (total %d), want только его задачу", got, total)
		}
		if untouched, err := s.Get(ctx, alice, 1); err != nil || untouched.Done {
			t.Errorf("задача Алисы = %+v, %v; want нетронутую", untouched, err)
		}
	})

	t.Run("List", func(t *testing.T) {
		s, alice, _ := seed(t, "a", "b", "c", "d", "e")
		for _, id := range []int64{2, 4} {
			if _, err := s.Update(ctx, alice, id, task.Patch{Done: ptr(true)}); err != nil {
				t.Fatal(err)
			}
		}
		tests := []struct {
			name      string
			filter    task.Filter
			wantIDs   []int64
			wantTotal int
		}{
			{name: "без ограничений", filter: task.Filter{}, wantIDs: []int64{1, 2, 3, 4, 5}, wantTotal: 5},
			{name: "первая страница", filter: task.Filter{Limit: 2}, wantIDs: []int64{1, 2}, wantTotal: 5},
			{name: "вторая страница по смещению", filter: task.Filter{Limit: 2, Offset: 2}, wantIDs: []int64{3, 4}, wantTotal: 5},
			{name: "неполная последняя страница", filter: task.Filter{Limit: 2, Offset: 4}, wantIDs: []int64{5}, wantTotal: 5},
			{name: "смещение за концом", filter: task.Filter{Limit: 2, Offset: 50}, wantIDs: nil, wantTotal: 5},
			{name: "вторая страница по ключу", filter: task.Filter{Limit: 2, AfterID: 2}, wantIDs: []int64{3, 4}, wantTotal: 5},
			{name: "ключ за концом", filter: task.Filter{Limit: 2, AfterID: 5}, wantIDs: nil, wantTotal: 5},
			{name: "только выполненные", filter: task.Filter{Done: ptr(true)}, wantIDs: []int64{2, 4}, wantTotal: 2},
			{name: "невыполненные по ключу", filter: task.Filter{Done: ptr(false), Limit: 2, AfterID: 1}, wantIDs: []int64{3, 5}, wantTotal: 3},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, total, err := s.List(ctx, alice, tt.filter)
				if err != nil {
					t.Fatal(err)
				}
				if total != tt.wantTotal {
					t.Errorf("total = %d, want %d", total, tt.wantTotal)
				}
				if got == nil {
					t.Error("List() вернул nil, want пустой слайс: в JSON он должен стать [], а не null")
				}
				if len(got) != len(tt.wantIDs) {
					t.Fatalf("len = %d, want %d: %+v", len(got), len(tt.wantIDs), got)
				}
				for i, tk := range got {
					if tk.ID != tt.wantIDs[i] {
						t.Errorf("got[%d].ID = %d, want %d", i, tk.ID, tt.wantIDs[i])
					}
				}
			})
		}
	})

	t.Run("Update", func(t *testing.T) {
		due := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
		tests := []struct {
			name    string
			id      int64
			patch   task.Patch
			wantErr error
			check   func(t *testing.T, got task.Task)
		}{
			{name: "заголовок", id: 1, patch: task.Patch{Title: ptr("новый")}, check: func(t *testing.T, got task.Task) {
				if got.Title != "новый" || got.Done {
					t.Errorf("got %+v, want заголовок «новый» и done = false", got)
				}
			}},
			{name: "выполнение", id: 1, patch: task.Patch{Done: ptr(true)}, check: func(t *testing.T, got task.Task) {
				if !got.Done || got.Title != "задача" {
					t.Errorf("got %+v, want done = true и прежний заголовок", got)
				}
			}},
			{name: "срок", id: 1, patch: task.Patch{DueAt: &due}, check: func(t *testing.T, got task.Task) {
				if got.DueAt == nil || !got.DueAt.Equal(due) {
					t.Errorf("DueAt = %v, want %v", got.DueAt, due)
				}
			}},
			{name: "неизвестный id", id: 9, patch: task.Patch{Done: ptr(true)}, wantErr: task.ErrNotFound},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				s, alice, _ := seed(t, "задача")
				got, err := s.Update(ctx, alice, tt.id, tt.patch)
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Update() error = %v, want %v", err, tt.wantErr)
				}
				if tt.check == nil {
					return
				}
				tt.check(t, got)
				saved, err := s.Get(ctx, alice, tt.id) // изменение должно сохраниться, а не только вернуться
				if err != nil {
					t.Fatal(err)
				}
				tt.check(t, saved)
			})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		s, alice, _ := seed(t, "первая", "вторая")
		if err := s.Delete(ctx, alice, 1); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, alice, 1); !errors.Is(err, task.ErrNotFound) {
			t.Errorf("повторный Delete(1) error = %v, want ErrNotFound", err)
		}
		got, total, err := s.List(ctx, alice, task.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		if total != 1 || len(got) != 1 || got[0].ID != 2 {
			t.Errorf("после удаления остались %+v, want одну задачу с id 2", got)
		}
		created, err := s.Create(ctx, alice, task.NewTask{Title: "третья"}) // удалённый id не выдаётся повторно
		if err != nil {
			t.Fatal(err)
		}
		if created.ID != 3 {
			t.Errorf("новая задача получила id %d, want 3", created.ID)
		}
	})

	t.Run("Reminders", func(t *testing.T) {
		now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		past, future := now.Add(-time.Minute), now.Add(time.Minute)
		s, alice, bob := newStore(t)
		for i, in := range []task.NewTask{
			{Title: "без срока"},
			{Title: "просрочена", DueAt: &past},
			{Title: "срок ровно сейчас", DueAt: &now},
			{Title: "срок в будущем", DueAt: &future},
			{Title: "просрочена, но выполнена", DueAt: &past},
		} {
			owner := alice
			if i == 2 {
				owner = bob // воркер видит задачи всех пользователей
			}
			if _, err := s.Create(ctx, owner, in); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Update(ctx, alice, 5, task.Patch{Done: ptr(true)}); err != nil {
			t.Fatal(err)
		}

		dueIDs := func() map[int64]bool {
			t.Helper()
			due, err := s.DueForReminder(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			ids := map[int64]bool{}
			for _, tk := range due {
				ids[tk.ID] = true
			}
			return ids
		}

		if got := dueIDs(); len(got) != 2 || !got[2] || !got[3] {
			t.Fatalf("DueForReminder() ids = %v, want 2 и 3", got)
		}
		if err := s.MarkReminded(ctx, 2, now); err != nil {
			t.Fatal(err)
		}
		if got := dueIDs(); len(got) != 1 || !got[3] {
			t.Errorf("после MarkReminded(2) ids = %v, want только 3", got)
		}
		if saved, err := s.Get(ctx, alice, 2); err != nil || saved.RemindedAt == nil || !saved.RemindedAt.Equal(now) {
			t.Errorf("Get(2) = %+v, %v; want отметку о напоминании %v", saved, err, now)
		}

		// Новый срок сбрасывает отметку: о задаче нужно напомнить ещё раз.
		if _, err := s.Update(ctx, alice, 2, task.Patch{DueAt: &past}); err != nil {
			t.Fatal(err)
		}
		if got := dueIDs(); len(got) != 2 {
			t.Errorf("после переноса срока ids = %v, want 2 и 3", got)
		}
		// Изменение других полей отметку не трогает.
		if err := s.MarkReminded(ctx, 2, now); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Update(ctx, alice, 2, task.Patch{Title: ptr("переименована")}); err != nil {
			t.Fatal(err)
		}
		if got := dueIDs(); len(got) != 1 {
			t.Errorf("после переименования ids = %v, want только 3", got)
		}
		if err := s.MarkReminded(ctx, 9, now); !errors.Is(err, task.ErrNotFound) {
			t.Errorf("MarkReminded(9) error = %v, want ErrNotFound", err)
		}
	})
}
