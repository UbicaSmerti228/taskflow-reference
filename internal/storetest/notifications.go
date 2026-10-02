package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
)

// NotificationStore — методы хранилища уведомлений.
type NotificationStore interface {
	Save(ctx context.Context, eventID string, n notification.Notification) (notification.Notification, bool, error)
	List(ctx context.Context, userID int64, limit int, beforeID int64) ([]notification.Notification, error)
}

// RunNotificationSuite проверяет поведение хранилища уведомлений. newStore возвращает пустое хранилище.
func RunNotificationSuite(t *testing.T, newStore func(t *testing.T) NotificationStore) {
	ctx := context.Background()
	at := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	item := func(userID, taskID int64) notification.Notification {
		return notification.Notification{UserID: userID, Kind: notification.KindTaskCreated, TaskID: taskID, Title: "отчёт", CreatedAt: at}
	}

	t.Run("Save и List", func(t *testing.T) {
		s := newStore(t)
		saved, isNew, err := s.Save(ctx, "task.created:7", item(1, 7))
		if err != nil || !isNew {
			t.Fatalf("Save() = %+v, %v, %v; want новое уведомление", saved, isNew, err)
		}
		if saved.ID != 1 || saved.UserID != 1 || saved.Kind != notification.KindTaskCreated || saved.TaskID != 7 ||
			saved.Title != "отчёт" || !saved.CreatedAt.Equal(at) {
			t.Errorf("Save() = %+v", saved)
		}
		got, err := s.List(ctx, 1, 10, 0)
		if err != nil || len(got) != 1 || got[0] != saved {
			t.Errorf("List() = %+v, %v; want одно уведомление %+v", got, err, saved)
		}
		if got[0].CreatedAt.Location() != time.UTC {
			t.Errorf("время уведомления в поясе %v, want UTC", got[0].CreatedAt.Location())
		}
	})

	// Главное свойство: повторная доставка события не создаёт второе уведомление.
	t.Run("повторное событие не сохраняется", func(t *testing.T) {
		s := newStore(t)
		if _, isNew, err := s.Save(ctx, "task.created:7", item(1, 7)); err != nil || !isNew {
			t.Fatalf("первый Save(): %v, %v", isNew, err)
		}
		for range 3 {
			dup, isNew, err := s.Save(ctx, "task.created:7", item(1, 7))
			if err != nil || isNew || dup.ID != 0 {
				t.Fatalf("повторный Save() = %+v, %v, %v; want пустое уведомление и false", dup, isNew, err)
			}
		}
		// Другое событие о той же задаче — другое уведомление.
		if _, isNew, err := s.Save(ctx, "task.due:7:1777629600", item(1, 7)); err != nil || !isNew {
			t.Fatalf("Save() другого события: %v, %v", isNew, err)
		}
		if got, _ := s.List(ctx, 1, 10, 0); len(got) != 2 {
			t.Errorf("уведомлений %d, want 2", len(got))
		}
	})

	t.Run("List: свои уведомления, новые первыми, постранично", func(t *testing.T) {
		s := newStore(t)
		for task := int64(1); task <= 5; task++ {
			if _, _, err := s.Save(ctx, "a:"+string(rune('0'+task)), item(1, task)); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := s.Save(ctx, "b:1", item(2, 100)); err != nil { // чужое
			t.Fatal(err)
		}

		tasks := func(items []notification.Notification) []int64 {
			out := make([]int64, len(items))
			for i, n := range items {
				out[i] = n.TaskID
			}
			return out
		}
		same := func(a, b []int64) bool {
			if len(a) != len(b) {
				return false
			}
			for i := range a {
				if a[i] != b[i] {
					return false
				}
			}
			return true
		}

		page1, err := s.List(ctx, 1, 2, 0)
		if err != nil || !same(tasks(page1), []int64{5, 4}) {
			t.Fatalf("первая страница: задачи %v, %v; want 5, 4", tasks(page1), err)
		}
		page2, err := s.List(ctx, 1, 2, page1[1].ID)
		if err != nil || !same(tasks(page2), []int64{3, 2}) {
			t.Fatalf("вторая страница: задачи %v, %v; want 3, 2", tasks(page2), err)
		}
		page3, err := s.List(ctx, 1, 2, page2[1].ID)
		if err != nil || !same(tasks(page3), []int64{1}) {
			t.Fatalf("третья страница: задачи %v, %v; want 1", tasks(page3), err)
		}
		if all, _ := s.List(ctx, 1, 100, 0); len(all) != 5 {
			t.Errorf("у первого пользователя %d уведомлений, want 5: чужие попадать не должны", len(all))
		}
		empty, err := s.List(ctx, 3, 10, 0)
		if err != nil || empty == nil || len(empty) != 0 {
			t.Errorf("List() без уведомлений = %v, %v; want пустой срез, не nil", empty, err)
		}
	})
}
