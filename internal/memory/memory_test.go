package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/storetest"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

func TestTasks(t *testing.T) {
	storetest.RunTaskSuite(t, func(*testing.T) (storetest.TaskStore, int64, int64) { return NewTasks(), 1, 2 })
}

func TestOutbox(t *testing.T) {
	storetest.RunOutboxSuite(t, func(*testing.T) (storetest.TaskStore, storetest.Outbox, int64) {
		tasks := NewTasks()
		return tasks, tasks.Outbox(), 1
	})
}

func TestNotifications(t *testing.T) {
	storetest.RunNotificationSuite(t, func(*testing.T) storetest.NotificationStore { return NewNotifications() })
}

func TestUsers(t *testing.T) {
	storetest.RunUserSuite(t, func(*testing.T) storetest.UserStore { return NewUsers() })
}

func TestKV(t *testing.T) {
	storetest.RunKVSuite(t, func(*testing.T) (storetest.KV, func(time.Duration)) {
		kv := NewKV()
		return kv, kv.Advance
	})
}

func TestKVLen(t *testing.T) {
	ctx, kv := context.Background(), NewKV()
	_ = kv.Set(ctx, "a", nil, time.Minute)
	_ = kv.SaveRefresh(ctx, "h", 1, time.Hour)
	if kv.Len() != 2 {
		t.Errorf("Len() = %d, want 2", kv.Len())
	}
	kv.Advance(2 * time.Minute)
	if kv.Len() != 1 {
		t.Errorf("после истечения срока Len() = %d, want 1", kv.Len())
	}
}

// Поле Err заставляет каждый метод вернуть ошибку: так тесты сервисов проверяют отказы хранилищ.
func TestErr(t *testing.T) {
	ctx, boom := context.Background(), errors.New("boom")
	tasks, users, kv := NewTasks(), NewUsers(), NewKV()
	tasks.Err, users.Err, kv.Err = boom, boom, boom

	calls := map[string]func() error{
		"Tasks.Create":         func() error { _, err := tasks.Create(ctx, 1, task.NewTask{Title: "a"}); return err },
		"Tasks.Get":            func() error { _, err := tasks.Get(ctx, 1, 1); return err },
		"Tasks.List":           func() error { _, _, err := tasks.List(ctx, 1, task.Filter{}); return err },
		"Tasks.Update":         func() error { _, err := tasks.Update(ctx, 1, 1, task.Patch{}); return err },
		"Tasks.Delete":         func() error { return tasks.Delete(ctx, 1, 1) },
		"Tasks.DueForReminder": func() error { _, err := tasks.DueForReminder(ctx, time.Now()); return err },
		"Tasks.MarkReminded":   func() error { return tasks.MarkReminded(ctx, 1, time.Now()) },
		"Users.Create":         func() error { _, err := users.Create(ctx, "a@b.c", "h"); return err },
		"Users.ByEmail":        func() error { _, err := users.ByEmail(ctx, "a@b.c"); return err },
		"KV.Get":               func() error { _, _, err := kv.Get(ctx, "k"); return err },
		"KV.Set":               func() error { return kv.Set(ctx, "k", nil, time.Minute) },
		"KV.Del":               func() error { return kv.Del(ctx, "k") },
		"KV.SaveRefresh":       func() error { return kv.SaveRefresh(ctx, "h", 1, time.Minute) },
		"KV.TakeRefresh":       func() error { _, _, err := kv.TakeRefresh(ctx, "h"); return err },
		"KV.DeleteRefresh":     func() error { return kv.DeleteRefresh(ctx, "h") },
		"KV.Allow":             func() error { _, err := kv.Allow(ctx, "k", 1, time.Minute); return err },
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, boom) {
			t.Errorf("%s error = %v, want %v", name, err, boom)
		}
	}
}
