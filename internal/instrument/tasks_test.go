package instrument

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/memory"
	"github.com/UbicaSmerti228/taskflow-reference/internal/service"
	"github.com/UbicaSmerti228/taskflow-reference/internal/storetest"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// Обёртка не меняет поведение хранилища: она проходит тот же набор тестов, что и оно само.
func TestTasksBehavesLikeStore(t *testing.T) {
	storetest.RunTaskSuite(t, func(*testing.T) (storetest.TaskStore, int64, int64) {
		inner := memory.NewTasks()
		// Методы воркера напоминаний обёртка не трогает: они идут в хранилище напрямую.
		return wrapped{Tasks(inner, slog.New(slog.DiscardHandler), time.Second), inner}, 1, 2
	})
}

type wrapped struct {
	service.TaskRepo
	reminders *memory.Tasks
}

func (w wrapped) DueForReminder(ctx context.Context, now time.Time) ([]task.Task, error) {
	return w.reminders.DueForReminder(ctx, now)
}

func (w wrapped) MarkReminded(ctx context.Context, id int64, at time.Time) error {
	return w.reminders.MarkReminded(ctx, id, at)
}

// brokenRepo отвечает заданной ошибкой и двигает часы на заданное время.
type brokenRepo struct {
	*memory.Tasks
	err   error
	took  time.Duration
	clock *time.Time
}

func (r *brokenRepo) Get(ctx context.Context, userID, id int64) (task.Task, error) {
	*r.clock = r.clock.Add(r.took)
	if r.err != nil {
		return task.Task{}, r.err
	}
	return r.Tasks.Get(ctx, userID, id)
}

func TestTasksLogs(t *testing.T) {
	tests := []struct {
		name string
		err  error
		took time.Duration
		want string
	}{
		{name: "обычный вызов", took: 5 * time.Millisecond, want: `"level":"DEBUG","msg":"repository call","op":"tasks.get","duration_ms":5`},
		{name: "медленный вызов", took: 250 * time.Millisecond, want: `"level":"WARN","msg":"slow repository call","op":"tasks.get","duration_ms":250`},
		{name: "сбой хранилища", err: errors.New("connection refused"), want: `"level":"ERROR","msg":"repository call failed","op":"tasks.get","duration_ms":0,"err":"connection refused"`},
		{name: "не найдено — не сбой", err: task.ErrNotFound, want: `"level":"DEBUG"`},
		{name: "отмена клиентом — не сбой", err: context.Canceled, want: `"level":"DEBUG"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			inner := &brokenRepo{Tasks: memory.NewTasks(), err: tt.err, took: tt.took, clock: &now}
			created, err := inner.Create(context.Background(), 1, task.NewTask{Title: "задача"})
			if err != nil {
				t.Fatal(err)
			}

			repo := Tasks(inner, logging.New(&logs, slog.LevelDebug), 200*time.Millisecond).(*tasks)
			repo.now = func() time.Time { return now }

			ctx := logging.WithRequestID(context.Background(), "req-1")
			if _, err := repo.Get(ctx, 1, created.ID); !errors.Is(err, tt.err) {
				t.Fatalf("Get() error = %v, want %v", err, tt.err)
			}
			out := logs.String()
			if !strings.Contains(out, tt.want) {
				t.Errorf("в логе нет %s\n%s", tt.want, out)
			}
			// Запись связана с запросом: по request_id видно, какой запрос был медленным.
			if !strings.Contains(out, `"request_id":"req-1"`) {
				t.Errorf("в записи нет request_id\n%s", out)
			}
		})
	}
}
