package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/memory"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

var ctx = context.Background()

func ptr[T any](v T) *T { return &v }

// countingRepo считает обращения к хранилищу: так видно, ответил кэш или база.
type countingRepo struct {
	*memory.Tasks
	gets int
}

func (r *countingRepo) Get(ctx context.Context, userID, id int64) (task.Task, error) {
	r.gets++
	return r.Tasks.Get(ctx, userID, id)
}

func newTasks(t *testing.T) (*Tasks, *countingRepo, *memory.KV) {
	t.Helper()
	repo, kv := &countingRepo{Tasks: memory.NewTasks()}, memory.NewKV()
	return NewTasks(repo, kv, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil))), repo, kv
}

func TestTasksCreateValidates(t *testing.T) {
	tests := []struct {
		name    string
		title   string
		want    string
		wantErr error
	}{
		{name: "обычный заголовок", title: "купить молоко", want: "купить молоко"},
		{name: "пробелы обрезаются", title: "  позвонить  ", want: "позвонить"},
		{name: "пустой заголовок", title: "   ", wantErr: task.ErrEmptyTitle},
		{name: "слишком длинный заголовок", title: strings.Repeat("я", task.MaxTitleLen+1), wantErr: task.ErrTitleTooLong},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, repo, _ := newTasks(t)
			got, err := svc.Create(ctx, 1, task.NewTask{Title: tt.title})
			if !errors.Is(err, tt.wantErr) || got.Title != tt.want {
				t.Fatalf("Create(%q) = %q, %v; want %q, %v", tt.title, got.Title, err, tt.want, tt.wantErr)
			}
			if _, total, _ := repo.List(ctx, 1, task.Filter{}); tt.wantErr != nil && total != 0 {
				t.Error("невалидная задача дошла до хранилища")
			}
		})
	}
}

func TestTasksUpdateValidates(t *testing.T) {
	svc, _, _ := newTasks(t)
	if _, err := svc.Create(ctx, 1, task.NewTask{Title: "задача"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Update(ctx, 1, 1, task.Patch{}); !errors.Is(err, task.ErrEmptyPatch) {
		t.Errorf("пустой патч: error = %v, want ErrEmptyPatch", err)
	}
	if _, err := svc.Update(ctx, 1, 1, task.Patch{Title: ptr(" ")}); !errors.Is(err, task.ErrEmptyTitle) {
		t.Errorf("пустой заголовок: error = %v, want ErrEmptyTitle", err)
	}
	if _, err := svc.Update(ctx, 1, 9, task.Patch{Done: ptr(true)}); !errors.Is(err, task.ErrNotFound) {
		t.Errorf("неизвестная задача: error = %v, want ErrNotFound", err)
	}
	got, err := svc.Update(ctx, 1, 1, task.Patch{Title: ptr(" новый ")})
	if err != nil || got.Title != "новый" {
		t.Errorf("Update = %+v, %v; want заголовок «новый»", got, err)
	}
}

func TestTasksGetUsesCache(t *testing.T) {
	svc, repo, kv := newTasks(t)
	if _, err := svc.Create(ctx, 1, task.NewTask{Title: "задача"}); err != nil {
		t.Fatal(err)
	}

	for i := range 3 {
		got, err := svc.Get(ctx, 1, 1)
		if err != nil || got.Title != "задача" {
			t.Fatalf("Get #%d = %+v, %v", i+1, got, err)
		}
	}
	if repo.gets != 1 {
		t.Errorf("хранилище опрошено %d раз, want 1: второй и третий запрос должен обслужить кэш", repo.gets)
	}

	// Через минуту запись в кэше истекает — сервис снова идёт в хранилище.
	kv.Advance(time.Minute + time.Second)
	if _, err := svc.Get(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	if repo.gets != 2 {
		t.Errorf("после истечения срока хранилище опрошено %d раз, want 2", repo.gets)
	}
}

func TestTasksCacheIsInvalidated(t *testing.T) {
	svc, _, _ := newTasks(t)
	if _, err := svc.Create(ctx, 1, task.NewTask{Title: "старый заголовок"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(ctx, 1, 1); err != nil { // задача попала в кэш
		t.Fatal(err)
	}

	if _, err := svc.Update(ctx, 1, 1, task.Patch{Title: ptr("новый заголовок")}); err != nil {
		t.Fatal(err)
	}
	if got, err := svc.Get(ctx, 1, 1); err != nil || got.Title != "новый заголовок" {
		t.Errorf("после Update Get = %+v, %v; want новый заголовок, а не значение из кэша", got, err)
	}

	if err := svc.Delete(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(ctx, 1, 1); !errors.Is(err, task.ErrNotFound) {
		t.Errorf("после Delete Get error = %v, want ErrNotFound, а не задачу из кэша", err)
	}
	if err := svc.Delete(ctx, 1, 1); !errors.Is(err, task.ErrNotFound) {
		t.Errorf("повторный Delete error = %v, want ErrNotFound", err)
	}
}

// Задача одного пользователя не должна прийти другому даже из кэша.
func TestTasksCacheIsPerUser(t *testing.T) {
	svc, _, _ := newTasks(t)
	if _, err := svc.Create(ctx, 1, task.NewTask{Title: "секрет Алисы"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(ctx, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(ctx, 2, 1); !errors.Is(err, task.ErrNotFound) {
		t.Errorf("Get чужой задачи: error = %v, want ErrNotFound", err)
	}
}

// Сломанный кэш не ломает сервис: все операции продолжают работать через хранилище.
func TestTasksSurviveBrokenCache(t *testing.T) {
	svc, repo, kv := newTasks(t)
	kv.Err = errors.New("redis недоступен")

	if _, err := svc.Create(ctx, 1, task.NewTask{Title: "задача"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if got, err := svc.Get(ctx, 1, 1); err != nil || got.Title != "задача" {
			t.Fatalf("Get при сломанном кэше = %+v, %v", got, err)
		}
	}
	if repo.gets != 2 {
		t.Errorf("хранилище опрошено %d раз, want 2", repo.gets)
	}
	if _, err := svc.Update(ctx, 1, 1, task.Patch{Done: ptr(true)}); err != nil {
		t.Errorf("Update при сломанном кэше: %v", err)
	}
	if err := svc.Delete(ctx, 1, 1); err != nil {
		t.Errorf("Delete при сломанном кэше: %v", err)
	}
}

func TestTasksIgnoreBrokenCacheValue(t *testing.T) {
	svc, _, kv := newTasks(t)
	if _, err := svc.Create(ctx, 1, task.NewTask{Title: "задача"}); err != nil {
		t.Fatal(err)
	}
	if err := kv.Set(ctx, taskKey(1, 1), []byte("{не json"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if got, err := svc.Get(ctx, 1, 1); err != nil || got.Title != "задача" {
		t.Errorf("Get с мусором в кэше = %+v, %v; want задачу из хранилища", got, err)
	}
}

func TestTasksList(t *testing.T) {
	svc, _, _ := newTasks(t)
	for _, title := range []string{"a", "b", "c"} {
		if _, err := svc.Create(ctx, 1, task.NewTask{Title: title}); err != nil {
			t.Fatal(err)
		}
	}
	got, total, err := svc.List(ctx, 1, task.Filter{Limit: 2})
	if err != nil || total != 3 || len(got) != 2 {
		t.Errorf("List = %d задач, total %d, %v; want 2, 3, nil", len(got), total, err)
	}
}

func TestTasksRepoErrors(t *testing.T) {
	svc, repo, _ := newTasks(t)
	boom := errors.New("база недоступна")
	repo.Err = boom

	if _, err := svc.Create(ctx, 1, task.NewTask{Title: "a"}); !errors.Is(err, boom) {
		t.Errorf("Create error = %v, want %v", err, boom)
	}
	if _, err := svc.Get(ctx, 1, 1); !errors.Is(err, boom) {
		t.Errorf("Get error = %v, want %v", err, boom)
	}
	if _, _, err := svc.List(ctx, 1, task.Filter{}); !errors.Is(err, boom) {
		t.Errorf("List error = %v, want %v", err, boom)
	}
	if _, err := svc.Update(ctx, 1, 1, task.Patch{Done: ptr(true)}); !errors.Is(err, boom) {
		t.Errorf("Update error = %v, want %v", err, boom)
	}
	if err := svc.Delete(ctx, 1, 1); !errors.Is(err, boom) {
		t.Errorf("Delete error = %v, want %v", err, boom)
	}
}
