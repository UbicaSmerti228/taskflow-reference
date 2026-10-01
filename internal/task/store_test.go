package task

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newStore(t *testing.T) *FileStore {
	t.Helper()
	s := NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	s.now = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }
	return s
}

func TestAdd(t *testing.T) {
	tests := []struct {
		name    string
		title   string
		want    string
		wantErr error
	}{
		{name: "обычный заголовок", title: "купить молоко", want: "купить молоко"},
		{name: "пробелы по краям обрезаются", title: "  позвонить  ", want: "позвонить"},
		{name: "пустой заголовок", title: "", wantErr: ErrEmptyTitle},
		{name: "заголовок из пробелов", title: " \t ", wantErr: ErrEmptyTitle},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newStore(t).Add(tt.title)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Add(%q) error = %v, want %v", tt.title, err, tt.wantErr)
			}
			if got.Title != tt.want {
				t.Errorf("Add(%q).Title = %q, want %q", tt.title, got.Title, tt.want)
			}
		})
	}
}

func TestAddAssignsIDsAndKeepsOrder(t *testing.T) {
	s := newStore(t)
	for _, title := range []string{"первая", "вторая", "третья"} {
		if _, err := s.Add(title); err != nil {
			t.Fatal(err)
		}
	}

	tasks, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 3 {
		t.Fatalf("len(List()) = %d, want 3", len(tasks))
	}
	for i, tk := range tasks {
		if tk.ID != i+1 {
			t.Errorf("tasks[%d].ID = %d, want %d", i, tk.ID, i+1)
		}
		if tk.Done {
			t.Errorf("tasks[%d].Done = true, want false", i)
		}
		if tk.CreatedAt.IsZero() {
			t.Errorf("tasks[%d].CreatedAt не заполнено", i)
		}
	}
}

func TestListEmpty(t *testing.T) {
	tasks, err := newStore(t).List()
	if err != nil {
		t.Fatal(err)
	}
	if tasks == nil || len(tasks) != 0 {
		t.Errorf("List() = %v, want пустой слайс, не nil", tasks)
	}
}

func TestDone(t *testing.T) {
	tests := []struct {
		name    string
		id      int
		wantErr error
	}{
		{name: "существующая задача", id: 1},
		{name: "неизвестный id", id: 42, wantErr: ErrNotFound},
		{name: "отрицательный id", id: -1, wantErr: ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStore(t)
			if _, err := s.Add("задача"); err != nil {
				t.Fatal(err)
			}

			got, err := s.Done(tt.id)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Done(%d) error = %v, want %v", tt.id, err, tt.wantErr)
			}
			if tt.wantErr == nil && !got.Done {
				t.Errorf("Done(%d).Done = false, want true", tt.id)
			}
		})
	}
}

func TestDonePersists(t *testing.T) {
	s := newStore(t)
	if _, err := s.Add("задача"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Done(1); err != nil {
		t.Fatal(err)
	}

	// Новое хранилище на том же файле видит изменения.
	tasks, err := NewFileStore(s.path).List()
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || !tasks[0].Done {
		t.Errorf("после перезапуска tasks = %+v, want одну выполненную задачу", tasks)
	}
}

func TestBrokenFile(t *testing.T) {
	s := newStore(t)
	if err := os.WriteFile(s.path, []byte("{не json"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := s.List(); err == nil {
		t.Error("List() на битом файле вернул nil, want ошибку")
	}
	if _, err := s.Add("задача"); err == nil {
		t.Error("Add() на битом файле вернул nil, want ошибку")
	}
	if _, err := s.Done(1); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("Done() на битом файле: error = %v, want ошибку чтения", err)
	}
}
