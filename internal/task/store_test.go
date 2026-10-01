package task

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var testNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func newStore(t *testing.T) *FileStore {
	t.Helper()
	s := NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	s.now = func() time.Time { return testNow }
	return s
}

// seed создаёт задачи с заголовками titles и возвращает хранилище.
func seed(t *testing.T, titles ...string) *FileStore {
	t.Helper()
	s := newStore(t)
	for _, title := range titles {
		if _, err := s.Create(NewTask{Title: title}); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func ptr[T any](v T) *T { return &v }

func TestCreate(t *testing.T) {
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
			got, err := newStore(t).Create(NewTask{Title: tt.title})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Create(%q) error = %v, want %v", tt.title, err, tt.wantErr)
			}
			if got.Title != tt.want {
				t.Errorf("Create(%q).Title = %q, want %q", tt.title, got.Title, tt.want)
			}
		})
	}
}

func TestCreateAssignsIDs(t *testing.T) {
	s := seed(t, "первая", "вторая", "третья")
	if err := s.Delete(3); err != nil {
		t.Fatal(err)
	}
	got, err := s.Create(NewTask{Title: "четвёртая"})
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != 3 || !got.CreatedAt.Equal(testNow) {
		t.Errorf("Create() = %+v, want ID 3 и CreatedAt %v", got, testNow)
	}
}

func TestGet(t *testing.T) {
	s := seed(t, "первая", "вторая")

	got, err := s.Get(2)
	if err != nil || got.Title != "вторая" {
		t.Errorf("Get(2) = %+v, %v; want задачу «вторая»", got, err)
	}
	if _, err := s.Get(9); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(9) error = %v, want ErrNotFound", err)
	}
}

func TestList(t *testing.T) {
	s := seed(t, "a", "b", "c", "d", "e")
	for _, id := range []int{2, 4} {
		if _, err := s.Update(id, Patch{Done: ptr(true)}); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name      string
		filter    Filter
		wantIDs   []int
		wantTotal int
	}{
		{name: "без ограничений", filter: Filter{}, wantIDs: []int{1, 2, 3, 4, 5}, wantTotal: 5},
		{name: "первая страница", filter: Filter{Limit: 2}, wantIDs: []int{1, 2}, wantTotal: 5},
		{name: "вторая страница", filter: Filter{Limit: 2, Offset: 2}, wantIDs: []int{3, 4}, wantTotal: 5},
		{name: "неполная последняя страница", filter: Filter{Limit: 2, Offset: 4}, wantIDs: []int{5}, wantTotal: 5},
		{name: "смещение за концом", filter: Filter{Limit: 2, Offset: 50}, wantIDs: nil, wantTotal: 5},
		{name: "только выполненные", filter: Filter{Done: ptr(true)}, wantIDs: []int{2, 4}, wantTotal: 2},
		{name: "невыполненные со смещением", filter: Filter{Done: ptr(false), Limit: 2, Offset: 1}, wantIDs: []int{3, 5}, wantTotal: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, total, err := s.List(tt.filter)
			if err != nil {
				t.Fatal(err)
			}
			if total != tt.wantTotal {
				t.Errorf("total = %d, want %d", total, tt.wantTotal)
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
}

func TestListEmptyIsNotNil(t *testing.T) {
	got, total, err := newStore(t).List(Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 || total != 0 {
		t.Errorf("List() = %v, %d; want пустой слайс (не nil) и 0", got, total)
	}
}

func TestUpdate(t *testing.T) {
	due := testNow.Add(time.Hour)
	tests := []struct {
		name    string
		id      int
		patch   Patch
		wantErr error
		check   func(t *testing.T, got Task)
	}{
		{name: "заголовок", id: 1, patch: Patch{Title: ptr(" новый ")}, check: func(t *testing.T, got Task) {
			if got.Title != "новый" || got.Done {
				t.Errorf("got %+v, want заголовок «новый» и Done = false", got)
			}
		}},
		{name: "выполнение", id: 1, patch: Patch{Done: ptr(true)}, check: func(t *testing.T, got Task) {
			if !got.Done || got.Title != "задача" {
				t.Errorf("got %+v, want Done = true и прежний заголовок", got)
			}
		}},
		{name: "срок", id: 1, patch: Patch{DueAt: &due}, check: func(t *testing.T, got Task) {
			if got.DueAt == nil || !got.DueAt.Equal(due) {
				t.Errorf("DueAt = %v, want %v", got.DueAt, due)
			}
		}},
		{name: "пустой патч", id: 1, patch: Patch{}, wantErr: ErrEmptyPatch},
		{name: "пустой заголовок", id: 1, patch: Patch{Title: ptr("  ")}, wantErr: ErrEmptyTitle},
		{name: "неизвестный id", id: 9, patch: Patch{Done: ptr(true)}, wantErr: ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := seed(t, "задача")
			got, err := s.Update(tt.id, tt.patch)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Update() error = %v, want %v", err, tt.wantErr)
			}
			if tt.check != nil {
				tt.check(t, got)
				// Изменение должно дойти до файла, а не остаться в памяти.
				saved, err := NewFileStore(s.path).Get(tt.id)
				if err != nil {
					t.Fatal(err)
				}
				tt.check(t, saved)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	s := seed(t, "первая", "вторая")

	if err := s.Delete(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(1); !errors.Is(err, ErrNotFound) {
		t.Errorf("повторный Delete(1) error = %v, want ErrNotFound", err)
	}
	got, total, err := s.List(Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || got[0].ID != 2 {
		t.Errorf("после удаления остались %+v, want одну задачу с id 2", got)
	}
}

func TestReminders(t *testing.T) {
	past, future := testNow.Add(-time.Minute), testNow.Add(time.Minute)
	s := newStore(t)
	for _, in := range []NewTask{
		{Title: "без срока"},
		{Title: "просрочена", DueAt: &past},
		{Title: "срок ровно сейчас", DueAt: &testNow},
		{Title: "срок в будущем", DueAt: &future},
		{Title: "просрочена, но выполнена", DueAt: &past},
	} {
		if _, err := s.Create(in); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Update(5, Patch{Done: ptr(true)}); err != nil {
		t.Fatal(err)
	}

	ids := func() []int {
		t.Helper()
		due, err := s.DueForReminder(testNow)
		if err != nil {
			t.Fatal(err)
		}
		var out []int
		for _, tk := range due {
			out = append(out, tk.ID)
		}
		return out
	}

	if got := ids(); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("DueForReminder() ids = %v, want [2 3]", got)
	}

	if err := s.MarkReminded(2, testNow); err != nil {
		t.Fatal(err)
	}
	if got := ids(); len(got) != 1 || got[0] != 3 {
		t.Errorf("после MarkReminded(2) ids = %v, want [3]", got)
	}

	// Новый срок сбрасывает отметку: о задаче нужно напомнить ещё раз.
	if _, err := s.Update(2, Patch{DueAt: &past}); err != nil {
		t.Fatal(err)
	}
	if got := ids(); len(got) != 2 {
		t.Errorf("после переноса срока ids = %v, want [2 3]", got)
	}

	if err := s.MarkReminded(9, testNow); !errors.Is(err, ErrNotFound) {
		t.Errorf("MarkReminded(9) error = %v, want ErrNotFound", err)
	}
}

func TestBrokenFile(t *testing.T) {
	s := newStore(t)
	if err := os.WriteFile(s.path, []byte("{не json"), 0o600); err != nil {
		t.Fatal(err)
	}

	calls := map[string]func() error{
		"Create":         func() error { _, err := s.Create(NewTask{Title: "задача"}); return err },
		"Get":            func() error { _, err := s.Get(1); return err },
		"List":           func() error { _, _, err := s.List(Filter{}); return err },
		"Update":         func() error { _, err := s.Update(1, Patch{Done: ptr(true)}); return err },
		"Delete":         func() error { return s.Delete(1) },
		"DueForReminder": func() error { _, err := s.DueForReminder(testNow); return err },
		"MarkReminded":   func() error { return s.MarkReminded(1, testNow) },
	}
	for name, call := range calls {
		if err := call(); err == nil || errors.Is(err, ErrNotFound) {
			t.Errorf("%s на битом файле: error = %v, want ошибку чтения", name, err)
		}
	}
}
