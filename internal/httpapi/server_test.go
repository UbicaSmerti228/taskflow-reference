package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// fakeStore — хранилище в памяти, написанное руками.
type fakeStore struct {
	tasks []task.Task
	err   error
}

func (f *fakeStore) Add(title string) (task.Task, error) {
	if f.err != nil {
		return task.Task{}, f.err
	}
	if strings.TrimSpace(title) == "" {
		return task.Task{}, task.ErrEmptyTitle
	}
	t := task.Task{ID: len(f.tasks) + 1, Title: title}
	f.tasks = append(f.tasks, t)
	return t, nil
}

func (f *fakeStore) List() ([]task.Task, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.tasks, nil
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func TestCreateTask(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "задача создана", body: `{"title":"купить молоко"}`, want: http.StatusCreated},
		{name: "пустой заголовок", body: `{"title":""}`, want: http.StatusBadRequest},
		{name: "нет поля title", body: `{}`, want: http.StatusBadRequest},
		{name: "битый JSON", body: `{"title":`, want: http.StatusBadRequest},
		{name: "лишнее поле", body: `{"title":"a","admin":true}`, want: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, New(&fakeStore{}), http.MethodPost, "/tasks", tt.body)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; тело: %s", rec.Code, tt.want, rec.Body)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}
		})
	}
}

func TestCreateThenList(t *testing.T) {
	h := New(&fakeStore{})
	do(t, h, http.MethodPost, "/tasks", `{"title":"первая"}`)
	do(t, h, http.MethodPost, "/tasks", `{"title":"вторая"}`)

	rec := do(t, h, http.MethodGet, "/tasks", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got []task.Task
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("тело не разбирается как JSON: %v", err)
	}
	if len(got) != 2 || got[0].Title != "первая" || got[1].Title != "вторая" {
		t.Errorf("задачи = %+v, want «первая» и «вторая»", got)
	}
}

func TestStoreErrorIsHidden(t *testing.T) {
	h := New(&fakeStore{err: errors.New("disk /var/data is full")})

	for _, req := range []struct{ method, body string }{
		{http.MethodGet, ""},
		{http.MethodPost, `{"title":"задача"}`},
	} {
		rec := do(t, h, req.method, "/tasks", req.body)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("%s: status = %d, want 500", req.method, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "disk") {
			t.Errorf("%s: внутренняя ошибка попала в ответ: %s", req.method, rec.Body)
		}
	}
}

func TestMethodNotAllowed(t *testing.T) {
	rec := do(t, New(&fakeStore{}), http.MethodDelete, "/tasks", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rec.Code)
	}
}
