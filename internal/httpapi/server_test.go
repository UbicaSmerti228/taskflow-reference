package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// newAPI поднимает обработчик на настоящем файловом хранилище во временной папке.
func newAPI(t *testing.T, titles ...string) http.Handler {
	t.Helper()
	store := task.NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	for _, title := range titles {
		if _, err := store.Create(task.NewTask{Title: title}); err != nil {
			t.Fatal(err)
		}
	}
	return New(store)
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("тело не разбирается как JSON: %v; тело: %s", err, rec.Body)
	}
	return v
}

func TestCreate(t *testing.T) {
	rec := do(t, newAPI(t), http.MethodPost, "/tasks", `{"title":"купить молоко","due_at":"2026-05-01T10:00:00Z"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; тело: %s", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); loc != "/tasks/1" {
		t.Errorf("Location = %q, want /tasks/1", loc)
	}
	got := decodeBody[task.Task](t, rec)
	if got.ID != 1 || got.Title != "купить молоко" || got.DueAt == nil {
		t.Errorf("задача = %+v, want id 1, заголовок и срок", got)
	}
}

// Каждая ошибка приходит в едином формате {"error":{"code","message"}}.
func TestErrors(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		want     int
		wantCode string
	}{
		{name: "пустой заголовок", method: "POST", path: "/tasks", body: `{"title":" "}`, want: 400, wantCode: "invalid_argument"},
		{name: "битый JSON", method: "POST", path: "/tasks", body: `{"title":`, want: 400, wantCode: "invalid_argument"},
		{name: "лишнее поле", method: "POST", path: "/tasks", body: `{"title":"a","admin":true}`, want: 400, wantCode: "invalid_argument"},
		{name: "мусор после JSON", method: "POST", path: "/tasks", body: `{"title":"a"}{"title":"b"}`, want: 400, wantCode: "invalid_argument"},
		{name: "неверная дата", method: "POST", path: "/tasks", body: `{"title":"a","due_at":"завтра"}`, want: 400, wantCode: "invalid_argument"},
		{name: "слишком большое тело", method: "POST", path: "/tasks", body: `{"title":"` + strings.Repeat("a", maxBody) + `"}`, want: 413, wantCode: "too_large"},
		{name: "задачи нет", method: "GET", path: "/tasks/9", want: 404, wantCode: "not_found"},
		{name: "id не число", method: "GET", path: "/tasks/abc", want: 400, wantCode: "invalid_argument"},
		{name: "id меньше единицы", method: "DELETE", path: "/tasks/0", want: 400, wantCode: "invalid_argument"},
		{name: "патч несуществующей задачи", method: "PATCH", path: "/tasks/9", body: `{"done":true}`, want: 404, wantCode: "not_found"},
		{name: "пустой патч", method: "PATCH", path: "/tasks/1", body: `{}`, want: 400, wantCode: "invalid_argument"},
		{name: "удаление несуществующей задачи", method: "DELETE", path: "/tasks/9", want: 404, wantCode: "not_found"},
		{name: "limit не число", method: "GET", path: "/tasks?limit=many", want: 400, wantCode: "invalid_argument"},
		{name: "limit больше максимума", method: "GET", path: "/tasks?limit=101", want: 400, wantCode: "invalid_argument"},
		{name: "отрицательный offset", method: "GET", path: "/tasks?offset=-1", want: 400, wantCode: "invalid_argument"},
		{name: "done не булево", method: "GET", path: "/tasks?done=maybe", want: 400, wantCode: "invalid_argument"},
		{name: "метод не поддерживается для списка", method: "PUT", path: "/tasks", want: 405, wantCode: "method_not_allowed"},
		{name: "метод не поддерживается для задачи", method: "POST", path: "/tasks/1", want: 405, wantCode: "method_not_allowed"},
		{name: "неизвестный путь", method: "GET", path: "/users", want: 404, wantCode: "not_found"},
	}
	h := newAPI(t, "задача")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, h, tt.method, tt.path, tt.body)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; тело: %s", rec.Code, tt.want, rec.Body)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}
			got := decodeBody[errorBody](t, rec)
			if got.Error.Code != tt.wantCode || got.Error.Message == "" {
				t.Errorf("error = %+v, want код %q и непустое сообщение", got.Error, tt.wantCode)
			}
			if tt.want == http.StatusMethodNotAllowed && rec.Header().Get("Allow") == "" {
				t.Error("в ответе 405 нет заголовка Allow")
			}
		})
	}
}

func TestList(t *testing.T) {
	titles := make([]string, 25)
	for i := range titles {
		titles[i] = fmt.Sprintf("задача %d", i+1)
	}
	h := newAPI(t, titles...)
	for _, id := range []int{1, 2, 3} {
		do(t, h, http.MethodPatch, fmt.Sprintf("/tasks/%d", id), `{"done":true}`)
	}

	tests := []struct {
		name       string
		query      string
		wantFirst  int
		wantLen    int
		wantTotal  int
		wantLimit  int
		wantOffset int
	}{
		{name: "по умолчанию 20 задач", query: "", wantFirst: 1, wantLen: 20, wantTotal: 25, wantLimit: 20},
		{name: "вторая страница", query: "?limit=10&offset=10", wantFirst: 11, wantLen: 10, wantTotal: 25, wantLimit: 10, wantOffset: 10},
		{name: "неполная последняя страница", query: "?limit=10&offset=20", wantFirst: 21, wantLen: 5, wantTotal: 25, wantLimit: 10, wantOffset: 20},
		{name: "только выполненные", query: "?done=true", wantFirst: 1, wantLen: 3, wantTotal: 3, wantLimit: 20},
		{name: "невыполненные", query: "?done=false&limit=5", wantFirst: 4, wantLen: 5, wantTotal: 22, wantLimit: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, h, http.MethodGet, "/tasks"+tt.query, "")
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; тело: %s", rec.Code, rec.Body)
			}
			got := decodeBody[listResponse](t, rec)
			if len(got.Items) != tt.wantLen || got.Total != tt.wantTotal || got.Limit != tt.wantLimit || got.Offset != tt.wantOffset {
				t.Fatalf("items = %d, total = %d, limit = %d, offset = %d; want %d, %d, %d, %d",
					len(got.Items), got.Total, got.Limit, got.Offset, tt.wantLen, tt.wantTotal, tt.wantLimit, tt.wantOffset)
			}
			if got.Items[0].ID != tt.wantFirst {
				t.Errorf("первая задача id = %d, want %d", got.Items[0].ID, tt.wantFirst)
			}
		})
	}
}

func TestEmptyListIsArray(t *testing.T) {
	rec := do(t, newAPI(t), http.MethodGet, "/tasks", "")
	if !strings.Contains(rec.Body.String(), `"items":[]`) {
		t.Errorf("пустой список должен приходить как [], а не null: %s", rec.Body)
	}
}

func TestGetPatchDelete(t *testing.T) {
	h := newAPI(t, "первая", "вторая")

	rec := do(t, h, http.MethodPatch, "/tasks/2", `{"title":"вторая, исправленная","done":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d, want 200; тело: %s", rec.Code, rec.Body)
	}

	got := decodeBody[task.Task](t, do(t, h, http.MethodGet, "/tasks/2", ""))
	if got.Title != "вторая, исправленная" || !got.Done {
		t.Errorf("после PATCH задача = %+v, want новый заголовок и done = true", got)
	}

	rec = do(t, h, http.MethodDelete, "/tasks/2", "")
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Errorf("DELETE status = %d, тело %q; want 204 без тела", rec.Code, rec.Body)
	}
	if rec := do(t, h, http.MethodGet, "/tasks/2", ""); rec.Code != http.StatusNotFound {
		t.Errorf("GET после DELETE status = %d, want 404", rec.Code)
	}
	// Повторное удаление не меняет состояние и честно отвечает 404.
	if rec := do(t, h, http.MethodDelete, "/tasks/2", ""); rec.Code != http.StatusNotFound {
		t.Errorf("повторный DELETE status = %d, want 404", rec.Code)
	}
}

// brokenStore всегда возвращает внутреннюю ошибку.
type brokenStore struct{}

var errDisk = errors.New("disk /var/data is full")

func (brokenStore) Create(task.NewTask) (task.Task, error)     { return task.Task{}, errDisk }
func (brokenStore) Get(int) (task.Task, error)                 { return task.Task{}, errDisk }
func (brokenStore) List(task.Filter) ([]task.Task, int, error) { return nil, 0, errDisk }
func (brokenStore) Update(int, task.Patch) (task.Task, error)  { return task.Task{}, errDisk }
func (brokenStore) Delete(int) error                           { return errDisk }

func TestInternalErrorIsHidden(t *testing.T) {
	h := New(brokenStore{})
	requests := []struct{ method, path, body string }{
		{"GET", "/tasks", ""},
		{"POST", "/tasks", `{"title":"задача"}`},
		{"GET", "/tasks/1", ""},
		{"PATCH", "/tasks/1", `{"done":true}`},
		{"DELETE", "/tasks/1", ""},
	}
	for _, req := range requests {
		rec := do(t, h, req.method, req.path, req.body)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("%s %s: status = %d, want 500", req.method, req.path, rec.Code)
		}
		if got := decodeBody[errorBody](t, rec); got.Error.Code != "internal" {
			t.Errorf("%s %s: код ошибки = %q, want internal", req.method, req.path, got.Error.Code)
		}
		if strings.Contains(rec.Body.String(), "disk") {
			t.Errorf("%s %s: внутренняя ошибка попала в ответ: %s", req.method, req.path, rec.Body)
		}
	}
}
