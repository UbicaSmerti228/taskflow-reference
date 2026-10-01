// Package httpapi отдаёт задачи по HTTP.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

const (
	maxBody      = 1 << 20 // 1 МБ
	defaultLimit = 20
	maxLimit     = 100
)

// Store — то, что серверу нужно от хранилища.
// Интерфейс объявлен здесь, у потребителя: в тестах его легко подменить.
type Store interface {
	Create(in task.NewTask) (task.Task, error)
	Get(id int) (task.Task, error)
	List(f task.Filter) ([]task.Task, int, error)
	Update(id int, p task.Patch) (task.Task, error)
	Delete(id int) error
}

type server struct {
	store Store
}

// New собирает обработчик со всеми маршрутами.
func New(store Store) http.Handler {
	s := &server{store: store}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /tasks", s.list)
	mux.HandleFunc("POST /tasks", s.create)
	mux.HandleFunc("GET /tasks/{id}", s.get)
	mux.HandleFunc("PATCH /tasks/{id}", s.update)
	mux.HandleFunc("DELETE /tasks/{id}", s.delete)

	// Шаблоны без метода ловят остальные методы на тех же путях,
	// а "/" — все прочие пути: так 405 и 404 приходят в том же формате, что и остальные ошибки.
	mux.HandleFunc("/tasks", methodNotAllowed("GET, POST"))
	mux.HandleFunc("/tasks/{id}", methodNotAllowed("GET, PATCH, DELETE"))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "no such route")
	})

	return mux
}

type listResponse struct {
	Items  []task.Task `json:"items"`
	Total  int         `json:"total"`
	Limit  int         `json:"limit"`
	Offset int         `json:"offset"`
}

func (s *server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := task.Filter{Limit: defaultLimit}

	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxLimit {
			writeError(w, http.StatusBadRequest, "invalid_argument", fmt.Sprintf("limit must be a number from 1 to %d", maxLimit))
			return
		}
		f.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid_argument", "offset must be a non-negative number")
			return
		}
		f.Offset = n
	}
	if v := q.Get("done"); v != "" {
		done, err := strconv.ParseBool(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_argument", "done must be true or false")
			return
		}
		f.Done = &done
	}

	tasks, total, err := s.store.List(f)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse{Items: tasks, Total: total, Limit: f.Limit, Offset: f.Offset})
}

func (s *server) create(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Title string     `json:"title"`
		DueAt *time.Time `json:"due_at"`
	}
	if !decode(w, r, &in) {
		return
	}
	t, err := s.store.Create(task.NewTask{Title: in.Title, DueAt: in.DueAt})
	if err != nil {
		s.fail(w, err)
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/tasks/%d", t.ID))
	writeJSON(w, http.StatusCreated, t)
}

func (s *server) get(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	t, err := s.store.Get(id)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *server) update(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var in struct {
		Title *string    `json:"title"`
		Done  *bool      `json:"done"`
		DueAt *time.Time `json:"due_at"`
	}
	if !decode(w, r, &in) {
		return
	}
	t, err := s.store.Update(id, task.Patch{Title: in.Title, Done: in.Done, DueAt: in.DueAt})
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *server) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := s.store.Delete(id); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// fail переводит ошибку хранилища в ответ. Это единственное место, где ошибки превращаются в статусы.
func (s *server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, task.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "task not found")
	case errors.Is(err, task.ErrEmptyTitle):
		writeError(w, http.StatusBadRequest, "invalid_argument", "title is required")
	case errors.Is(err, task.ErrEmptyPatch):
		writeError(w, http.StatusBadRequest, "invalid_argument", "nothing to update")
	default:
		// Подробности — в лог, клиенту — общее сообщение.
		log.Printf("internal error: %v", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

func methodNotAllowed(allow string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Allow", allow)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

func pathID(w http.ResponseWriter, r *http.Request) (int, bool) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "invalid_argument", "id must be a positive number")
		return 0, false
	}
	return id, true
}

// decode разбирает тело запроса строго: неизвестные поля и мусор после JSON — ошибка.
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "too_large", "request body is too large")
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid_argument", "invalid JSON body")
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid_argument", "invalid JSON body")
		return false
	}
	return true
}

type errorBody struct {
	Error errorInfo `json:"error"`
}

type errorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorBody{Error: errorInfo{Code: code, Message: msg}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}
