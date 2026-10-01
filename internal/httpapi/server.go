// Package httpapi отдаёт задачи по HTTP.
package httpapi

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

const maxBody = 1 << 20 // 1 МБ

// Store — то, что серверу нужно от хранилища.
// Интерфейс объявлен здесь, у потребителя: в тестах его легко подменить.
type Store interface {
	Add(title string) (task.Task, error)
	List() ([]task.Task, error)
}

// New собирает обработчик со всеми маршрутами.
func New(store Store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /tasks", func(w http.ResponseWriter, _ *http.Request) {
		tasks, err := store.List()
		if err != nil {
			log.Printf("list tasks: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		writeJSON(w, http.StatusOK, tasks)
	})

	mux.HandleFunc("POST /tasks", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Title string `json:"title"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON body")
			return
		}

		t, err := store.Add(in.Title)
		switch {
		case errors.Is(err, task.ErrEmptyTitle):
			writeError(w, http.StatusBadRequest, "title is required")
		case err != nil:
			// Подробности — в лог, клиенту — общее сообщение.
			log.Printf("add task: %v", err)
			writeError(w, http.StatusInternalServerError, "internal error")
		default:
			writeJSON(w, http.StatusCreated, t)
		}
	})

	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
