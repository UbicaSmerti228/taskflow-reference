// Package task хранит задачи и правила работы с ними.
package task

import (
	"errors"
	"time"
)

// Task — одна задача трекера.
type Task struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Done      bool      `json:"done"`
	CreatedAt time.Time `json:"created_at"`
}

// Ошибки, по которым вызывающий код выбирает, что ответить пользователю.
var (
	ErrNotFound   = errors.New("task not found")
	ErrEmptyTitle = errors.New("title is empty")
)
