// Package task хранит задачи и правила работы с ними.
package task

import (
	"errors"
	"time"
)

// Task — одна задача трекера.
type Task struct {
	ID         int        `json:"id"`
	Title      string     `json:"title"`
	Done       bool       `json:"done"`
	DueAt      *time.Time `json:"due_at,omitempty"`      // срок; nil — срока нет
	RemindedAt *time.Time `json:"reminded_at,omitempty"` // когда отправлено напоминание
	CreatedAt  time.Time  `json:"created_at"`
}

// NewTask — данные для создания задачи.
type NewTask struct {
	Title string
	DueAt *time.Time
}

// Patch — частичное изменение задачи: nil означает «поле не трогать».
type Patch struct {
	Title *string
	Done  *bool
	DueAt *time.Time
}

// Empty сообщает, что в патче нет ни одного поля.
func (p Patch) Empty() bool {
	return p.Title == nil && p.Done == nil && p.DueAt == nil
}

// Filter — условия выборки списка. Limit 0 означает «без ограничения».
type Filter struct {
	Done   *bool
	Limit  int
	Offset int
}

// Ошибки, по которым вызывающий код выбирает, что ответить пользователю.
var (
	ErrNotFound   = errors.New("task not found")
	ErrEmptyTitle = errors.New("title is empty")
	ErrEmptyPatch = errors.New("nothing to update")
)
