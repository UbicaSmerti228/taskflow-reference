// Package task описывает задачу и правила, общие для всех хранилищ.
package task

import (
	"errors"
	"strings"
	"time"
)

// Task — одна задача трекера.
type Task struct {
	ID         int64      `json:"id"`
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

// Filter — условия выборки списка.
type Filter struct {
	Done    *bool
	Limit   int   // 0 — без ограничения
	Offset  int   // пропустить столько задач
	AfterID int64 // вернуть задачи с id больше этого: пагинация по ключу
}

// Ошибки, по которым вызывающий код выбирает, что ответить пользователю.
var (
	ErrNotFound   = errors.New("task not found")
	ErrEmptyTitle = errors.New("title is empty")
	ErrEmptyPatch = errors.New("nothing to update")
)

// Normalize проверяет данные новой задачи и обрезает пробелы в заголовке.
func (n NewTask) Normalize() (NewTask, error) {
	n.Title = strings.TrimSpace(n.Title)
	if n.Title == "" {
		return NewTask{}, ErrEmptyTitle
	}
	return n, nil
}

// Normalize проверяет патч и обрезает пробелы в заголовке.
func (p Patch) Normalize() (Patch, error) {
	if p.Title == nil && p.Done == nil && p.DueAt == nil {
		return Patch{}, ErrEmptyPatch
	}
	if p.Title != nil {
		title := strings.TrimSpace(*p.Title)
		if title == "" {
			return Patch{}, ErrEmptyTitle
		}
		p.Title = &title
	}
	return p, nil
}
