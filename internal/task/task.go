// Package task описывает задачу и правила работы с ней.
package task

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
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
	ErrNotFound     = errors.New("task not found")
	ErrEmptyTitle   = errors.New("title is empty")
	ErrTitleTooLong = errors.New("title is too long")
	ErrEmptyPatch   = errors.New("nothing to update")
)

// Normalize проверяет данные новой задачи и обрезает пробелы в заголовке.
func (n NewTask) Normalize() (NewTask, error) {
	title, err := normalizeTitle(n.Title)
	if err != nil {
		return NewTask{}, err
	}
	n.Title = title
	return n, nil
}

// Normalize проверяет патч и обрезает пробелы в заголовке.
func (p Patch) Normalize() (Patch, error) {
	if p.Title == nil && p.Done == nil && p.DueAt == nil {
		return Patch{}, ErrEmptyPatch
	}
	if p.Title != nil {
		title, err := normalizeTitle(*p.Title)
		if err != nil {
			return Patch{}, err
		}
		p.Title = &title
	}
	return p, nil
}

// MaxTitleLen — наибольшая длина заголовка в символах.
const MaxTitleLen = 200

func normalizeTitle(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ErrEmptyTitle
	}
	if utf8.RuneCountInString(s) > MaxTitleLen {
		return "", ErrTitleTooLong
	}
	return s, nil
}
