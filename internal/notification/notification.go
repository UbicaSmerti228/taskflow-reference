// Package notification описывает уведомление — то, что сервис notifier хранит и отдаёт.
package notification

import (
	"errors"
	"time"
)

// Виды уведомлений. Значения совпадают с типами событий, из которых уведомления получаются.
const (
	KindTaskCreated = "task.created"
	KindTaskDue     = "task.due"
)

// Notification — одно уведомление пользователя.
type Notification struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"-"` // владельца клиент и так знает: это он сам
	Kind      string    `json:"kind"`
	TaskID    int64     `json:"task_id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
}

// ErrUnavailable — сервис уведомлений не ответил вовремя или недоступен.
var ErrUnavailable = errors.New("notifications are unavailable")

// Границы размера страницы.
const (
	DefaultLimit = 20
	MaxLimit     = 100
)
