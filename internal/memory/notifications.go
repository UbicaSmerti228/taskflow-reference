package memory

import (
	"context"
	"sync"

	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
)

// Notifications хранит уведомления в памяти. Ведёт себя как хранилище notifier в PostgreSQL.
type Notifications struct {
	mu        sync.Mutex
	items     []notification.Notification
	processed map[string]bool

	Err error // если задано, все методы возвращают эту ошибку
}

// NewNotifications возвращает пустое хранилище уведомлений.
func NewNotifications() *Notifications { return &Notifications{processed: map[string]bool{}} }

// Save сохраняет уведомление, если событие eventID ещё не обрабатывалось.
// Для уже обработанного события возвращает saved = false и ничего не меняет.
func (s *Notifications) Save(_ context.Context, eventID string, n notification.Notification) (notification.Notification, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return notification.Notification{}, false, s.Err
	}
	if s.processed[eventID] {
		return notification.Notification{}, false, nil
	}
	s.processed[eventID] = true
	n.ID = int64(len(s.items) + 1)
	n.CreatedAt = n.CreatedAt.UTC()
	s.items = append(s.items, n)
	return n, true, nil
}

// List возвращает до limit уведомлений пользователя, сначала новые. beforeID > 0 — только с id меньше него.
func (s *Notifications) List(_ context.Context, userID int64, limit int, beforeID int64) ([]notification.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, s.Err
	}
	out := []notification.Notification{}
	for i := len(s.items) - 1; i >= 0 && len(out) < limit; i-- {
		n := s.items[i]
		if n.UserID == userID && (beforeID == 0 || n.ID < beforeID) {
			out = append(out, n)
		}
	}
	return out, nil
}
