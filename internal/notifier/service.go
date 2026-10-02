// Package notifier — логика сервиса уведомлений: превращает события задач в уведомления,
// хранит их и раздаёт подписчикам. Про Kafka и gRPC этот пакет не знает.
package notifier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
)

// Store — что сервису нужно от хранилища.
type Store interface {
	// Save сохраняет уведомление, если событие eventID ещё не обрабатывалось;
	// для обработанного возвращает saved = false.
	Save(ctx context.Context, eventID string, n notification.Notification) (stored notification.Notification, saved bool, err error)
	List(ctx context.Context, userID int64, limit int, beforeID int64) ([]notification.Notification, error)
}

// Metrics считает обработанные события. Реализация — в пакете metrics.
type Metrics interface {
	// Event вызывается на каждое событие. result: saved, duplicate, ignored, malformed или failed.
	Event(eventType, result string)
}

type noMetrics struct{}

func (noMetrics) Event(string, string) {}

// ErrMalformed — событие нельзя разобрать. Повторная доставка этого не исправит.
var ErrMalformed = errors.New("malformed event")

// ErrBadRequest — запрос с недопустимыми параметрами.
var ErrBadRequest = errors.New("bad request")

// Service — сервис уведомлений.
type Service struct {
	store   Store
	hub     *Hub
	log     *slog.Logger
	metrics Metrics
}

// New собирает сервис. metrics может быть nil.
func New(store Store, hub *Hub, log *slog.Logger, metrics Metrics) *Service {
	if metrics == nil {
		metrics = noMetrics{}
	}
	return &Service{store: store, hub: hub, log: log, metrics: metrics}
}

// Handle обрабатывает одно событие из топика задач.
//
// Обработчик идемпотентен: событие с уже знакомым id ничего не меняет. Это обязательно,
// потому что Kafka доставляет «хотя бы один раз» — после сбоя событие приходит повторно.
func (s *Service) Handle(ctx context.Context, payload []byte) error {
	e, err := event.Decode(payload)
	if err != nil {
		s.metrics.Event("unknown", "malformed")
		return fmt.Errorf("%w: %w", ErrMalformed, err)
	}

	var kind string
	switch e.Type {
	case event.TypeTaskCreated:
		kind = notification.KindTaskCreated
	case event.TypeTaskDue:
		kind = notification.KindTaskDue
	default:
		// Издатель может начать слать новые типы событий раньше, чем обновится этот сервис.
		s.metrics.Event("unknown", "ignored")
		s.log.DebugContext(ctx, "event of unknown type ignored", "event_id", e.ID, "type", e.Type)
		return nil
	}

	stored, saved, err := s.store.Save(ctx, e.ID, notification.Notification{
		UserID: e.UserID, Kind: kind, TaskID: e.TaskID, Title: e.Title, CreatedAt: e.OccurredAt,
	})
	if err != nil {
		s.metrics.Event(e.Type, "failed")
		return err
	}
	if !saved {
		s.metrics.Event(e.Type, "duplicate")
		s.log.DebugContext(ctx, "duplicate event skipped", "event_id", e.ID)
		return nil
	}
	s.metrics.Event(e.Type, "saved")
	s.log.InfoContext(ctx, "notification saved", "event_id", e.ID, "notification_id", stored.ID, "user_id", stored.UserID)
	// Подписчикам уведомление уходит после коммита: они не увидят того, чего нет в базе.
	s.hub.Publish(stored)
	return nil
}

// List возвращает страницу уведомлений пользователя, сначала новые.
// limit 0 означает размер по умолчанию; beforeID 0 — с самых новых.
func (s *Service) List(ctx context.Context, userID int64, limit int, beforeID int64) ([]notification.Notification, error) {
	if userID < 1 {
		return nil, fmt.Errorf("%w: user_id must be positive", ErrBadRequest)
	}
	if limit < 0 || limit > notification.MaxLimit {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d", ErrBadRequest, notification.MaxLimit)
	}
	if beforeID < 0 {
		return nil, fmt.Errorf("%w: before_id must not be negative", ErrBadRequest)
	}
	if limit == 0 {
		limit = notification.DefaultLimit
	}
	return s.store.List(ctx, userID, limit, beforeID)
}

// Subscribe подписывает на новые уведомления пользователя. cancel нужно вызвать обязательно.
func (s *Service) Subscribe(userID int64) (ch <-chan notification.Notification, cancel func(), err error) {
	if userID < 1 {
		return nil, nil, fmt.Errorf("%w: user_id must be positive", ErrBadRequest)
	}
	ch, cancel = s.hub.Subscribe(userID)
	return ch, cancel, nil
}
