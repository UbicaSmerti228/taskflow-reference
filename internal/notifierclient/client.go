// Package notifierclient — клиент сервиса notifier для TaskFlow.
// Скрывает gRPC: остальной код получает обычные Go-типы и ошибки предметной области.
package notifierclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/UbicaSmerti228/taskflow-reference/internal/gen/notifierv1"
	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
	"github.com/UbicaSmerti228/taskflow-reference/internal/resilience"
)

// Client вызывает notifier по gRPC.
type Client struct {
	conn    *grpc.ClientConn
	api     notifierv1.NotifierServiceClient
	timeout time.Duration
	breaker *resilience.Breaker
}

// Option меняет одну настройку клиента.
type Option func(*Client)

// WithTimeout задаёт deadline одного вызова.
func WithTimeout(d time.Duration) Option { return func(c *Client) { c.timeout = d } }

// WithBreaker задаёт circuit breaker.
func WithBreaker(b *resilience.Breaker) Option { return func(c *Client) { c.breaker = b } }

// New создаёт клиент. Соединение устанавливается при первом вызове: TaskFlow стартует,
// даже когда notifier ещё не поднялся.
//
// Соединение не шифруется: сервисы общаются во внутренней сети. В сети, которой нельзя доверять,
// здесь должен быть TLS, а лучше взаимный (mTLS).
func New(addr string, opts ...Option) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("notifier client: %w", err)
	}
	return NewWithConn(conn, opts...), nil
}

// NewWithConn создаёт клиент поверх готового соединения — так его собирают тесты.
func NewWithConn(conn *grpc.ClientConn, opts ...Option) *Client {
	c := &Client{
		conn:    conn,
		api:     notifierv1.NewNotifierServiceClient(conn),
		timeout: 2 * time.Second,
		breaker: resilience.NewBreaker(5, 10*time.Second),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Close закрывает соединение.
func (c *Client) Close() error { return c.conn.Close() }

// List возвращает страницу уведомлений пользователя, сначала новые.
// Если notifier не отвечает вовремя, возвращает notification.ErrUnavailable.
func (c *Client) List(ctx context.Context, userID int64, limit int, beforeID int64) ([]notification.Notification, error) {
	// Deadline уходит на сервер вместе с вызовом: когда он истечёт, сервер тоже перестанет работать над запросом.
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	// Идентификатор HTTP-запроса едет дальше в метаданных: по нему связываются логи двух сервисов.
	if id := logging.RequestID(ctx); id != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, logging.MetadataRequestID, id)
	}

	var (
		resp    *notifierv1.ListNotificationsResponse
		callErr error
	)
	err := c.breaker.Do(func() error {
		resp, callErr = c.api.ListNotifications(ctx, &notifierv1.ListNotificationsRequest{
			UserId: userID, Limit: int32(limit), BeforeId: beforeID, // limit не больше 100: его проверил HTTP-слой
		})
		if unavailable(callErr) {
			return callErr // только такие отказы говорят, что сервису плохо
		}
		return nil
	})
	switch {
	case errors.Is(err, resilience.ErrOpen):
		return nil, fmt.Errorf("%w: %w", notification.ErrUnavailable, err)
	case unavailable(callErr):
		return nil, fmt.Errorf("%w: %w", notification.ErrUnavailable, callErr)
	case callErr != nil:
		return nil, fmt.Errorf("list notifications: %w", callErr)
	}

	items := make([]notification.Notification, len(resp.GetNotifications()))
	for i, n := range resp.GetNotifications() {
		items[i] = fromProto(n)
	}
	return items, nil
}

// unavailable отличает «сервис не отвечает» от «сервис ответил отказом».
func unavailable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}

var kinds = map[notifierv1.Kind]string{
	notifierv1.Kind_KIND_TASK_CREATED: notification.KindTaskCreated,
	notifierv1.Kind_KIND_TASK_DUE:     notification.KindTaskDue,
}

func fromProto(n *notifierv1.Notification) notification.Notification {
	kind, known := kinds[n.GetKind()]
	if !known {
		kind = "unknown" // сервер новее клиента и прислал вид, которого клиент ещё не знает
	}
	return notification.Notification{
		ID:        n.GetId(),
		UserID:    n.GetUserId(),
		Kind:      kind,
		TaskID:    n.GetTaskId(),
		Title:     n.GetTitle(),
		CreatedAt: n.GetCreatedAt().AsTime(),
	}
}
