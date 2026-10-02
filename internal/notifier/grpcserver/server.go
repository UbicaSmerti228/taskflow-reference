// Package grpcserver — транспортный слой notifier: принимает gRPC-вызовы и зовёт сервис.
package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/UbicaSmerti228/taskflow-reference/internal/gen/notifierv1"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notifier"
)

// Notifications — что серверу нужно от сервиса уведомлений.
type Notifications interface {
	List(ctx context.Context, userID int64, limit int, beforeID int64) ([]notification.Notification, error)
	Subscribe(userID int64) (ch <-chan notification.Notification, cancel func(), err error)
}

// Metrics получает длительность и итог каждого вызова. Реализация — в пакете metrics.
type Metrics interface {
	Call(method, code string, took time.Duration)
}

type noMetrics struct{}

func (noMetrics) Call(string, string, time.Duration) {}

// Server — gRPC-сервер notifier.
type Server struct {
	notifierv1.UnimplementedNotifierServiceServer // новые методы контракта не ломают сборку: они отвечают Unimplemented

	svc             Notifications
	log             *slog.Logger
	metrics         Metrics
	shutdownTimeout time.Duration
	stopping        chan struct{} // закрывается в начале остановки: открытые стримы завершаются
}

// Option меняет одну настройку сервера.
type Option func(*Server)

// WithMetrics подключает метрики.
func WithMetrics(m Metrics) Option { return func(s *Server) { s.metrics = m } }

// WithShutdownTimeout задаёт, сколько ждать текущие вызовы при остановке.
func WithShutdownTimeout(d time.Duration) Option { return func(s *Server) { s.shutdownTimeout = d } }

// New собирает сервер.
func New(svc Notifications, log *slog.Logger, opts ...Option) *Server {
	s := &Server{svc: svc, log: log, metrics: noMetrics{}, shutdownTimeout: 10 * time.Second, stopping: make(chan struct{})}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Run слушает адрес и работает, пока не отменён ctx.
func (s *Server) Run(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	return s.Serve(ctx, ln)
}

// Serve принимает соединения из ln, пока не отменён ctx, затем останавливается: новые вызовы
// отклоняются, текущие дорабатывают, стримы закрываются.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := grpc.NewServer(
		// Перехватчики — то же, что middleware в HTTP: оборачивают каждый вызов.
		grpc.ChainUnaryInterceptor(s.unary),
		grpc.ChainStreamInterceptor(s.stream),
	)
	notifierv1.RegisterNotifierServiceServer(srv, s)

	// Стандартный сервис проверки здоровья: его понимают Kubernetes, балансировщики и команда healthcheck.
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	// Reflection позволяет grpcurl и подобным утилитам узнать методы сервиса без .proto-файла.
	reflection.Register(srv)

	failed := make(chan error, 1)
	go func() { failed <- srv.Serve(ln) }()
	s.log.Info("grpc server started", "addr", ln.Addr().String())

	select {
	case err := <-failed:
		return err
	case <-ctx.Done():
	}

	s.log.Info("grpc server shutting down")
	hs.Shutdown()     // проверки здоровья отвечают NOT_SERVING: балансировщик перестаёт слать вызовы
	close(s.stopping) // стримы живут сколько угодно, сами они не закончатся

	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop() // ждёт завершения текущих вызовов
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(s.shutdownTimeout):
		// Не дождались: обрываем соединения, контексты вызовов отменяются. Возврата Stop не ждём:
		// он может встать за GracefulStop, а тот ждёт обработчики — зависший обработчик не вернётся никогда.
		go srv.Stop()
		return errors.New("grpc shutdown: timeout, connections were closed forcibly")
	}
	s.log.Info("grpc server stopped")
	return nil
}

// ListNotifications возвращает страницу уведомлений пользователя.
func (s *Server) ListNotifications(ctx context.Context, req *notifierv1.ListNotificationsRequest) (*notifierv1.ListNotificationsResponse, error) {
	items, err := s.svc.List(ctx, req.GetUserId(), int(req.GetLimit()), req.GetBeforeId())
	if err != nil {
		return nil, s.toStatus(ctx, err)
	}
	resp := &notifierv1.ListNotificationsResponse{Notifications: make([]*notifierv1.Notification, len(items))}
	for i, n := range items {
		resp.Notifications[i] = toProto(n)
	}
	return resp, nil
}

// WatchNotifications присылает уведомления пользователя по мере появления.
func (s *Server) WatchNotifications(req *notifierv1.WatchNotificationsRequest, stream grpc.ServerStreamingServer[notifierv1.WatchNotificationsResponse]) error {
	ctx := stream.Context()
	ch, cancel, err := s.svc.Subscribe(req.GetUserId())
	if err != nil {
		return s.toStatus(ctx, err)
	}
	defer cancel() // без отписки подписка осталась бы в памяти после ухода клиента

	for {
		select {
		case <-ctx.Done():
			// Клиент ушёл или истёк его deadline: перестаём слать.
			return status.FromContextError(ctx.Err()).Err()
		case <-s.stopping:
			return status.Error(codes.Unavailable, "server is shutting down")
		case n, open := <-ch:
			if !open {
				return status.Error(codes.ResourceExhausted, "client reads too slowly, reconnect and list missed notifications")
			}
			if err := stream.Send(&notifierv1.WatchNotificationsResponse{Notification: toProto(n)}); err != nil {
				return err
			}
		}
	}
}

// toStatus переводит ошибку сервиса в код gRPC. Для внутренних ошибок клиент видит только
// «internal error»: текст ошибки базы ему не нужен и может выдать лишнее. Причина пишется в лог.
func (s *Server) toStatus(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, notifier.ErrBadRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		s.log.ErrorContext(ctx, "grpc handler failed", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}

var kinds = map[string]notifierv1.Kind{
	notification.KindTaskCreated: notifierv1.Kind_KIND_TASK_CREATED,
	notification.KindTaskDue:     notifierv1.Kind_KIND_TASK_DUE,
}

func toProto(n notification.Notification) *notifierv1.Notification {
	return &notifierv1.Notification{
		Id:        n.ID,
		UserId:    n.UserID,
		Kind:      kinds[n.Kind], // незнакомый вид станет KIND_UNSPECIFIED
		TaskId:    n.TaskID,
		Title:     n.Title,
		CreatedAt: timestamppb.New(n.CreatedAt),
	}
}
