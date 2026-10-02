package grpcserver

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
	"github.com/UbicaSmerti228/taskflow-reference/internal/gen/notifierv1"
	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/memory"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notifier"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// syncBuffer — буфер логов, в который сервер пишет из своих горутин.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// calls — метрики, которые можно прочитать в тесте.
type calls struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *calls) Call(method, code string, _ time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[method[strings.LastIndex(method, "/")+1:]+"/"+code]++
}

func (c *calls) count(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[key]
}

// env — сервер на настоящем сервисе и хранилище в памяти. Сеть заменена на bufconn:
// клиент и сервер говорят по настоящему gRPC, но внутри процесса.
type env struct {
	client  notifierv1.NotifierServiceClient
	health  healthpb.HealthClient
	svc     *notifier.Service
	store   *memory.Notifications
	hub     *notifier.Hub
	logs    *syncBuffer
	metrics *calls
	stop    func() error
}

func start(t *testing.T, svc Notifications, opts ...Option) *env {
	t.Helper()
	e := &env{store: memory.NewNotifications(), hub: notifier.NewHub(), logs: &syncBuffer{}, metrics: &calls{}}
	log := logging.New(e.logs, slog.LevelDebug)
	e.svc = notifier.New(e.store, e.hub, log, nil)
	if svc == nil {
		svc = e.svc
	}

	ln := bufconn.Listen(1 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(svc, log, append([]Option{WithMetrics(e.metrics)}, opts...)...).Serve(ctx, ln) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	e.client, e.health = notifierv1.NewNotifierServiceClient(conn), healthpb.NewHealthClient(conn)

	stopped := false
	e.stop = func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			return errors.New("сервер не остановился за 10 секунд")
		}
	}
	t.Cleanup(func() {
		e.stop()     //nolint:errcheck // тест уже завершён
		conn.Close() //nolint:errcheck // тест уже завершён
	})
	return e
}

func (e *env) add(t *testing.T, userID, taskID int64) {
	t.Helper()
	rec, err := event.TaskCreated(userID, task.Task{ID: taskID, Title: "сдать отчёт", CreatedAt: time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)}).Record()
	if err != nil {
		t.Fatal(err)
	}
	if err := e.svc.Handle(context.Background(), rec.Payload); err != nil {
		t.Fatal(err)
	}
}

func ctxTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestListNotifications(t *testing.T) {
	e := start(t, nil)
	for id := int64(1); id <= 3; id++ {
		e.add(t, 5, id)
	}
	e.add(t, 6, 100) // чужое

	resp, err := e.client.ListNotifications(ctxTimeout(t), &notifierv1.ListNotificationsRequest{UserId: 5, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	got := resp.GetNotifications()
	if len(got) != 2 || got[0].GetTaskId() != 3 || got[1].GetTaskId() != 2 {
		t.Fatalf("ListNotifications() = %v, want задачи 3 и 2", got)
	}
	n := got[0]
	if n.GetUserId() != 5 || n.GetKind() != notifierv1.Kind_KIND_TASK_CREATED || n.GetTitle() != "сдать отчёт" ||
		!n.GetCreatedAt().AsTime().Equal(time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("уведомление = %v", n)
	}

	// Следующая страница — уведомления старше последнего полученного.
	resp, err = e.client.ListNotifications(ctxTimeout(t), &notifierv1.ListNotificationsRequest{UserId: 5, Limit: 2, BeforeId: got[1].GetId()})
	if err != nil || len(resp.GetNotifications()) != 1 || resp.GetNotifications()[0].GetTaskId() != 1 {
		t.Errorf("вторая страница = %v, %v; want задачу 1", resp.GetNotifications(), err)
	}
}

// Ошибки возвращаются кодами gRPC, а не текстом: клиент решает, что делать, по коду.
func TestListNotificationsErrors(t *testing.T) {
	e := start(t, nil)
	tests := []struct {
		name string
		req  *notifierv1.ListNotificationsRequest
		want codes.Code
	}{
		{name: "нет пользователя", req: &notifierv1.ListNotificationsRequest{}, want: codes.InvalidArgument},
		{name: "слишком большой limit", req: &notifierv1.ListNotificationsRequest{UserId: 1, Limit: 1000}, want: codes.InvalidArgument},
		{name: "отрицательный before_id", req: &notifierv1.ListNotificationsRequest{UserId: 1, BeforeId: -1}, want: codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := e.client.ListNotifications(ctxTimeout(t), tt.req)
			if status.Code(err) != tt.want {
				t.Errorf("код = %v (%v), want %v", status.Code(err), err, tt.want)
			}
		})
	}

	// Сбой хранилища: клиент видит Internal без подробностей, причина — только в логе сервера.
	e.store.Err = errors.New("connection refused: password=secret")
	_, err := e.client.ListNotifications(ctxTimeout(t), &notifierv1.ListNotificationsRequest{UserId: 1})
	if st := status.Convert(err); st.Code() != codes.Internal || st.Message() != "internal error" {
		t.Errorf("сбой хранилища: %v, want Internal «internal error»", err)
	}
	if !strings.Contains(e.logs.String(), "connection refused") {
		t.Error("причина сбоя не попала в лог сервера")
	}
	if e.metrics.count("ListNotifications/InvalidArgument") != 3 || e.metrics.count("ListNotifications/Internal") != 1 {
		t.Errorf("метрики = %v", e.metrics.n)
	}
}

func TestWatchNotifications(t *testing.T) {
	e := start(t, nil)
	ctx, cancel := context.WithCancel(ctxTimeout(t))
	stream, err := e.client.WatchNotifications(ctx, &notifierv1.WatchNotificationsRequest{UserId: 5})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "подписка на сервере", func() bool { return e.hub.Subscribers() == 1 })

	e.add(t, 6, 100) // чужое уведомление в стрим не попадает
	e.add(t, 5, 1)
	e.add(t, 5, 2)
	for _, want := range []int64{1, 2} {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if got := resp.GetNotification().GetTaskId(); got != want {
			t.Errorf("из стрима пришла задача %d, want %d", got, want)
		}
	}

	// Клиент закрыл стрим: сервер перестаёт слать и освобождает подписку.
	cancel()
	if _, err := stream.Recv(); status.Code(err) != codes.Canceled {
		t.Errorf("после отмены Recv() error = %v, want Canceled", err)
	}
	waitFor(t, "отписка на сервере", func() bool { return e.hub.Subscribers() == 0 })
}

// Клиент ставит deadline на стрим: сервер закрывает его сам, когда время вышло.
func TestWatchNotificationsDeadline(t *testing.T) {
	e := start(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	stream, err := e.client.WatchNotifications(ctx, &notifierv1.WatchNotificationsRequest{UserId: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("Recv() error = %v, want DeadlineExceeded", err)
	}
	waitFor(t, "отписка на сервере", func() bool { return e.hub.Subscribers() == 0 })
}

func TestWatchNotificationsErrors(t *testing.T) {
	e := start(t, nil)
	stream, err := e.client.WatchNotifications(ctxTimeout(t), &notifierv1.WatchNotificationsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("стрим без пользователя: error = %v, want InvalidArgument", err)
	}

	// Клиент не читает: сервер не копит уведомления без конца, а закрывает стрим.
	stream, err = e.client.WatchNotifications(ctxTimeout(t), &notifierv1.WatchNotificationsRequest{UserId: 5})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "подписка на сервере", func() bool { return e.hub.Subscribers() == 1 })
	// Обработчик стрима заблокирован в Send не будет (буфер bufconn большой), поэтому
	// переполняем подписку напрямую: публикуем быстрее, чем обработчик успевает забирать.
	for i := range 10_000 {
		e.hub.Publish(notification.Notification{ID: int64(i + 1), UserID: 5})
		if e.hub.Subscribers() == 0 {
			break
		}
	}
	for {
		_, err := stream.Recv()
		if err == nil {
			continue
		}
		if status.Code(err) != codes.ResourceExhausted {
			t.Errorf("медленный клиент: error = %v, want ResourceExhausted", err)
		}
		break
	}
}

// Остановка сервера: открытые стримы закрываются с Unavailable, сервер завершается без ошибки.
func TestShutdownClosesStreams(t *testing.T) {
	e := start(t, nil)
	stream, err := e.client.WatchNotifications(ctxTimeout(t), &notifierv1.WatchNotificationsRequest{UserId: 5})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "подписка на сервере", func() bool { return e.hub.Subscribers() == 1 })

	if resp, err := e.health.Check(ctxTimeout(t), &healthpb.HealthCheckRequest{}); err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("health до остановки = %v, %v; want SERVING", resp.GetStatus(), err)
	}

	stopErr := make(chan error, 1)
	go func() { stopErr <- e.stop() }()
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Errorf("стрим при остановке: error = %v, want Unavailable", err)
	}
	if err := <-stopErr; err != nil {
		t.Errorf("остановка: %v", err)
	}
	out := e.logs.String()
	for _, want := range []string{`"msg":"grpc server started"`, `"msg":"grpc server shutting down"`, `"msg":"grpc server stopped"`} {
		if !strings.Contains(out, want) {
			t.Errorf("в логе нет записи %s", want)
		}
	}
}

// stuck — сервис, чей List не возвращается, пока тест не разрешит: изображает зависший вызов.
type stuck struct {
	Notifications
	release chan struct{}
	entered chan struct{}
}

func (s *stuck) List(context.Context, int64, int, int64) ([]notification.Notification, error) {
	close(s.entered)
	<-s.release
	return nil, nil
}

// Вызов не укладывается в таймаут остановки: сервер обрывает соединения и сообщает об этом.
func TestShutdownTimeout(t *testing.T) {
	s := &stuck{release: make(chan struct{}), entered: make(chan struct{})}
	e := start(t, s, WithShutdownTimeout(50*time.Millisecond))
	defer close(s.release)

	callErr := make(chan error, 1)
	go func() {
		_, err := e.client.ListNotifications(ctxTimeout(t), &notifierv1.ListNotificationsRequest{UserId: 1})
		callErr <- err
	}()
	<-s.entered

	if err := e.stop(); err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Errorf("остановка: error = %v, want ошибку о таймауте", err)
	}
	if err := <-callErr; status.Code(err) != codes.Unavailable {
		t.Errorf("оборванный вызов: error = %v, want Unavailable", err)
	}
}

// panicky — сервис, который паникует.
type panicky struct{ Notifications }

func (panicky) List(context.Context, int64, int, int64) ([]notification.Notification, error) {
	panic("что-то пошло не так")
}

func (panicky) Subscribe(int64) (<-chan notification.Notification, func(), error) {
	panic("что-то пошло не так")
}

// Паника в обработчике не роняет сервер: вызов получает Internal, следующий вызов работает.
func TestPanicIsRecovered(t *testing.T) {
	e := start(t, panicky{})
	if _, err := e.client.ListNotifications(ctxTimeout(t), &notifierv1.ListNotificationsRequest{UserId: 1}); status.Code(err) != codes.Internal {
		t.Errorf("паника в обычном вызове: error = %v, want Internal", err)
	}
	stream, err := e.client.WatchNotifications(ctxTimeout(t), &notifierv1.WatchNotificationsRequest{UserId: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Internal {
		t.Errorf("паника в стриме: error = %v, want Internal", err)
	}
	if resp, err := e.health.Check(ctxTimeout(t), &healthpb.HealthCheckRequest{}); err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("после паники сервер не отвечает: %v, %v", resp.GetStatus(), err)
	}
	if !strings.Contains(e.logs.String(), `"msg":"panic in grpc handler"`) {
		t.Error("паника не записана в лог")
	}
}

// Идентификатор запроса из метаданных попадает в логи сервера.
func TestRequestIDFromMetadata(t *testing.T) {
	e := start(t, nil)
	ctx := metadata.AppendToOutgoingContext(ctxTimeout(t), logging.MetadataRequestID, "req-42")
	if _, err := e.client.ListNotifications(ctx, &notifierv1.ListNotificationsRequest{UserId: 1}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "запись о вызове в логе", func() bool {
		return strings.Contains(e.logs.String(), `"msg":"grpc call","method":"/taskflow.notifier.v1.NotifierService/ListNotifications","code":"OK"`)
	})
	if !strings.Contains(e.logs.String(), `"request_id":"req-42"`) {
		t.Errorf("в логе нет request_id\n%s", e.logs)
	}
}

func TestRunListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // тест завершается
	srv := New(notifier.New(memory.NewNotifications(), notifier.NewHub(), slog.New(slog.DiscardHandler), nil), slog.New(slog.DiscardHandler))
	if err := srv.Run(context.Background(), ln.Addr().String()); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Errorf("занятый порт: error = %v, want ошибку listen", err)
	}

	// На свободном порту сервер поднимается и останавливается по отмене контекста.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, "127.0.0.1:0") }()
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() error = %v", err)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
