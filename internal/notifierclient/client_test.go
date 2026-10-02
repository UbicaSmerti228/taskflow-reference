package notifierclient

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/UbicaSmerti228/taskflow-reference/internal/gen/notifierv1"
	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
	"github.com/UbicaSmerti228/taskflow-reference/internal/resilience"
)

// fakeNotifier — сервер, которым управляет тест: отвечает заданной ошибкой или с задержкой.
type fakeNotifier struct {
	notifierv1.UnimplementedNotifierServiceServer
	mu        sync.Mutex
	err       error
	delay     time.Duration
	calls     int
	lastReq   *notifierv1.ListNotificationsRequest
	requestID string
	deadline  time.Duration // сколько времени клиент дал на вызов
}

func (f *fakeNotifier) ListNotifications(ctx context.Context, req *notifierv1.ListNotificationsRequest) (*notifierv1.ListNotificationsResponse, error) {
	f.mu.Lock()
	f.calls++
	f.lastReq = req
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get(logging.MetadataRequestID)) > 0 {
		f.requestID = md.Get(logging.MetadataRequestID)[0]
	}
	if d, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(d)
	}
	err, delay := f.err, f.delay
	f.mu.Unlock()

	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err != nil {
		return nil, err
	}
	return &notifierv1.ListNotificationsResponse{Notifications: []*notifierv1.Notification{
		{Id: 2, UserId: req.GetUserId(), Kind: notifierv1.Kind_KIND_TASK_DUE, TaskId: 7, Title: "сдать отчёт", CreatedAt: timestamppb.New(time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC))},
		{Id: 1, UserId: req.GetUserId(), Kind: notifierv1.Kind(99), TaskId: 7, Title: "сдать отчёт"},
	}}, nil
}

func (f *fakeNotifier) set(err error, delay time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err, f.delay = err, delay
}

func (f *fakeNotifier) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func start(t *testing.T, opts ...Option) (*Client, *fakeNotifier) {
	t.Helper()
	fake := &fakeNotifier{}
	ln := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	notifierv1.RegisterNotifierServiceServer(srv, fake)
	go srv.Serve(ln) //nolint:errcheck // сервер останавливается в конце теста
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	c := NewWithConn(conn, opts...)
	t.Cleanup(func() { c.Close() }) //nolint:errcheck // тест уже завершён
	return c, fake
}

func TestList(t *testing.T) {
	c, fake := start(t)
	ctx := logging.WithRequestID(context.Background(), "req-7")

	got, err := c.List(ctx, 5, 10, 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("List() вернул %d уведомлений, want 2", len(got))
	}
	want := notification.Notification{ID: 2, UserID: 5, Kind: notification.KindTaskDue, TaskID: 7, Title: "сдать отчёт", CreatedAt: time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)}
	if got[0] != want {
		t.Errorf("уведомление = %+v, want %+v", got[0], want)
	}
	// Сервер новее клиента и прислал незнакомый вид: клиент не падает.
	if got[1].Kind != "unknown" {
		t.Errorf("незнакомый вид = %q, want unknown", got[1].Kind)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if r := fake.lastReq; r.GetUserId() != 5 || r.GetLimit() != 10 || r.GetBeforeId() != 30 {
		t.Errorf("запрос = %v", r)
	}
	if fake.requestID != "req-7" {
		t.Errorf("request id на сервере = %q, want req-7", fake.requestID)
	}
	// Deadline доехал до сервера: тот знает, сколько времени у него есть.
	if fake.deadline <= 0 || fake.deadline > 2*time.Second {
		t.Errorf("deadline на сервере = %v, want до 2 секунд", fake.deadline)
	}
}

func TestListErrors(t *testing.T) {
	tests := []struct {
		name            string
		err             error
		wantUnavailable bool
	}{
		{name: "сервис недоступен", err: status.Error(codes.Unavailable, "down"), wantUnavailable: true},
		{name: "сервис перегружен", err: status.Error(codes.ResourceExhausted, "busy"), wantUnavailable: true},
		{name: "внутренняя ошибка сервиса", err: status.Error(codes.Internal, "internal error")},
		{name: "неверный запрос", err: status.Error(codes.InvalidArgument, "bad limit")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, fake := start(t)
			fake.set(tt.err, 0)
			_, err := c.List(context.Background(), 5, 10, 0)
			if err == nil || errors.Is(err, notification.ErrUnavailable) != tt.wantUnavailable {
				t.Errorf("List() error = %v, want ErrUnavailable: %v", err, tt.wantUnavailable)
			}
		})
	}
}

// Сервис завис: клиент не ждёт дольше своего таймаута.
func TestListTimeout(t *testing.T) {
	c, fake := start(t, WithTimeout(50*time.Millisecond))
	fake.set(nil, time.Minute)

	start := time.Now()
	_, err := c.List(context.Background(), 5, 10, 0)
	if !errors.Is(err, notification.ErrUnavailable) {
		t.Errorf("List() error = %v, want ErrUnavailable", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("вызов занял %v, таймаут не сработал", took)
	}
}

// После серии отказов breaker размыкается: вызовы отклоняются сразу, до сервиса не доходят.
func TestListBreaker(t *testing.T) {
	b := resilience.NewBreaker(3, time.Hour)
	c, fake := start(t, WithBreaker(b))
	fake.set(status.Error(codes.Unavailable, "down"), 0)

	for range 3 {
		if _, err := c.List(context.Background(), 5, 10, 0); !errors.Is(err, notification.ErrUnavailable) {
			t.Fatalf("List() error = %v, want ErrUnavailable", err)
		}
	}
	for range 5 {
		_, err := c.List(context.Background(), 5, 10, 0)
		if !errors.Is(err, notification.ErrUnavailable) || !errors.Is(err, resilience.ErrOpen) {
			t.Fatalf("List() при разомкнутом breaker: error = %v, want ErrUnavailable и ErrOpen", err)
		}
	}
	if n := fake.callCount(); n != 3 {
		t.Errorf("до сервиса дошло %d вызовов, want 3", n)
	}
}

// Отказ «неверный запрос» означает, что сервис жив: breaker из-за него не размыкается.
func TestListRejectionDoesNotOpenBreaker(t *testing.T) {
	b := resilience.NewBreaker(2, time.Hour)
	c, fake := start(t, WithBreaker(b))
	fake.set(status.Error(codes.InvalidArgument, "bad"), 0)
	for range 5 {
		_, _ = c.List(context.Background(), 5, 10, 0)
	}
	if b.State() != resilience.StateClosed || fake.callCount() != 5 {
		t.Errorf("состояние %s, вызовов %d; want closed и 5", b.State(), fake.callCount())
	}
}

// Адрес, по которому никто не слушает: клиент создаётся, а вызов возвращает ErrUnavailable.
func TestNewWithoutServer(t *testing.T) {
	c, err := New("127.0.0.1:1", WithTimeout(500*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck // тест завершается
	if _, err := c.List(context.Background(), 5, 10, 0); !errors.Is(err, notification.ErrUnavailable) {
		t.Errorf("List() error = %v, want ErrUnavailable", err)
	}
}
