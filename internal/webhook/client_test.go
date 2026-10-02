package webhook

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/resilience"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

var due = time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)

var sample = task.Task{ID: 7, Title: "сдать отчёт", DueAt: &due}

// received — один запрос, который дошёл до получателя.
type received struct {
	header http.Header
	body   []byte
}

// receiver — получатель вебхуков: отвечает статусами по очереди, последний повторяется.
type receiver struct {
	*httptest.Server
	mu       sync.Mutex
	statuses []int
	got      []received
}

func newReceiver(t *testing.T, statuses ...int) *receiver {
	t.Helper()
	r := &receiver{statuses: statuses}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		defer r.mu.Unlock()
		status := r.statuses[min(len(r.got), len(r.statuses)-1)]
		r.got = append(r.got, received{header: req.Header.Clone(), body: body})
		w.WriteHeader(status)
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *receiver) requests() []received {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]received(nil), r.got...)
}

// noWait — повторы без пауз, чтобы тесты не ждали.
func noWait(attempts int) Option {
	return WithRetry(resilience.Retry{Attempts: attempts, Sleep: func(context.Context, time.Duration) error { return nil }})
}

func TestNotifySendsSignedEvent(t *testing.T) {
	r := newReceiver(t, http.StatusNoContent)
	secret := []byte("секрет получателя")

	if err := New(r.URL, WithSecret(secret)).Notify(context.Background(), sample); err != nil {
		t.Fatal(err)
	}
	got := r.requests()
	if len(got) != 1 {
		t.Fatalf("запросов %d, want 1", len(got))
	}

	var ev Event
	if err := json.Unmarshal(got[0].body, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Event != "task.due" || ev.TaskID != 7 || ev.Title != "сдать отчёт" || ev.DueAt == nil || !ev.DueAt.Equal(due) {
		t.Errorf("событие = %+v", ev)
	}
	if ct := got[0].header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if id := got[0].header.Get(HeaderEventID); id != "task.due:7:1777629600" {
		t.Errorf("%s = %q", HeaderEventID, id)
	}
	// Получатель проверяет подпись так же: считает HMAC тела своим секретом.
	sig := strings.TrimPrefix(got[0].header.Get(HeaderSignature), "sha256=")
	if !hmac.Equal([]byte(sig), []byte(Sign(secret, got[0].body))) {
		t.Errorf("подпись %q не сходится с телом", sig)
	}
}

func TestNotifyWithoutSecretAndDueDate(t *testing.T) {
	r := newReceiver(t, http.StatusOK)
	if err := New(r.URL).Notify(context.Background(), task.Task{ID: 3, Title: "без срока"}); err != nil {
		t.Fatal(err)
	}
	got := r.requests()[0]
	if _, signed := got.header[HeaderSignature]; signed {
		t.Error("без секрета подписи быть не должно")
	}
	if id := got.header.Get(HeaderEventID); id != "task.due:3" {
		t.Errorf("%s = %q", HeaderEventID, id)
	}
}

func TestNotifyRetries(t *testing.T) {
	tests := []struct {
		name      string
		statuses  []int
		wantCalls int
		wantErr   string // пусто — успех
	}{
		{name: "успех с первого раза", statuses: []int{200}, wantCalls: 1},
		{name: "временный сбой, потом успех", statuses: []int{503, 500, 200}, wantCalls: 3},
		{name: "429 повторяется", statuses: []int{429, 204}, wantCalls: 2},
		{name: "попытки кончились", statuses: []int{502}, wantCalls: 3, wantErr: "status 502"},
		{name: "400 не повторяется", statuses: []int{400, 200}, wantCalls: 1, wantErr: "status 400"},
		{name: "404 не повторяется", statuses: []int{404, 200}, wantCalls: 1, wantErr: "status 404"},
		// Перенаправление не выполняется: адрес из настроек — единственное место, куда уходят данные.
		{name: "перенаправление не выполняется", statuses: []int{302, 200}, wantCalls: 1, wantErr: "status 302"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newReceiver(t, tt.statuses...)
			err := New(r.URL, noWait(3)).Notify(context.Background(), sample)
			if tt.wantErr == "" && err != nil {
				t.Errorf("err = %v, want nil", err)
			}
			if tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)) {
				t.Errorf("err = %v, want ошибку с %q", err, tt.wantErr)
			}
			got := r.requests()
			if len(got) != tt.wantCalls {
				t.Fatalf("запросов %d, want %d", len(got), tt.wantCalls)
			}
			// У всех повторов один идентификатор события: получатель отбросит дубликаты.
			for _, req := range got {
				if id := req.header.Get(HeaderEventID); id != got[0].header.Get(HeaderEventID) {
					t.Errorf("идентификатор повтора %q отличается от первого", id)
				}
			}
		})
	}
}

// Получатель завис: попытка обрывается по таймауту, а не держит воркер.
func TestNotifyTimeout(t *testing.T) {
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer slow.Close()
	defer close(release)

	start := time.Now()
	err := New(slow.URL, WithTimeout(50*time.Millisecond), noWait(2)).Notify(context.Background(), sample)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("две попытки заняли %v, таймаут не сработал", took)
	}
}

func TestNotifyUnreachable(t *testing.T) {
	r := newReceiver(t, 200)
	url := r.URL
	r.Close()
	if err := New(url, noWait(2)).Notify(context.Background(), sample); err == nil {
		t.Error("получатель недоступен: err = nil")
	}
	if err := New("://не адрес", noWait(3)).Notify(context.Background(), sample); err == nil {
		t.Error("неверный адрес: err = nil")
	}
}

// После серии отказов breaker размыкается: до получателя запросы больше не доходят.
func TestNotifyBreaker(t *testing.T) {
	r := newReceiver(t, http.StatusInternalServerError)
	c := New(r.URL, noWait(1), WithBreaker(resilience.NewBreaker(3, time.Hour)))

	for i := range 3 {
		if err := c.Notify(context.Background(), sample); err == nil || errors.Is(err, resilience.ErrOpen) {
			t.Fatalf("вызов %d: err = %v, want ошибку получателя", i+1, err)
		}
	}
	for range 5 {
		if err := c.Notify(context.Background(), sample); !errors.Is(err, resilience.ErrOpen) {
			t.Fatalf("err = %v, want ErrOpen", err)
		}
	}
	if n := len(r.requests()); n != 3 {
		t.Errorf("до получателя дошло %d запросов, want 3", n)
	}
}

// Отказ 4xx означает, что получатель жив: breaker из-за него не размыкается.
func TestNotifyRejectionDoesNotOpenBreaker(t *testing.T) {
	r := newReceiver(t, http.StatusBadRequest)
	b := resilience.NewBreaker(2, time.Hour)
	c := New(r.URL, noWait(3), WithBreaker(b))
	for range 5 {
		_ = c.Notify(context.Background(), sample)
	}
	if b.State() != resilience.StateClosed {
		t.Errorf("состояние %s, want closed", b.State())
	}
	if n := len(r.requests()); n != 5 {
		t.Errorf("запросов %d, want 5", n)
	}
}

// Повтор внутри одного вызова останавливается, как только breaker разомкнулся.
func TestNotifyStopsRetryingWhenBreakerOpens(t *testing.T) {
	r := newReceiver(t, http.StatusServiceUnavailable)
	c := New(r.URL, noWait(10), WithBreaker(resilience.NewBreaker(2, time.Hour)))
	if err := c.Notify(context.Background(), sample); !errors.Is(err, resilience.ErrOpen) {
		t.Errorf("err = %v, want ErrOpen", err)
	}
	if n := len(r.requests()); n != 2 {
		t.Errorf("запросов %d, want 2", n)
	}
}

func TestNotifyCancelledContext(t *testing.T) {
	r := newReceiver(t, 200)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := New(r.URL).Notify(ctx, sample); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestNotifyWithHTTPClient(t *testing.T) {
	calls := 0
	h := &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	})}
	if err := New("http://example.invalid/hook", WithHTTPClient(h)).Notify(context.Background(), sample); err != nil || calls != 1 {
		t.Errorf("err = %v, вызовов %d", err, calls)
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
