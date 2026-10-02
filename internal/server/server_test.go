package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewDefaultsAndOptions(t *testing.T) {
	s := New(http.NotFoundHandler())
	if s.addr != ":8080" || s.readTimeout != 10*time.Second || s.writeTimeout != 10*time.Second || s.shutdownTimeout != 10*time.Second {
		t.Errorf("значения по умолчанию = %+v", s)
	}

	s = New(http.NotFoundHandler(), WithAddr(":9000"), WithReadTimeout(time.Second), WithShutdownTimeout(3*time.Second))
	if s.addr != ":9000" || s.readTimeout != time.Second || s.shutdownTimeout != 3*time.Second {
		t.Errorf("опции не применились: %+v", s)
	}
	// Незаданная опция оставляет значение по умолчанию.
	if s.writeTimeout != 10*time.Second {
		t.Errorf("writeTimeout = %v, want 10s по умолчанию", s.writeTimeout)
	}
}

// freeAddr возвращает адрес со свободным портом.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // порт нужен только на мгновение
	return ln.Addr().String()
}

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

func waitUp(t *testing.T, addr string) {
	t.Helper()
	for range 200 {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close() //nolint:errcheck // проверяем только, что порт открыт
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("сервер не поднялся")
}

// Запрос, начатый до остановки, получает ответ: сервер ждёт его завершения.
func TestRunFinishesRequestsInFlight(t *testing.T) {
	addr, logs := freeAddr(t), &syncBuffer{}
	started, release := make(chan struct{}), make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		io.WriteString(w, "готово") //nolint:errcheck // тест
	})

	hooked := make(chan struct{})
	s := New(handler, WithAddr(addr), WithLogger(slog.New(slog.NewTextHandler(logs, nil))), WithShutdownHook(func() { close(hooked) }))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitUp(t, addr)

	body := make(chan string, 1)
	go func() {
		resp, err := http.Get("http://" + addr)
		if err != nil {
			body <- "ошибка: " + err.Error()
			return
		}
		defer resp.Body.Close() //nolint:errcheck // тест
		b, _ := io.ReadAll(resp.Body)
		body <- string(b)
	}()

	<-started
	cancel() // остановка начинается, пока запрос ещё обрабатывается
	<-hooked
	select {
	case err := <-done:
		t.Fatalf("Run вернулся (%v), не дождавшись запроса", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if got := <-body; got != "готово" {
		t.Errorf("ответ = %q, want «готово»", got)
	}
	if err := <-done; err != nil {
		t.Errorf("Run вернул %v, want nil", err)
	}
	for _, want := range []string{"server started", "shutting down", "server stopped"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("в логе нет записи %q", want)
		}
	}
}

// Зависший запрос не держит остановку дольше таймаута.
func TestRunShutdownTimeout(t *testing.T) {
	addr := freeAddr(t)
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release
	})
	s := New(handler, WithAddr(addr), WithShutdownTimeout(50*time.Millisecond), WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	waitUp(t, addr)
	go http.Get("http://" + addr) //nolint:errcheck // ответ не важен

	<-started
	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "shutdown") {
			t.Errorf("Run вернул %v, want ошибку остановки по таймауту", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не вернулся: таймаут остановки не сработал")
	}
}

func TestRunBusyPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // тест завершается

	err = New(http.NotFoundHandler(), WithAddr(ln.Addr().String())).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Errorf("Run на занятом порту вернул %v, want ошибку listen", err)
	}
}
