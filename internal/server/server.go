// Package server запускает HTTP-сервер и останавливает его без потери запросов.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server — HTTP-сервер с настройками по умолчанию, которые меняются опциями.
type Server struct {
	handler         http.Handler
	addr            string
	readTimeout     time.Duration
	writeTimeout    time.Duration
	shutdownTimeout time.Duration
	log             *slog.Logger
	onShutdown      func()
}

// Option меняет одну настройку сервера. Это функциональные опции:
// конструктор остаётся коротким, а новые настройки добавляются без изменения его сигнатуры.
type Option func(*Server)

// WithAddr задаёт адрес, на котором слушает сервер.
func WithAddr(addr string) Option { return func(s *Server) { s.addr = addr } }

// WithReadTimeout ограничивает время чтения запроса.
func WithReadTimeout(d time.Duration) Option { return func(s *Server) { s.readTimeout = d } }

// WithWriteTimeout ограничивает время записи ответа.
func WithWriteTimeout(d time.Duration) Option { return func(s *Server) { s.writeTimeout = d } }

// WithShutdownTimeout задаёт, сколько ждать текущие запросы при остановке.
func WithShutdownTimeout(d time.Duration) Option { return func(s *Server) { s.shutdownTimeout = d } }

// WithLogger задаёт логгер.
func WithLogger(log *slog.Logger) Option { return func(s *Server) { s.log = log } }

// WithShutdownHook задаёт функцию, которая вызывается в начале остановки —
// например, чтобы перевести readiness-пробу в 503.
func WithShutdownHook(fn func()) Option { return func(s *Server) { s.onShutdown = fn } }

// New собирает сервер: сначала значения по умолчанию, потом опции.
func New(handler http.Handler, opts ...Option) *Server {
	s := &Server{
		handler:         handler,
		addr:            ":8080",
		readTimeout:     10 * time.Second,
		writeTimeout:    10 * time.Second,
		shutdownTimeout: 10 * time.Second,
		log:             slog.Default(),
		onShutdown:      func() {},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Run слушает адрес и работает, пока не отменён ctx. После отмены сервер перестаёт принимать
// новые соединения и ждёт завершения текущих запросов не дольше таймаута остановки.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}

	srv := &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: 5 * time.Second, // без него медленный клиент держит соединение сколько угодно
		ReadTimeout:       s.readTimeout,
		WriteTimeout:      s.writeTimeout,
		IdleTimeout:       time.Minute,
	}
	failed := make(chan error, 1)
	go func() { failed <- srv.Serve(ln) }()
	s.log.Info("server started", "addr", ln.Addr().String())

	select {
	case err := <-failed:
		return err
	case <-ctx.Done():
	}

	s.log.Info("shutting down")
	s.onShutdown()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	if err := <-failed; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	s.log.Info("server stopped")
	return nil
}
