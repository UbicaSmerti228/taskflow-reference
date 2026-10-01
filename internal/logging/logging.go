// Package logging настраивает структурные логи и связывает их с запросом через request_id.
package logging

import (
	"context"
	"io"
	"log/slog"
)

type ctxKey struct{}

// WithRequestID кладёт идентификатор запроса в контекст.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// RequestID достаёт идентификатор запроса из контекста.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// New возвращает логгер, который пишет JSON в w.
// Если запись сделана через методы с контекстом (InfoContext и другие), в неё попадает request_id:
// сервисам и хранилищам не нужно передавать его руками.
func New(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(handler{slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})})
}

type handler struct{ slog.Handler }

func (h handler) Handle(ctx context.Context, r slog.Record) error {
	if id := RequestID(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return handler{h.Handler.WithAttrs(attrs)}
}

func (h handler) WithGroup(name string) slog.Handler {
	return handler{h.Handler.WithGroup(name)}
}
