package grpcserver

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
)

// unary оборачивает обычные вызовы: связывает лог с запросом, ловит панику, пишет лог и метрики.
func (s *Server) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	ctx = withRequestID(ctx)
	defer s.observe(ctx, info.FullMethod, time.Now(), &err)
	return handler(ctx, req)
}

// stream — то же для стримов. Контекст стрима нельзя заменить напрямую, поэтому стрим оборачивается.
func (s *Server) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	ctx := withRequestID(ss.Context())
	defer s.observe(ctx, info.FullMethod, time.Now(), &err)
	return handler(srv, &streamWithContext{ServerStream: ss, ctx: ctx})
}

type streamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *streamWithContext) Context() context.Context { return s.ctx }

func withRequestID(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ids := md.Get(logging.MetadataRequestID); len(ids) > 0 && ids[0] != "" {
			return logging.WithRequestID(ctx, ids[0])
		}
	}
	return ctx
}

// observe вызывается через defer после обработчика: превращает панику в codes.Internal,
// пишет запись в лог и метрики.
func (s *Server) observe(ctx context.Context, method string, start time.Time, errp *error) {
	if p := recover(); p != nil {
		// Без этого паника в одном вызове уронила бы весь процесс.
		s.log.ErrorContext(ctx, "panic in grpc handler", "method", method, "panic", p, "stack", string(debug.Stack()))
		*errp = status.Error(codes.Internal, "internal error")
	}
	took := time.Since(start)
	code := status.Code(*errp)
	s.metrics.Call(method, code.String(), took)

	level := slog.LevelInfo
	switch code {
	case codes.Internal, codes.Unknown, codes.DataLoss:
		level = slog.LevelError
	case codes.OK, codes.Canceled:
	default:
		level = slog.LevelWarn
	}
	s.log.Log(ctx, level, "grpc call", "method", method, "code", code.String(), "duration_ms", took.Milliseconds())
}
