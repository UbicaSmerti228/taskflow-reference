// Команда notifier — сервис уведомлений: читает события задач из Kafka, хранит уведомления
// и отдаёт их по gRPC.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/UbicaSmerti228/taskflow-reference/internal/admin"
	"github.com/UbicaSmerti228/taskflow-reference/internal/config"
	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
	"github.com/UbicaSmerti228/taskflow-reference/internal/kafka"
	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/metrics"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notifier"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notifier/grpcserver"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notifier/pgstore"
	"github.com/UbicaSmerti228/taskflow-reference/internal/server"
)

const usage = `Использование:
  notifier serve        накатить миграции, запустить чтение событий и gRPC-сервер
  notifier migrate      только накатить миграции
  notifier healthcheck  проверить, что запущенный сервис отвечает (для healthcheck в Docker)

Настройки берутся из переменных окружения:
  DATABASE_URL     адрес PostgreSQL, обязательна; таблицы лежат в схеме notifier
  KAFKA_BROKERS    адреса брокеров Kafka через запятую, обязательна
  KAFKA_GROUP      consumer group, по умолчанию notifier
  GRPC_ADDR        адрес gRPC-сервера, по умолчанию :9000
  ADMIN_ADDR       адрес служебного сервера с /metrics и /debug/pprof, по умолчанию :9001
  LOG_LEVEL        debug, info, warn или error; по умолчанию info
  SHUTDOWN_TIMEOUT сколько ждать текущие вызовы при остановке, по умолчанию 10s`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "ошибка:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	if len(args) != 1 {
		return errors.New("нужна одна команда\n" + usage)
	}
	switch args[0] {
	case "serve":
		cfg, err := config.LoadNotifier(getenv)
		if err != nil {
			return fmt.Errorf("настройки:\n%w", err)
		}
		return serve(ctx, cfg, out)

	case "migrate":
		dsn := getenv("DATABASE_URL")
		if dsn == "" {
			return errors.New("не задана переменная DATABASE_URL")
		}
		if err := pgstore.Migrate(ctx, dsn); err != nil {
			return err
		}
		_, err := fmt.Fprintln(out, "Миграции применены")
		return err

	case "healthcheck":
		return healthcheck(ctx, getenv("GRPC_ADDR"))

	default:
		return fmt.Errorf("неизвестная команда %q\n%s", args[0], usage)
	}
}

func serve(ctx context.Context, cfg config.Notifier, out io.Writer) error {
	log := logging.New(out, cfg.LogLevel)

	if err := pgstore.Migrate(ctx, cfg.DatabaseURL); err != nil {
		return err
	}
	pool, err := pgstore.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	return runService(ctx, cfg, log, pgstore.New(pool))
}

// runService собирает сервис и работает, пока не отменён ctx. В тестах сюда приходит хранилище в памяти.
func runService(ctx context.Context, cfg config.Notifier, log *slog.Logger, store notifier.Store) error {
	reg := metrics.NewRegistry()
	hub := notifier.NewHub()
	svc := notifier.New(store, hub, log, metrics.NewEvents(reg))
	metrics.RegisterGauge(reg, "notifier_stream_subscribers", "Число открытых стримов WatchNotifications.",
		func() float64 { return float64(hub.Subscribers()) })

	// Сервис уведомлений не знает про Kafka, читатель Kafka не знает про уведомления. Связывает их эта функция:
	// она же решает, какое сообщение пропустить, а какое подать снова.
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaGroup, event.Topic, event.TopicPartitions,
		func(ctx context.Context, m kafka.Message) error {
			err := svc.Handle(ctx, m.Value)
			if errors.Is(err, notifier.ErrMalformed) {
				return kafka.Skip(err) // битое событие повтор не исправит
			}
			return err
		}, log)
	if err != nil {
		return err
	}
	defer consumer.Close()

	api := grpcserver.New(svc, log,
		grpcserver.WithMetrics(metrics.NewGRPC(reg)),
		grpcserver.WithShutdownTimeout(cfg.ShutdownTimeout),
	)
	internal := server.New(admin.Handler(reg),
		server.WithAddr(cfg.AdminAddr),
		server.WithLogger(log.With("server", "admin")),
		server.WithShutdownTimeout(cfg.ShutdownTimeout),
		server.WithWriteTimeout(2*time.Minute), // CPU-профиль снимается 30 секунд и дольше
	)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return api.Run(ctx, cfg.GRPCAddr) })
	g.Go(func() error { return internal.Run(ctx) })
	g.Go(func() error { consumer.Run(ctx); return nil })
	return g.Wait()
}

// healthcheck спрашивает у сервиса на этой же машине стандартную gRPC-проверку здоровья.
// В образе нет ни оболочки, ни grpc_health_probe, поэтому проверку делает сам бинарник.
func healthcheck(ctx context.Context, addr string) error {
	if addr == "" {
		addr = ":9000"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck // проверка закончена
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("сервис не готов: %s", resp.GetStatus())
	}
	return nil
}
