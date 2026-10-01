// Команда taskflow — сервис трекера задач: HTTP API, воркер напоминаний и миграции базы.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/config"
	"github.com/UbicaSmerti228/taskflow-reference/internal/httpapi"
	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/postgres"
	"github.com/UbicaSmerti228/taskflow-reference/internal/redisstore"
	"github.com/UbicaSmerti228/taskflow-reference/internal/reminder"
	"github.com/UbicaSmerti228/taskflow-reference/internal/service"
)

const usage = `Использование:
  taskflow serve        накатить миграции, запустить HTTP-сервер и воркер напоминаний
  taskflow migrate      только накатить миграции
  taskflow healthcheck  проверить, что запущенный сервис готов (для healthcheck в Docker)

Настройки берутся из переменных окружения:
  DATABASE_URL     адрес PostgreSQL, обязательна
  REDIS_URL        адрес Redis, обязательна
  JWT_SECRET       ключ подписи токенов, не меньше 32 байт, обязательна
  ADDR             адрес сервера, по умолчанию :8080
  LOG_LEVEL        debug, info, warn или error; по умолчанию info
  ACCESS_TTL       срок жизни access-токена, по умолчанию 15m
  REFRESH_TTL      срок жизни refresh-токена, по умолчанию 720h
  CACHE_TTL        срок жизни задачи в кэше, по умолчанию 1m
  LOGIN_LIMIT      попыток входа в минуту с одного IP, по умолчанию 10
  REMIND_INTERVAL  как часто искать просроченные задачи, по умолчанию 30s
  REMIND_WORKERS   сколько напоминаний отправлять одновременно, по умолчанию 4
  TRUSTED_PROXIES  адреса прокси через запятую, чьему X-Forwarded-For можно верить`

func main() {
	// SIGINT и SIGTERM отменяют контекст: сервер успевает завершить текущие запросы.
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
		cfg, err := config.Load(getenv)
		if err != nil {
			// Без настроек не стартуем: лучше упасть сразу с понятной ошибкой, чем на первом запросе.
			return fmt.Errorf("настройки:\n%w", err)
		}
		return serve(ctx, cfg, out)

	case "migrate":
		dsn := getenv("DATABASE_URL")
		if dsn == "" {
			return errors.New("не задана переменная DATABASE_URL")
		}
		if err := postgres.Migrate(ctx, dsn); err != nil {
			return err
		}
		_, err := fmt.Fprintln(out, "Миграции применены")
		return err

	case "healthcheck":
		return healthcheck(ctx, getenv("ADDR"))

	default:
		return fmt.Errorf("неизвестная команда %q\n%s", args[0], usage)
	}
}

// serve подключается к базе и Redis и запускает сервис. Это место, где собираются все зависимости:
// связи между слоями видны целиком, а сами слои знают только про интерфейсы.
func serve(ctx context.Context, cfg config.Config, out io.Writer) error {
	log := logging.New(out, cfg.LogLevel)

	// Схема обновляется при каждом старте: отдельный шаг деплоя не нужен.
	if err := postgres.Migrate(ctx, cfg.DatabaseURL); err != nil {
		return err
	}
	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	kv, err := redisstore.Connect(ctx, cfg.RedisURL)
	if err != nil {
		return err
	}
	defer kv.Close() //nolint:errcheck // процесс завершается

	tasks := postgres.NewTasks(pool)
	return runService(ctx, cfg, log, components{
		taskRepo:  tasks,
		userRepo:  postgres.NewUsers(pool),
		reminders: tasks,
		kv:        kv,
		ready: map[string]httpapi.Check{
			"postgres": pool.Ping,
			"redis":    kv.Ping,
		},
	})
}

// kvStore — всё, что сервису нужно от Redis.
type kvStore interface {
	service.Cache
	service.RefreshStore
	httpapi.Limiter
}

// components — хранилища, на которых работает сервис. В тестах сюда попадают хранилища в памяти.
type components struct {
	taskRepo  service.TaskRepo
	userRepo  service.UserRepo
	reminders reminder.Store
	kv        kvStore
	ready     map[string]httpapi.Check
}

// runService собирает слои и работает, пока не отменён ctx.
func runService(ctx context.Context, cfg config.Config, log *slog.Logger, c components) error {
	auth, err := service.NewAuth(c.userRepo, c.kv, service.AuthConfig{
		Secret:     cfg.JWTSecret,
		AccessTTL:  cfg.AccessTTL,
		RefreshTTL: cfg.RefreshTTL,
	})
	if err != nil {
		return err
	}
	api, err := httpapi.New(httpapi.Deps{
		Tasks:          service.NewTasks(c.taskRepo, c.kv, cfg.CacheTTL, log),
		Auth:           auth,
		Limiter:        c.kv,
		Log:            log,
		Ready:          c.ready,
		LoginLimit:     cfg.LoginLimit,
		TrustedProxies: cfg.TrustedProxies,
	})
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Addr, err)
	}

	ctx, stop := context.WithCancel(ctx)
	defer stop()

	var bg sync.WaitGroup
	bg.Go(func() {
		reminder.New(c.reminders, reminder.LogNotifier{Log: log}, cfg.RemindInterval, cfg.RemindWorkers, log).Run(ctx)
	})

	srv := &http.Server{
		Handler:           api,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       time.Minute,
	}
	failed := make(chan error, 1)
	go func() { failed <- srv.Serve(ln) }()
	log.Info("server started", "addr", ln.Addr().String())

	select {
	case err := <-failed:
		stop()
		bg.Wait()
		return err
	case <-ctx.Done():
	}

	// Остановка по порядку: перестаём называться готовыми, дообрабатываем текущие запросы, ждём воркер.
	// Соединения с базой и Redis закроются после возврата.
	log.Info("shutting down")
	api.StartShutdown()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	bg.Wait()
	log.Info("server stopped")
	return err
}

// healthcheck спрашивает /readyz у сервиса на этой же машине.
// В образе нет ни оболочки, ни curl, поэтому проверку делает сам бинарник.
func healthcheck(ctx context.Context, addr string) error {
	if addr == "" {
		addr = ":8080"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/readyz", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck // тело не читаем
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("сервис не готов: /readyz ответил %d", resp.StatusCode)
	}
	return nil
}
