// Команда taskflow — сервис трекера задач: HTTP API, воркер напоминаний и миграции базы.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/UbicaSmerti228/taskflow-reference/internal/admin"
	"github.com/UbicaSmerti228/taskflow-reference/internal/config"
	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
	"github.com/UbicaSmerti228/taskflow-reference/internal/httpapi"
	"github.com/UbicaSmerti228/taskflow-reference/internal/instrument"
	"github.com/UbicaSmerti228/taskflow-reference/internal/kafka"
	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/metrics"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notifierclient"
	"github.com/UbicaSmerti228/taskflow-reference/internal/outbox"
	"github.com/UbicaSmerti228/taskflow-reference/internal/postgres"
	"github.com/UbicaSmerti228/taskflow-reference/internal/redisstore"
	"github.com/UbicaSmerti228/taskflow-reference/internal/reminder"
	"github.com/UbicaSmerti228/taskflow-reference/internal/server"
	"github.com/UbicaSmerti228/taskflow-reference/internal/service"
	"github.com/UbicaSmerti228/taskflow-reference/internal/webhook"
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
  TRUSTED_PROXIES  адреса прокси через запятую, чьему X-Forwarded-For можно верить
  SHUTDOWN_TIMEOUT сколько ждать текущие запросы при остановке, по умолчанию 10s
  SLOW_QUERY       запрос к базе дольше этого попадает в лог как медленный, по умолчанию 200ms
  WEBHOOK_URL      куда отправлять напоминания; без неё они пишутся в лог
  WEBHOOK_SECRET   ключ подписи тела вебхука (заголовок X-Taskflow-Signature)
  WEBHOOK_TIMEOUT  время на одну попытку отправки вебхука, по умолчанию 3s
  KAFKA_BROKERS    адреса брокеров Kafka через запятую, обязательна
  OUTBOX_INTERVAL  как часто переносить события из outbox в Kafka, по умолчанию 1s
  NOTIFIER_ADDR    адрес gRPC-сервера notifier, обязательна
  NOTIFIER_TIMEOUT сколько ждать ответа notifier, по умолчанию 2s
  ADMIN_ADDR       адрес служебного сервера с /metrics и /debug/pprof, по умолчанию :8081`

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

// serve подключается к базе, Redis, Kafka и notifier и запускает сервис. Это место, где собираются
// все зависимости: связи между слоями видны целиком, а сами слои знают только про интерфейсы.
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

	// Клиенты Kafka и notifier соединяются при первом обращении. Без базы и Redis сервис не стартует,
	// а без этих двоих — стартует: события подождут в outbox, а уведомления ответят 503.
	producer, err := kafka.NewProducer(cfg.KafkaBrokers, event.TopicPartitions)
	if err != nil {
		return err
	}
	defer producer.Close()

	notifications, err := notifierclient.New(cfg.NotifierAddr, notifierclient.WithTimeout(cfg.NotifierTimeout))
	if err != nil {
		return err
	}
	defer notifications.Close() //nolint:errcheck // процесс завершается

	tasks := postgres.NewTasks(pool)
	return runService(ctx, cfg, log, components{
		taskRepo:      tasks,
		userRepo:      postgres.NewUsers(pool),
		reminders:     tasks,
		kv:            kv,
		outbox:        postgres.NewOutbox(pool),
		publisher:     producer,
		notifications: notifications,
		// В готовность входят только зависимости, без которых сервис не может отвечать.
		// Kafka и notifier сюда не входят: при их падении задачи продолжают работать.
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

// components — хранилища и клиенты, на которых работает сервис. В тестах сюда попадают реализации в памяти.
type components struct {
	taskRepo      service.TaskRepo
	userRepo      service.UserRepo
	reminders     reminder.Store
	kv            kvStore
	outbox        outbox.Store
	publisher     outbox.Publisher
	notifications httpapi.NotificationService
	ready         map[string]httpapi.Check
}

// runService собирает слои и работает, пока не отменён ctx.
func runService(ctx context.Context, cfg config.Config, log *slog.Logger, c components) error {
	reg := metrics.NewRegistry()

	auth, err := service.NewAuth(c.userRepo, c.kv, service.AuthConfig{
		Secret:     cfg.JWTSecret,
		AccessTTL:  cfg.AccessTTL,
		RefreshTTL: cfg.RefreshTTL,
	})
	if err != nil {
		return err
	}
	// Хранилище задач обёрнуто декоратором: сервис получает тот же интерфейс, а в логах и метриках появляется время запросов.
	taskRepo := instrument.Tasks(c.taskRepo, log, cfg.SlowQuery, metrics.NewRepo(reg))
	api, err := httpapi.New(httpapi.Deps{
		Tasks:          service.NewTasks(taskRepo, c.kv, cfg.CacheTTL, log),
		Auth:           auth,
		Notifications:  c.notifications,
		Limiter:        c.kv,
		Log:            log,
		Metrics:        metrics.NewHTTP(reg),
		Ready:          c.ready,
		LoginLimit:     cfg.LoginLimit,
		TrustedProxies: cfg.TrustedProxies,
	})
	if err != nil {
		return err
	}

	// Напоминания уходят вебхуком, если задан адрес, иначе пишутся в лог.
	// Воркер знает только интерфейс reminder.Notifier и не замечает разницы.
	// Событие «срок наступил» для notifier хранилище записывает само, вместе с отметкой о напоминании.
	var notifier reminder.Notifier = reminder.LogNotifier{Log: log}
	if cfg.WebhookURL != "" {
		notifier = webhook.New(cfg.WebhookURL, webhook.WithSecret(cfg.WebhookSecret), webhook.WithTimeout(cfg.WebhookTimeout))
	}
	reminders := reminder.New(c.reminders, notifier, cfg.RemindInterval, cfg.RemindWorkers, log)
	relay := outbox.New(c.outbox, c.publisher, log, outbox.WithInterval(cfg.OutboxInterval), outbox.WithMetrics(metrics.NewOutbox(reg)))

	public := server.New(api,
		server.WithAddr(cfg.Addr),
		server.WithLogger(log.With("server", "api")),
		server.WithShutdownTimeout(cfg.ShutdownTimeout),
		// Остановка по порядку: сначала перестаём называться готовыми, потом дообрабатываем текущие запросы.
		server.WithShutdownHook(api.StartShutdown),
	)
	// Метрики и pprof слушают отдельный порт: наружу он не публикуется.
	internal := server.New(admin.Handler(reg),
		server.WithAddr(cfg.AdminAddr),
		server.WithLogger(log.With("server", "admin")),
		server.WithShutdownTimeout(cfg.ShutdownTimeout),
		server.WithWriteTimeout(2*time.Minute), // CPU-профиль снимается 30 секунд и дольше
	)

	// errgroup запускает части сервиса и связывает их судьбу: если одна завершилась с ошибкой
	// (например, порт занят), контекст отменяется и остальные останавливаются. Wait ждёт всех.
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return public.Run(ctx) })
	g.Go(func() error { return internal.Run(ctx) })
	g.Go(func() error { reminders.Run(ctx); return nil })
	g.Go(func() error { relay.Run(ctx); return nil })
	return g.Wait()
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
