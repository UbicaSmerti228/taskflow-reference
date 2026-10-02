// Package config читает настройки сервисов из переменных окружения.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config — все настройки TaskFlow. Один и тот же образ работает в разных окружениях с разными значениями.
type Config struct {
	Addr           string
	DatabaseURL    string
	RedisURL       string
	JWTSecret      []byte
	AccessTTL      time.Duration
	RefreshTTL     time.Duration
	CacheTTL       time.Duration
	LogLevel       slog.Level
	RemindInterval time.Duration
	RemindWorkers  int
	LoginLimit     int      // попыток входа и регистрации в минуту с одного IP
	TrustedProxies []string // адреса прокси, чьему X-Forwarded-For можно верить

	ShutdownTimeout time.Duration // сколько ждать текущие запросы при остановке
	SlowQuery       time.Duration // запрос к базе дольше этого попадает в лог с уровнем warn
	WebhookURL      string        // куда отправлять напоминания; пусто — писать в лог
	WebhookSecret   []byte        // ключ подписи тела вебхука; пусто — без подписи
	WebhookTimeout  time.Duration // время на одну попытку отправки

	AdminAddr       string        // адрес служебного сервера с метриками и pprof; наружу не публикуется
	KafkaBrokers    []string      // адреса брокеров Kafka
	OutboxInterval  time.Duration // как часто переносить события из outbox в Kafka
	NotifierAddr    string        // адрес gRPC-сервера notifier
	NotifierTimeout time.Duration // deadline одного вызова notifier
}

// Notifier — настройки сервиса notifier.
type Notifier struct {
	GRPCAddr        string
	AdminAddr       string
	DatabaseURL     string
	KafkaBrokers    []string
	KafkaGroup      string // consumer group: экземпляры с одним именем делят партиции между собой
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
}

// Load читает настройки TaskFlow через getenv (обычно os.Getenv). Ошибки собираются все сразу:
// сервис не стартует и называет каждую переменную, с которой что-то не так.
func Load(getenv func(string) string) (Config, error) {
	r := reader{getenv: getenv}
	cfg := Config{
		Addr:           r.str("ADDR", ":8080"),
		DatabaseURL:    r.required("DATABASE_URL"),
		RedisURL:       r.required("REDIS_URL"),
		JWTSecret:      []byte(r.required("JWT_SECRET")),
		AccessTTL:      r.duration("ACCESS_TTL", 15*time.Minute),
		RefreshTTL:     r.duration("REFRESH_TTL", 30*24*time.Hour),
		CacheTTL:       r.duration("CACHE_TTL", time.Minute),
		LogLevel:       r.level("LOG_LEVEL"),
		RemindInterval: r.duration("REMIND_INTERVAL", 30*time.Second),
		RemindWorkers:  r.number("REMIND_WORKERS", 4),
		LoginLimit:     r.number("LOGIN_LIMIT", 10),
		TrustedProxies: r.list("TRUSTED_PROXIES"),

		ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 10*time.Second),
		SlowQuery:       r.duration("SLOW_QUERY", 200*time.Millisecond),
		WebhookURL:      getenv("WEBHOOK_URL"),
		WebhookSecret:   []byte(getenv("WEBHOOK_SECRET")),
		WebhookTimeout:  r.duration("WEBHOOK_TIMEOUT", 3*time.Second),

		AdminAddr:       r.str("ADMIN_ADDR", ":8081"),
		KafkaBrokers:    r.brokers("KAFKA_BROKERS"),
		OutboxInterval:  r.duration("OUTBOX_INTERVAL", time.Second),
		NotifierAddr:    r.hostPort("NOTIFIER_ADDR"),
		NotifierTimeout: r.duration("NOTIFIER_TIMEOUT", 2*time.Second),
	}
	if n := len(cfg.JWTSecret); n > 0 && n < 32 {
		r.fail("JWT_SECRET: нужно не меньше 32 байт, получено %d", n)
	}
	if cfg.WebhookURL != "" {
		// Адрес проверяем при старте: опечатка не должна всплыть ночью на первом напоминании.
		if u, err := url.Parse(cfg.WebhookURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			r.fail("WEBHOOK_URL: нужен адрес вида https://example.com/hook, получено %q", cfg.WebhookURL)
		}
	}
	return cfg, errors.Join(r.errs...)
}

// LoadNotifier читает настройки сервиса notifier.
func LoadNotifier(getenv func(string) string) (Notifier, error) {
	r := reader{getenv: getenv}
	cfg := Notifier{
		GRPCAddr:        r.str("GRPC_ADDR", ":9000"),
		AdminAddr:       r.str("ADMIN_ADDR", ":9001"),
		DatabaseURL:     r.required("DATABASE_URL"),
		KafkaBrokers:    r.brokers("KAFKA_BROKERS"),
		KafkaGroup:      r.str("KAFKA_GROUP", "notifier"),
		LogLevel:        r.level("LOG_LEVEL"),
		ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
	return cfg, errors.Join(r.errs...)
}

// reader читает переменные и копит ошибки: сообщить нужно обо всех сразу, а не о первой.
type reader struct {
	getenv func(string) string
	errs   []error
}

func (r *reader) fail(format string, args ...any) {
	r.errs = append(r.errs, fmt.Errorf(format, args...))
}

func (r *reader) str(name, def string) string {
	if v := r.getenv(name); v != "" {
		return v
	}
	return def
}

func (r *reader) required(name string) string {
	v := r.getenv(name)
	if v == "" {
		r.fail("%s: переменная обязательна", name)
	}
	return v
}

func (r *reader) duration(name string, def time.Duration) time.Duration {
	v := r.getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		r.fail("%s: нужна положительная длительность вида 15m, получено %q", name, v)
		return def
	}
	return d
}

func (r *reader) number(name string, def int) int {
	v := r.getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		r.fail("%s: нужно положительное число, получено %q", name, v)
		return def
	}
	return n
}

func (r *reader) level(name string) slog.Level {
	var level slog.Level // нулевое значение — info
	if v := r.getenv(name); v != "" {
		if err := level.UnmarshalText([]byte(v)); err != nil {
			r.fail("%s: нужно debug, info, warn или error, получено %q", name, v)
		}
	}
	return level
}

// list разбирает значения через запятую. Пустая переменная даёт nil.
func (r *reader) list(name string) []string {
	var out []string
	for _, v := range strings.Split(r.getenv(name), ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// hostPort читает обязательный адрес вида host:port.
func (r *reader) hostPort(name string) string {
	v := r.required(name)
	if v != "" && !validHostPort(v) {
		r.fail("%s: нужен адрес вида host:port, получено %q", name, v)
	}
	return v
}

// brokers читает обязательный список адресов host:port через запятую.
func (r *reader) brokers(name string) []string {
	out := r.list(name)
	if len(out) == 0 {
		r.fail("%s: переменная обязательна", name)
	}
	for _, addr := range out {
		if !validHostPort(addr) {
			r.fail("%s: нужны адреса вида host:port через запятую, получено %q", name, addr)
		}
	}
	return out
}

func validHostPort(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	n, err := strconv.Atoi(port)
	return err == nil && n > 0 && n < 65536
}
