// Package config читает настройки сервиса из переменных окружения.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config — все настройки сервиса. Один и тот же образ работает в разных окружениях с разными значениями.
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
}

// Load читает настройки через getenv (обычно os.Getenv). Ошибки собираются все сразу:
// сервис не стартует и называет каждую переменную, с которой что-то не так.
func Load(getenv func(string) string) (Config, error) {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	required := func(name string) string {
		v := getenv(name)
		if v == "" {
			fail("%s: переменная обязательна", name)
		}
		return v
	}
	duration := func(name string, def time.Duration) time.Duration {
		v := getenv(name)
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			fail("%s: нужна положительная длительность вида 15m, получено %q", name, v)
			return def
		}
		return d
	}
	number := func(name string, def int) int {
		v := getenv(name)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			fail("%s: нужно положительное число, получено %q", name, v)
			return def
		}
		return n
	}

	cfg := Config{
		Addr:           getenv("ADDR"),
		DatabaseURL:    required("DATABASE_URL"),
		RedisURL:       required("REDIS_URL"),
		JWTSecret:      []byte(required("JWT_SECRET")),
		AccessTTL:      duration("ACCESS_TTL", 15*time.Minute),
		RefreshTTL:     duration("REFRESH_TTL", 30*24*time.Hour),
		CacheTTL:       duration("CACHE_TTL", time.Minute),
		RemindInterval: duration("REMIND_INTERVAL", 30*time.Second),
		RemindWorkers:  number("REMIND_WORKERS", 4),
		LoginLimit:     number("LOGIN_LIMIT", 10),

		ShutdownTimeout: duration("SHUTDOWN_TIMEOUT", 10*time.Second),
		SlowQuery:       duration("SLOW_QUERY", 200*time.Millisecond),
		WebhookURL:      getenv("WEBHOOK_URL"),
		WebhookSecret:   []byte(getenv("WEBHOOK_SECRET")),
		WebhookTimeout:  duration("WEBHOOK_TIMEOUT", 3*time.Second),
	}
	if cfg.Addr == "" {
		cfg.Addr = ":8080"
	}
	if n := len(cfg.JWTSecret); n > 0 && n < 32 {
		fail("JWT_SECRET: нужно не меньше 32 байт, получено %d", n)
	}
	if v := getenv("LOG_LEVEL"); v != "" {
		if err := cfg.LogLevel.UnmarshalText([]byte(v)); err != nil {
			fail("LOG_LEVEL: нужно debug, info, warn или error, получено %q", v)
		}
	}
	if cfg.WebhookURL != "" {
		// Адрес проверяем при старте: опечатка не должна всплыть ночью на первом напоминании.
		if u, err := url.Parse(cfg.WebhookURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			fail("WEBHOOK_URL: нужен адрес вида https://example.com/hook, получено %q", cfg.WebhookURL)
		}
	}
	for _, p := range strings.Split(getenv("TRUSTED_PROXIES"), ",") {
		if p = strings.TrimSpace(p); p != "" {
			cfg.TrustedProxies = append(cfg.TrustedProxies, p)
		}
	}
	return cfg, errors.Join(errs...)
}
