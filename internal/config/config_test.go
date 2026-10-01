package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

const secret = "0123456789abcdef0123456789abcdef"

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"DATABASE_URL": "postgres://localhost/taskflow",
		"REDIS_URL":    "redis://localhost:6379",
		"JWT_SECRET":   secret,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":8080" || cfg.AccessTTL != 15*time.Minute || cfg.RefreshTTL != 720*time.Hour ||
		cfg.CacheTTL != time.Minute || cfg.LogLevel != slog.LevelInfo || cfg.RemindInterval != 30*time.Second ||
		cfg.RemindWorkers != 4 || cfg.LoginLimit != 10 || cfg.TrustedProxies != nil {
		t.Errorf("значения по умолчанию = %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"DATABASE_URL": "postgres://db/taskflow", "REDIS_URL": "redis://cache:6379", "JWT_SECRET": secret,
		"ADDR": ":9000", "ACCESS_TTL": "5m", "REFRESH_TTL": "24h", "CACHE_TTL": "10s", "LOG_LEVEL": "debug",
		"REMIND_INTERVAL": "1s", "REMIND_WORKERS": "8", "LOGIN_LIMIT": "3", "TRUSTED_PROXIES": "10.0.0.0/8, 172.16.0.1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":9000" || cfg.AccessTTL != 5*time.Minute || cfg.RefreshTTL != 24*time.Hour || cfg.CacheTTL != 10*time.Second ||
		cfg.LogLevel != slog.LevelDebug || cfg.RemindInterval != time.Second || cfg.RemindWorkers != 8 || cfg.LoginLimit != 3 ||
		len(cfg.TrustedProxies) != 2 || cfg.TrustedProxies[1] != "172.16.0.1" {
		t.Errorf("переопределённые значения = %+v", cfg)
	}
}

// Сервис называет сразу все проблемные переменные, а не по одной за запуск.
func TestLoadReportsEveryProblem(t *testing.T) {
	_, err := Load(env(map[string]string{
		"JWT_SECRET": "короткий", "ACCESS_TTL": "скоро", "REMIND_WORKERS": "0", "LOG_LEVEL": "громко", "CACHE_TTL": "-1s",
	}))
	if err == nil {
		t.Fatal("Load() вернул nil, want ошибку")
	}
	for _, name := range []string{"DATABASE_URL", "REDIS_URL", "JWT_SECRET", "ACCESS_TTL", "REMIND_WORKERS", "LOG_LEVEL", "CACHE_TTL"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("в ошибке не названа переменная %s: %v", name, err)
		}
	}
}

func TestLoadRequiresSecret(t *testing.T) {
	_, err := Load(env(map[string]string{"DATABASE_URL": "x", "REDIS_URL": "y"}))
	if err == nil || !strings.Contains(err.Error(), "JWT_SECRET") {
		t.Errorf("Load() без JWT_SECRET: error = %v, want упоминание JWT_SECRET", err)
	}
}
