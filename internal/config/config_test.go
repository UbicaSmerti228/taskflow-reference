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
		"DATABASE_URL":  "postgres://localhost/taskflow",
		"REDIS_URL":     "redis://localhost:6379",
		"JWT_SECRET":    secret,
		"KAFKA_BROKERS": "localhost:9092",
		"NOTIFIER_ADDR": "localhost:9000",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":8080" || cfg.AccessTTL != 15*time.Minute || cfg.RefreshTTL != 720*time.Hour ||
		cfg.CacheTTL != time.Minute || cfg.LogLevel != slog.LevelInfo || cfg.RemindInterval != 30*time.Second ||
		cfg.RemindWorkers != 4 || cfg.LoginLimit != 10 || cfg.TrustedProxies != nil ||
		cfg.ShutdownTimeout != 10*time.Second || cfg.SlowQuery != 200*time.Millisecond ||
		cfg.WebhookURL != "" || len(cfg.WebhookSecret) != 0 || cfg.WebhookTimeout != 3*time.Second ||
		cfg.AdminAddr != ":8081" || len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:9092" ||
		cfg.OutboxInterval != time.Second || cfg.NotifierAddr != "localhost:9000" || cfg.NotifierTimeout != 2*time.Second {
		t.Errorf("значения по умолчанию = %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"DATABASE_URL": "postgres://db/taskflow", "REDIS_URL": "redis://cache:6379", "JWT_SECRET": secret,
		"ADDR": ":9000", "ACCESS_TTL": "5m", "REFRESH_TTL": "24h", "CACHE_TTL": "10s", "LOG_LEVEL": "debug",
		"REMIND_INTERVAL": "1s", "REMIND_WORKERS": "8", "LOGIN_LIMIT": "3", "TRUSTED_PROXIES": "10.0.0.0/8, 172.16.0.1",
		"SHUTDOWN_TIMEOUT": "25s", "SLOW_QUERY": "50ms",
		"WEBHOOK_URL": "https://hooks.example.com/taskflow", "WEBHOOK_SECRET": "ключ", "WEBHOOK_TIMEOUT": "1s",
		"ADMIN_ADDR": "127.0.0.1:7000", "KAFKA_BROKERS": "kafka-1:9092, kafka-2:9092", "OUTBOX_INTERVAL": "250ms",
		"NOTIFIER_ADDR": "notifier:9000", "NOTIFIER_TIMEOUT": "500ms",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":9000" || cfg.AccessTTL != 5*time.Minute || cfg.RefreshTTL != 24*time.Hour || cfg.CacheTTL != 10*time.Second ||
		cfg.LogLevel != slog.LevelDebug || cfg.RemindInterval != time.Second || cfg.RemindWorkers != 8 || cfg.LoginLimit != 3 ||
		len(cfg.TrustedProxies) != 2 || cfg.TrustedProxies[1] != "172.16.0.1" ||
		cfg.ShutdownTimeout != 25*time.Second || cfg.SlowQuery != 50*time.Millisecond ||
		cfg.WebhookURL != "https://hooks.example.com/taskflow" || string(cfg.WebhookSecret) != "ключ" || cfg.WebhookTimeout != time.Second ||
		cfg.AdminAddr != "127.0.0.1:7000" || len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[1] != "kafka-2:9092" ||
		cfg.OutboxInterval != 250*time.Millisecond || cfg.NotifierAddr != "notifier:9000" || cfg.NotifierTimeout != 500*time.Millisecond {
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
	for _, name := range []string{"DATABASE_URL", "REDIS_URL", "JWT_SECRET", "ACCESS_TTL", "REMIND_WORKERS", "LOG_LEVEL", "CACHE_TTL", "KAFKA_BROKERS", "NOTIFIER_ADDR"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("в ошибке не названа переменная %s: %v", name, err)
		}
	}
}

func TestLoadRequiresSecret(t *testing.T) {
	_, err := Load(env(map[string]string{"DATABASE_URL": "x", "REDIS_URL": "y", "KAFKA_BROKERS": "k:9092", "NOTIFIER_ADDR": "n:9000"}))
	if err == nil || !strings.Contains(err.Error(), "JWT_SECRET") {
		t.Errorf("Load() без JWT_SECRET: error = %v, want упоминание JWT_SECRET", err)
	}
}

func TestLoadWebhookURL(t *testing.T) {
	base := map[string]string{"DATABASE_URL": "x", "REDIS_URL": "y", "JWT_SECRET": secret, "KAFKA_BROKERS": "k:9092", "NOTIFIER_ADDR": "n:9000"}
	tests := []struct {
		url string
		ok  bool
	}{
		{url: "", ok: true},
		{url: "http://notifier:9000/hook", ok: true},
		{url: "https://hooks.example.com/t?k=1", ok: true},
		{url: "hooks.example.com/t", ok: false},
		{url: "ftp://hooks.example.com", ok: false},
		{url: "https://", ok: false},
		{url: "http://bad host/", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			vars := map[string]string{"WEBHOOK_URL": tt.url}
			for k, v := range base {
				vars[k] = v
			}
			_, err := Load(env(vars))
			if tt.ok && err != nil {
				t.Errorf("Load() error = %v, want nil", err)
			}
			if !tt.ok && (err == nil || !strings.Contains(err.Error(), "WEBHOOK_URL")) {
				t.Errorf("Load() error = %v, want упоминание WEBHOOK_URL", err)
			}
		})
	}
}

// Адреса соседних сервисов проверяются при старте: опечатка в порту видна сразу.
func TestLoadAddresses(t *testing.T) {
	base := map[string]string{"DATABASE_URL": "x", "REDIS_URL": "y", "JWT_SECRET": secret, "KAFKA_BROKERS": "k:9092", "NOTIFIER_ADDR": "n:9000"}
	tests := []struct {
		name, variable, value string
		ok                    bool
	}{
		{name: "один брокер", variable: "KAFKA_BROKERS", value: "kafka:9092", ok: true},
		{name: "несколько брокеров", variable: "KAFKA_BROKERS", value: "a:9092,b:9092 , c:9092", ok: true},
		{name: "брокер без порта", variable: "KAFKA_BROKERS", value: "kafka", ok: false},
		{name: "один из брокеров без порта", variable: "KAFKA_BROKERS", value: "a:9092,b", ok: false},
		{name: "пустой список брокеров", variable: "KAFKA_BROKERS", value: " , ", ok: false},
		{name: "notifier по имени", variable: "NOTIFIER_ADDR", value: "notifier:9000", ok: true},
		{name: "notifier по IPv6", variable: "NOTIFIER_ADDR", value: "[::1]:9000", ok: true},
		{name: "notifier без порта", variable: "NOTIFIER_ADDR", value: "notifier", ok: false},
		{name: "notifier с нечисловым портом", variable: "NOTIFIER_ADDR", value: "notifier:grpc", ok: false},
		{name: "notifier с портом вне диапазона", variable: "NOTIFIER_ADDR", value: "notifier:70000", ok: false},
		{name: "notifier без хоста", variable: "NOTIFIER_ADDR", value: ":9000", ok: false},
		{name: "notifier с URL вместо адреса", variable: "NOTIFIER_ADDR", value: "http://notifier:9000", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := map[string]string{}
			for k, v := range base {
				vars[k] = v
			}
			vars[tt.variable] = tt.value
			_, err := Load(env(vars))
			if tt.ok && err != nil {
				t.Errorf("Load() error = %v, want nil", err)
			}
			if !tt.ok && (err == nil || !strings.Contains(err.Error(), tt.variable)) {
				t.Errorf("Load() error = %v, want упоминание %s", err, tt.variable)
			}
		})
	}
}

func TestLoadNotifier(t *testing.T) {
	cfg, err := LoadNotifier(env(map[string]string{"DATABASE_URL": "postgres://db/taskflow", "KAFKA_BROKERS": "kafka:9092"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GRPCAddr != ":9000" || cfg.AdminAddr != ":9001" || cfg.KafkaGroup != "notifier" || cfg.LogLevel != slog.LevelInfo ||
		cfg.ShutdownTimeout != 10*time.Second || cfg.DatabaseURL != "postgres://db/taskflow" || len(cfg.KafkaBrokers) != 1 {
		t.Errorf("значения по умолчанию = %+v", cfg)
	}

	cfg, err = LoadNotifier(env(map[string]string{
		"DATABASE_URL": "postgres://db/taskflow", "KAFKA_BROKERS": "a:9092,b:9092", "GRPC_ADDR": ":7000", "ADMIN_ADDR": ":7001",
		"KAFKA_GROUP": "notifier-blue", "LOG_LEVEL": "warn", "SHUTDOWN_TIMEOUT": "3s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GRPCAddr != ":7000" || cfg.AdminAddr != ":7001" || cfg.KafkaGroup != "notifier-blue" || cfg.LogLevel != slog.LevelWarn ||
		cfg.ShutdownTimeout != 3*time.Second || len(cfg.KafkaBrokers) != 2 {
		t.Errorf("переопределённые значения = %+v", cfg)
	}

	_, err = LoadNotifier(env(map[string]string{"LOG_LEVEL": "громко", "SHUTDOWN_TIMEOUT": "скоро"}))
	for _, name := range []string{"DATABASE_URL", "KAFKA_BROKERS", "LOG_LEVEL", "SHUTDOWN_TIMEOUT"} {
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("в ошибке не названа переменная %s: %v", name, err)
		}
	}
}
