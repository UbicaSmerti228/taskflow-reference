package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/jackc/pgx/v5"

	"github.com/UbicaSmerti228/taskflow-reference/internal/config"
	"github.com/UbicaSmerti228/taskflow-reference/internal/httpapi"
	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/memory"
)

// cleanup очищает таблицы тестовой базы.
func cleanup(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx) //nolint:errcheck // тест
	if _, err := conn.Exec(ctx, `TRUNCATE users, tasks RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
}

const testSecret = "0123456789abcdef0123456789abcdef"

func getenv(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestRunArguments(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{name: "нет команды", args: nil, want: "Использование"},
		{name: "две команды", args: []string{"serve", "migrate"}, want: "Использование"},
		{name: "неизвестная команда", args: []string{"add"}, want: `неизвестная команда "add"`},
		{name: "migrate без адреса базы", args: []string{"migrate"}, want: "DATABASE_URL"},
		// serve называет сразу все недостающие переменные.
		{name: "serve без настроек", args: []string{"serve"}, want: "REDIS_URL"},
		{name: "serve с коротким секретом", args: []string{"serve"}, env: map[string]string{"DATABASE_URL": "x", "REDIS_URL": "y", "JWT_SECRET": "короткий"}, want: "JWT_SECRET"},
		{name: "serve с недоступной базой", args: []string{"serve"}, env: map[string]string{"DATABASE_URL": "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=2", "REDIS_URL": "redis://127.0.0.1:1", "JWT_SECRET": testSecret}, want: "migrations"},
		{name: "healthcheck без сервиса", args: []string{"healthcheck"}, env: map[string]string{"ADDR": "127.0.0.1:1"}, want: "connect"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(ctx, tt.args, getenv(tt.env), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("run(%v) error = %v, want ошибку с %q", tt.args, err, tt.want)
			}
		})
	}
}

// syncBuffer — буфер, в который сервис пишет логи из своих горутин, пока тест их читает.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startService запускает сервис в горутине и возвращает его адрес и функцию остановки.
func startService(t *testing.T, start func(ctx context.Context, addr string, logs io.Writer) error) (base string, logs *syncBuffer, stop func() error) {
	t.Helper()
	// Узнаём свободный порт: занимаем его и сразу отпускаем.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() //nolint:errcheck // порт нужен только на мгновение

	ctx, cancel := context.WithCancel(context.Background())
	logs = &syncBuffer{}
	done := make(chan error, 1)
	go func() { done <- start(ctx, addr, logs) }()

	base = "http://" + addr
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("сервис завершился при старте: %v\n%s", err, logs)
		default:
		}
		if resp, err := http.Get(base + "/healthz"); err == nil {
			resp.Body.Close() //nolint:errcheck // тело не читаем
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("сервис не поднялся за 10 секунд\n%s", logs)
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopped := false
	stop = func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(15 * time.Second):
			return fmt.Errorf("сервис не остановился за 15 секунд")
		}
	}
	t.Cleanup(func() { stop() }) //nolint:errcheck // тест уже завершён
	return base, logs, stop
}

// call отправляет JSON-запрос и разбирает ответ в out (если он задан).
func call(t *testing.T, method, url, token, body string, out any) int {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // тест
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatalf("%s %s: ответ не JSON: %v", method, url, err)
		}
	}
	return resp.StatusCode
}

// scenario проходит главный путь пользователя через настоящий HTTP.
func scenario(t *testing.T, base string) {
	t.Helper()
	creds := `{"email":"ann@example.com","password":"correct horse"}`
	if code := call(t, "POST", base+"/api/v1/auth/register", "", creds, nil); code != http.StatusCreated {
		t.Fatalf("register: status %d", code)
	}
	var tokens struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if code := call(t, "POST", base+"/api/v1/auth/login", "", creds, &tokens); code != http.StatusOK {
		t.Fatalf("login: status %d", code)
	}

	past := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	var created struct {
		ID int64 `json:"id"`
	}
	if code := call(t, "POST", base+"/api/v1/tasks", tokens.AccessToken, `{"title":"сдать отчёт","due_at":"`+past+`"}`, &created); code != http.StatusCreated {
		t.Fatalf("create task: status %d", code)
	}
	taskURL := fmt.Sprintf("%s/api/v1/tasks/%d", base, created.ID)

	// Воркер напоминаний работает рядом с сервером и отмечает просроченную задачу.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var got struct {
			RemindedAt *time.Time `json:"reminded_at"`
		}
		// PATCH сбрасывает кэш, поэтому читаем через список, который не кэшируется.
		var list struct {
			Items []struct {
				RemindedAt *time.Time `json:"reminded_at"`
			} `json:"items"`
		}
		if code := call(t, "GET", base+"/api/v1/tasks", tokens.AccessToken, "", &list); code != http.StatusOK || len(list.Items) != 1 {
			t.Fatalf("list tasks: status %d, задач %d", code, len(list.Items))
		}
		got.RemindedAt = list.Items[0].RemindedAt
		if got.RemindedAt != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("воркер не отправил напоминание за 10 секунд")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if code := call(t, "GET", taskURL, "", "", nil); code != http.StatusUnauthorized {
		t.Errorf("задача без токена: status %d, want 401", code)
	}
	if code := call(t, "PATCH", taskURL, tokens.AccessToken, `{"done":true}`, nil); code != http.StatusOK {
		t.Errorf("patch task: status %d", code)
	}
	if code := call(t, "POST", base+"/api/v1/auth/refresh", "", `{"refresh_token":"`+tokens.RefreshToken+`"}`, &tokens); code != http.StatusOK {
		t.Errorf("refresh: status %d", code)
	}
	if code := call(t, "DELETE", taskURL, tokens.AccessToken, "", nil); code != http.StatusNoContent {
		t.Errorf("delete task новым токеном: status %d", code)
	}
	if code := call(t, "GET", base+"/readyz", "", "", nil); code != http.StatusOK {
		t.Errorf("/readyz: status %d", code)
	}
}

// Сервис на хранилищах в памяти: проверяет сборку слоёв, запуск и остановку без базы и Redis.
func TestRunService(t *testing.T) {
	tasks, kv := memory.NewTasks(), memory.NewKV()
	base, logs, stop := startService(t, func(ctx context.Context, addr string, out io.Writer) error {
		cfg := config.Config{
			Addr: addr, JWTSecret: []byte(testSecret), AccessTTL: time.Minute, RefreshTTL: time.Hour, CacheTTL: time.Minute,
			RemindInterval: 10 * time.Millisecond, RemindWorkers: 2, LoginLimit: 100,
		}
		return runService(ctx, cfg, logging.New(out, slog.LevelDebug), components{
			taskRepo: tasks, userRepo: memory.NewUsers(), reminders: tasks, kv: kv,
			ready: map[string]httpapi.Check{},
		})
	})

	scenario(t, base)

	if err := healthcheck(context.Background(), strings.TrimPrefix(base, "http://")); err != nil {
		t.Errorf("healthcheck работающего сервиса: %v", err)
	}
	if err := stop(); err != nil {
		t.Errorf("остановка: %v", err)
	}
	if _, err := net.DialTimeout("tcp", strings.TrimPrefix(base, "http://"), time.Second); err == nil {
		t.Error("после остановки порт всё ещё принимает соединения")
	}
	out := logs.String()
	for _, want := range []string{`"msg":"server started"`, `"msg":"reminder"`, `"msg":"shutting down"`, `"msg":"server stopped"`} {
		if !strings.Contains(out, want) {
			t.Errorf("в логе нет записи %s", want)
		}
	}
	if strings.Contains(out, "correct horse") || strings.Contains(out, testSecret) {
		t.Error("в лог попал пароль или секрет")
	}
}

func TestRunServiceErrors(t *testing.T) {
	tasks := memory.NewTasks()
	c := components{taskRepo: tasks, userRepo: memory.NewUsers(), reminders: tasks, kv: memory.NewKV()}
	log := logging.New(io.Discard, slog.LevelInfo)
	good := config.Config{Addr: "127.0.0.1:0", JWTSecret: []byte(testSecret), AccessTTL: time.Minute, RefreshTTL: time.Hour, RemindInterval: time.Second, RemindWorkers: 1}

	short := good
	short.JWTSecret = []byte("короткий")
	if err := runService(context.Background(), short, log, c); err == nil {
		t.Error("короткий секрет: runService вернул nil, want ошибку")
	}

	badProxy := good
	badProxy.TrustedProxies = []string{"не адрес"}
	if err := runService(context.Background(), badProxy, log, c); err == nil {
		t.Error("неверный адрес прокси: runService вернул nil, want ошибку")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // тест завершается
	busy := good
	busy.Addr = ln.Addr().String()
	if err := runService(context.Background(), busy, log, c); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Errorf("занятый порт: error = %v, want ошибку listen", err)
	}
}

// Полный запуск командой serve на настоящем PostgreSQL. Redis заменён на miniredis — он говорит по тому же протоколу.
// Нужна база: TASKFLOW_TEST_DSN. В CI тот же путь проверяет запуск через docker compose.
func TestServeWithPostgres(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_TEST_DSN")
	if dsn == "" {
		t.Skip("интеграционный тест: задай TASKFLOW_TEST_DSN")
	}
	mr := miniredis.RunT(t)

	base, logs, stop := startService(t, func(ctx context.Context, addr string, out io.Writer) error {
		return run(ctx, []string{"serve"}, getenv(map[string]string{
			"DATABASE_URL": dsn, "REDIS_URL": "redis://" + mr.Addr(), "JWT_SECRET": testSecret,
			"ADDR": addr, "REMIND_INTERVAL": "20ms", "LOG_LEVEL": "debug",
		}), out)
	})
	// Сценарий регистрирует пользователя заново — убираем следы прошлого запуска.
	cleanup(t, dsn)

	scenario(t, base)

	if err := run(context.Background(), []string{"healthcheck"}, getenv(map[string]string{"ADDR": strings.TrimPrefix(base, "http://")}), io.Discard); err != nil {
		t.Errorf("healthcheck: %v", err)
	}
	// Без Redis сервис перестаёт быть готовым, но остаётся живым.
	mr.Close()
	if code := call(t, "GET", base+"/readyz", "", "", nil); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz без Redis: status %d, want 503", code)
	}
	if code := call(t, "GET", base+"/healthz", "", "", nil); code != http.StatusOK {
		t.Errorf("/healthz без Redis: status %d, want 200", code)
	}
	if err := stop(); err != nil {
		t.Errorf("остановка: %v\n%s", err, logs)
	}
}

func TestMigrateCommand(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_TEST_DSN")
	if dsn == "" {
		t.Skip("интеграционный тест: задай TASKFLOW_TEST_DSN")
	}
	var out bytes.Buffer
	if err := run(context.Background(), []string{"migrate"}, getenv(map[string]string{"DATABASE_URL": dsn}), &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Миграции применены") {
		t.Errorf("вывод = %q, want «Миграции применены»", out.String())
	}
}
