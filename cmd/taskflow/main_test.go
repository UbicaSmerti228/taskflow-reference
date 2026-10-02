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
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/UbicaSmerti228/taskflow-reference/internal/config"
	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
	"github.com/UbicaSmerti228/taskflow-reference/internal/httpapi"
	"github.com/UbicaSmerti228/taskflow-reference/internal/kafka"
	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/memory"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notifier"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notifier/grpcserver"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notifierclient"
	"github.com/UbicaSmerti228/taskflow-reference/internal/webhook"
)

const testSecret = "0123456789abcdef0123456789abcdef"

func getenv(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestRunArguments(t *testing.T) {
	ctx := context.Background()
	full := func(extra map[string]string) map[string]string {
		env := map[string]string{
			"DATABASE_URL": "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=2", "REDIS_URL": "redis://127.0.0.1:1",
			"JWT_SECRET": testSecret, "KAFKA_BROKERS": "127.0.0.1:1", "NOTIFIER_ADDR": "127.0.0.1:1",
		}
		for k, v := range extra {
			env[k] = v
		}
		return env
	}
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
		{name: "serve без настроек", args: []string{"serve"}, want: "KAFKA_BROKERS"},
		{name: "serve с коротким секретом", args: []string{"serve"}, env: full(map[string]string{"JWT_SECRET": "короткий"}), want: "JWT_SECRET"},
		{name: "serve с недоступной базой", args: []string{"serve"}, env: full(nil), want: "migrations"},
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

// ---------- запуск сервиса в тесте ----------

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

// freeAddr узнаёт свободный порт: занимает его и сразу отпускает.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // порт нужен только на мгновение
	return ln.Addr().String()
}

// running — сервис, запущенный в тесте.
type running struct {
	base  string // адрес API
	admin string // адрес служебного сервера
	logs  *syncBuffer
	stop  func() error
}

// startService запускает сервис в горутине и ждёт, пока он начнёт отвечать.
func startService(t *testing.T, start func(ctx context.Context, addr, adminAddr string, logs io.Writer) error) *running {
	t.Helper()
	addr, adminAddr := freeAddr(t), freeAddr(t)
	svc := &running{base: "http://" + addr, admin: "http://" + adminAddr, logs: &syncBuffer{}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- start(ctx, addr, adminAddr, svc.logs) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("сервис завершился при старте: %v\n%s", err, svc.logs)
		default:
		}
		if resp, err := http.Get(svc.base + "/healthz"); err == nil {
			resp.Body.Close() //nolint:errcheck // тело не читаем
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("сервис не поднялся за 10 секунд\n%s", svc.logs)
		}
		time.Sleep(10 * time.Millisecond)
	}

	stopped := false
	svc.stop = func() error {
		if stopped {
			return nil
		}
		stopped = true
		// HTTP-клиент теста мог открыть соединение про запас и не отправить по нему ни одного запроса.
		// Такое соединение Shutdown считает новым и ждёт до пяти секунд, поэтому закрываем его сами.
		http.DefaultClient.CloseIdleConnections()
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(15 * time.Second):
			return fmt.Errorf("сервис не остановился за 15 секунд")
		}
	}
	t.Cleanup(func() { svc.stop() }) //nolint:errcheck // тест уже завершён
	return svc
}

func testConfig(addr, adminAddr string) config.Config {
	return config.Config{
		Addr: addr, AdminAddr: adminAddr, JWTSecret: []byte(testSecret),
		AccessTTL: time.Minute, RefreshTTL: time.Hour, CacheTTL: time.Minute,
		RemindInterval: 10 * time.Millisecond, RemindWorkers: 2, LoginLimit: 100,
		ShutdownTimeout: 5 * time.Second, SlowQuery: time.Second,
		OutboxInterval: 10 * time.Millisecond, NotifierTimeout: time.Second,
	}
}

// broker — издатель в памяти: запоминает опубликованные события.
type broker struct {
	mu  sync.Mutex
	got []event.Event
}

func (b *broker) Publish(_ context.Context, batch []event.Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, rec := range batch {
		e, err := event.Decode(rec.Payload)
		if err != nil {
			return err
		}
		b.got = append(b.got, e)
	}
	return nil
}

func (b *broker) ids() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]string, len(b.got))
	for i, e := range b.got {
		ids[i] = e.ID
	}
	return ids
}

// noNotifications — сервис уведомлений, который всегда недоступен.
type noNotifications struct{}

func (noNotifications) List(context.Context, int64, int, int64) ([]notification.Notification, error) {
	return nil, notification.ErrUnavailable
}

// memoryComponents собирает сервис на хранилищах в памяти.
func memoryComponents(tasks *memory.Tasks, pub *broker) components {
	return components{
		taskRepo: tasks, userRepo: memory.NewUsers(), reminders: tasks, kv: memory.NewKV(),
		outbox: tasks.Outbox(), publisher: pub, notifications: noNotifications{},
		ready: map[string]httpapi.Check{},
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
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

// fetch возвращает статус и тело ответа на GET.
func fetch(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // тест
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

// login регистрирует пользователя и возвращает его access- и refresh-токены.
func login(t *testing.T, base, email string) (access, refresh string) {
	t.Helper()
	creds := fmt.Sprintf(`{"email":%q,"password":"correct horse"}`, email)
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
	return tokens.AccessToken, tokens.RefreshToken
}

// createOverdue создаёт просроченную задачу и ждёт, пока воркер отметит напоминание.
func createOverdue(t *testing.T, base, token, title string) int64 {
	t.Helper()
	past := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	var created struct {
		ID int64 `json:"id"`
	}
	if code := call(t, "POST", base+"/api/v1/tasks", token, fmt.Sprintf(`{"title":%q,"due_at":%q}`, title, past), &created); code != http.StatusCreated {
		t.Fatalf("create task: status %d", code)
	}
	// Задачу читаем через список: он не кэшируется.
	waitFor(t, 10*time.Second, "отметка о напоминании", func() bool {
		var list struct {
			Items []struct {
				ID         int64      `json:"id"`
				RemindedAt *time.Time `json:"reminded_at"`
			} `json:"items"`
		}
		if code := call(t, "GET", base+"/api/v1/tasks", token, "", &list); code != http.StatusOK {
			t.Fatalf("list tasks: status %d", code)
		}
		for _, item := range list.Items {
			if item.ID == created.ID {
				return item.RemindedAt != nil
			}
		}
		return false
	})
	return created.ID
}

// scenario проходит главный путь пользователя через настоящий HTTP.
func scenario(t *testing.T, base string) {
	t.Helper()
	access, refresh := login(t, base, "ann@example.com")
	id := createOverdue(t, base, access, "сдать отчёт")
	taskURL := fmt.Sprintf("%s/api/v1/tasks/%d", base, id)

	if code := call(t, "GET", taskURL, "", "", nil); code != http.StatusUnauthorized {
		t.Errorf("задача без токена: status %d, want 401", code)
	}
	if code := call(t, "PATCH", taskURL, access, `{"done":true}`, nil); code != http.StatusOK {
		t.Errorf("patch task: status %d", code)
	}
	var tokens struct {
		AccessToken string `json:"access_token"`
	}
	if code := call(t, "POST", base+"/api/v1/auth/refresh", "", `{"refresh_token":"`+refresh+`"}`, &tokens); code != http.StatusOK {
		t.Errorf("refresh: status %d", code)
	}
	if code := call(t, "DELETE", taskURL, tokens.AccessToken, "", nil); code != http.StatusNoContent {
		t.Errorf("delete task новым токеном: status %d", code)
	}
	if code := call(t, "GET", base+"/readyz", "", "", nil); code != http.StatusOK {
		t.Errorf("/readyz: status %d", code)
	}
}

// ---------- сервис на хранилищах в памяти ----------

// Проверяет сборку слоёв, запуск и остановку без базы, Redis и Kafka.
func TestRunService(t *testing.T) {
	tasks, pub := memory.NewTasks(), &broker{}
	svc := startService(t, func(ctx context.Context, addr, adminAddr string, out io.Writer) error {
		return runService(ctx, testConfig(addr, adminAddr), logging.New(out, slog.LevelDebug), memoryComponents(tasks, pub))
	})

	scenario(t, svc.base)

	// События о создании задачи и о наступившем сроке дошли до издателя, в порядке записи.
	waitFor(t, 5*time.Second, "два события у издателя", func() bool { return len(pub.ids()) == 2 })
	if ids := pub.ids(); ids[0] != "task.created:1" || !strings.HasPrefix(ids[1], "task.due:1:") {
		t.Errorf("события = %v, want task.created:1 и task.due:1:…", ids)
	}
	// Сосед не отвечает — уведомления отвечают 503, остальное API работает.
	access, _ := login(t, svc.base, "bob@example.com")
	var unavailable struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if code := call(t, "GET", svc.base+"/api/v1/notifications", access, "", &unavailable); code != http.StatusServiceUnavailable || unavailable.Error.Code != "notifications_unavailable" {
		t.Errorf("уведомления без notifier: status %d, код %q; want 503 notifications_unavailable", code, unavailable.Error.Code)
	}

	if err := healthcheck(context.Background(), strings.TrimPrefix(svc.base, "http://")); err != nil {
		t.Errorf("healthcheck работающего сервиса: %v", err)
	}

	// Метрики и pprof отвечают на служебном порту…
	code, body := fetch(t, svc.admin+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics: status %d", code)
	}
	for _, want := range []string{
		`taskflow_http_requests_total{method="POST",route="/api/v1/tasks",status="201"} 1`,
		// В метке — шаблон маршрута, а не путь с id задачи.
		`taskflow_http_requests_total{method="PATCH",route="/api/v1/tasks/:id",status="200"} 1`,
		`taskflow_http_request_duration_seconds_count{method="POST",route="/api/v1/auth/login"} 2`,
		`taskflow_repository_call_duration_seconds_count{op="tasks.create"} 1`,
		`taskflow_outbox_published_total 2`,
		`taskflow_outbox_pending 0`,
		`go_goroutines `,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("в /metrics нет строки %s", want)
		}
	}
	if code, body := fetch(t, svc.admin+"/debug/pprof/goroutine?debug=1"); code != http.StatusOK || !strings.Contains(body, "goroutine profile") {
		t.Errorf("pprof на служебном порту: status %d", code)
	}
	// …а на публичном их нет.
	for _, path := range []string{"/metrics", "/debug/pprof/", "/debug/pprof/heap"} {
		if code, _ := fetch(t, svc.base+path); code != http.StatusNotFound {
			t.Errorf("%s на публичном порту: status %d, want 404", path, code)
		}
	}

	if err := svc.stop(); err != nil {
		t.Errorf("остановка: %v", err)
	}
	for _, addr := range []string{svc.base, svc.admin} {
		if _, err := net.DialTimeout("tcp", strings.TrimPrefix(addr, "http://"), time.Second); err == nil {
			t.Errorf("после остановки порт %s всё ещё принимает соединения", addr)
		}
	}
	out := svc.logs.String()
	for _, want := range []string{`"msg":"server started"`, `"msg":"reminder"`, `"msg":"shutting down"`, `"msg":"server stopped"`, `"server":"admin"`} {
		if !strings.Contains(out, want) {
			t.Errorf("в логе нет записи %s", want)
		}
	}
	if strings.Contains(out, "correct horse") || strings.Contains(out, testSecret) {
		t.Error("в лог попал пароль или секрет")
	}
}

// С адресом вебхука напоминания уходят получателю. Пока он отвечает ошибкой, задача не отмечается
// и попадает в следующий обход; все повторы несут один идентификатор события и верную подпись.
func TestRunServiceWebhook(t *testing.T) {
	const hookSecret = "секрет получателя"
	var (
		mu       sync.Mutex
		ids      []string
		badSigns int
	)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get(webhook.HeaderSignature) != "sha256="+webhook.Sign([]byte(hookSecret), body) {
			badSigns++
		}
		ids = append(ids, r.Header.Get(webhook.HeaderEventID))
		if len(ids) <= 3 { // первая отправка целиком неудачна: три попытки, три отказа
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer receiver.Close()

	tasks, pub := memory.NewTasks(), &broker{}
	svc := startService(t, func(ctx context.Context, addr, adminAddr string, out io.Writer) error {
		cfg := testConfig(addr, adminAddr)
		cfg.WebhookURL, cfg.WebhookSecret, cfg.WebhookTimeout = receiver.URL, []byte(hookSecret), time.Second
		return runService(ctx, cfg, logging.New(out, slog.LevelDebug), memoryComponents(tasks, pub))
	})

	scenario(t, svc.base) // ждёт, пока у задачи появится отметка о напоминании

	// Событие «срок наступил» записано один раз, хотя вебхук отправлялся четырежды.
	waitFor(t, 5*time.Second, "два события у издателя", func() bool { return len(pub.ids()) == 2 })
	if err := svc.stop(); err != nil {
		t.Errorf("остановка: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 4 {
		t.Fatalf("получатель увидел %d запросов, want 4: три отказа и доставку", len(ids))
	}
	for _, id := range ids {
		if id != ids[0] || !strings.HasPrefix(id, "task.due:1:") {
			t.Errorf("идентификаторы событий %q: у повторов он должен совпадать", ids)
			break
		}
	}
	if badSigns != 0 {
		t.Errorf("запросов с неверной подписью: %d", badSigns)
	}
	out := svc.logs.String()
	if !strings.Contains(out, `"msg":"reminder was not sent"`) {
		t.Error("в логе нет записи о неудачной отправке")
	}
	if strings.Contains(out, hookSecret) {
		t.Error("в лог попал секрет вебхука")
	}
}

func TestRunServiceErrors(t *testing.T) {
	log := logging.New(io.Discard, slog.LevelInfo)
	c := memoryComponents(memory.NewTasks(), &broker{})
	good := func() config.Config { return testConfig(freeAddr(t), freeAddr(t)) }

	short := good()
	short.JWTSecret = []byte("короткий")
	if err := runService(context.Background(), short, log, c); err == nil {
		t.Error("короткий секрет: runService вернул nil, want ошибку")
	}

	badProxy := good()
	badProxy.TrustedProxies = []string{"не адрес"}
	if err := runService(context.Background(), badProxy, log, c); err == nil {
		t.Error("неверный адрес прокси: runService вернул nil, want ошибку")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // тест завершается

	busy := good()
	busy.Addr = ln.Addr().String()
	if err := runService(context.Background(), busy, log, c); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Errorf("занятый порт API: error = %v, want ошибку listen", err)
	}
	// Занят служебный порт: сервис не остаётся работать наполовину, а останавливается целиком.
	busyAdmin := good()
	busyAdmin.AdminAddr = ln.Addr().String()
	if err := runService(context.Background(), busyAdmin, log, c); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Errorf("занятый служебный порт: error = %v, want ошибку listen", err)
	}
}

// ---------- два сервиса и Kafka в одном процессе ----------

// startNotifier собирает сервис notifier из тех же частей, что и cmd/notifier, и возвращает адрес его gRPC-сервера.
func startNotifier(t *testing.T, brokers []string) (addr string, stop func()) {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	svc := notifier.New(memory.NewNotifications(), notifier.NewHub(), log, nil)
	consumer, err := kafka.NewConsumer(brokers, "notifier", event.Topic, event.TopicPartitions,
		func(ctx context.Context, m kafka.Message) error { return svc.Handle(ctx, m.Value) }, log)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { consumer.Run(ctx) })
	wg.Go(func() { grpcserver.New(svc, log).Serve(ctx, ln) }) //nolint:errcheck // остановку проверяют тесты grpcserver

	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		wg.Wait()
		consumer.Close()
	}
	t.Cleanup(stop)
	return ln.Addr().String(), stop
}

type notificationsPage struct {
	Items []struct {
		ID     int64  `json:"id"`
		Kind   string `json:"kind"`
		TaskID int64  `json:"task_id"`
		Title  string `json:"title"`
	} `json:"items"`
	NextBeforeID int64 `json:"next_before_id"`
}

// Вся цепочка: задача создана через HTTP → событие в outbox → Kafka → notifier → уведомление видно через HTTP.
// Kafka здесь — kfake: настоящий протокол внутри процесса.
func TestTwoServices(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	brokers := cluster.ListenAddrs()

	notifierAddr, stopNotifier := startNotifier(t, brokers)

	producer, err := kafka.NewProducer(brokers, event.TopicPartitions)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	client, err := notifierclient.New(notifierAddr, notifierclient.WithTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close() //nolint:errcheck // тест завершается

	tasks := memory.NewTasks()
	svc := startService(t, func(ctx context.Context, addr, adminAddr string, out io.Writer) error {
		c := memoryComponents(tasks, nil)
		c.publisher, c.notifications = producer, client
		return runService(ctx, testConfig(addr, adminAddr), logging.New(out, slog.LevelDebug), c)
	})

	ann, _ := login(t, svc.base, "ann@example.com")
	bob, _ := login(t, svc.base, "bob@example.com")
	id := createOverdue(t, svc.base, ann, "сдать отчёт")

	// Два уведомления: о создании задачи и о наступившем сроке. Новые первыми.
	var page notificationsPage
	waitFor(t, 20*time.Second, "два уведомления", func() bool {
		page = notificationsPage{}
		if code := call(t, "GET", svc.base+"/api/v1/notifications", ann, "", &page); code != http.StatusOK {
			t.Fatalf("notifications: status %d\n%s", code, svc.logs)
		}
		return len(page.Items) == 2
	})
	if page.Items[0].Kind != notification.KindTaskDue || page.Items[1].Kind != notification.KindTaskCreated {
		t.Errorf("виды уведомлений: %s, %s; want task.due, task.created", page.Items[0].Kind, page.Items[1].Kind)
	}
	if n := page.Items[1]; n.TaskID != id || n.Title != "сдать отчёт" {
		t.Errorf("уведомление = %+v", n)
	}

	// Постранично: курсор следующей страницы — id последнего полученного уведомления.
	var first, second notificationsPage
	if code := call(t, "GET", svc.base+"/api/v1/notifications?limit=1", ann, "", &first); code != http.StatusOK || len(first.Items) != 1 || first.NextBeforeID != first.Items[0].ID {
		t.Fatalf("первая страница: status %d, %+v", code, first)
	}
	next := svc.base + "/api/v1/notifications?limit=1&before_id=" + strconv.FormatInt(first.NextBeforeID, 10)
	if code := call(t, "GET", next, ann, "", &second); code != http.StatusOK || len(second.Items) != 1 || second.Items[0].Kind != notification.KindTaskCreated {
		t.Errorf("вторая страница: status %d, %+v", code, second)
	}

	// Чужих уведомлений пользователь не видит.
	var empty notificationsPage
	if code := call(t, "GET", svc.base+"/api/v1/notifications", bob, "", &empty); code != http.StatusOK || empty.Items == nil || len(empty.Items) != 0 {
		t.Errorf("уведомления другого пользователя: status %d, %+v; want пустой список", code, empty)
	}
	for _, query := range []string{"limit=0", "limit=101", "limit=много", "before_id=-1", "before_id=x"} {
		if code := call(t, "GET", svc.base+"/api/v1/notifications?"+query, ann, "", nil); code != http.StatusBadRequest {
			t.Errorf("?%s: status %d, want 400", query, code)
		}
	}
	if code := call(t, "GET", svc.base+"/api/v1/notifications", "", "", nil); code != http.StatusUnauthorized {
		t.Errorf("уведомления без токена: status %d, want 401", code)
	}

	// Notifier упал: уведомления отвечают 503, задачи продолжают работать.
	stopNotifier()
	if code := call(t, "GET", svc.base+"/api/v1/notifications", ann, "", nil); code != http.StatusServiceUnavailable {
		t.Errorf("уведомления без notifier: status %d, want 503", code)
	}
	if code := call(t, "GET", svc.base+"/api/v1/tasks", ann, "", nil); code != http.StatusOK {
		t.Errorf("задачи без notifier: status %d, want 200", code)
	}
	if code := call(t, "GET", svc.base+"/readyz", "", "", nil); code != http.StatusOK {
		t.Errorf("/readyz без notifier: status %d, want 200: сосед не входит в готовность", code)
	}
}

// Kafka лежит: задача создаётся, событие ждёт в outbox. Kafka поднялась: событие ушло.
func TestEventsWaitForKafka(t *testing.T) {
	kafkaAddr := freeAddr(t) // по этому адресу пока никто не слушает
	producer, err := kafka.NewProducer([]string{kafkaAddr}, event.TopicPartitions)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	tasks := memory.NewTasks()
	svc := startService(t, func(ctx context.Context, addr, adminAddr string, out io.Writer) error {
		c := memoryComponents(tasks, nil)
		c.publisher = producer
		return runService(ctx, testConfig(addr, adminAddr), logging.New(out, slog.LevelDebug), c)
	})

	access, _ := login(t, svc.base, "ann@example.com")
	if code := call(t, "POST", svc.base+"/api/v1/tasks", access, `{"title":"без брокера"}`, nil); code != http.StatusCreated {
		t.Fatalf("создание задачи без Kafka: status %d, want 201", code)
	}
	waitFor(t, 30*time.Second, "запись об отказе публикации", func() bool {
		return strings.Contains(svc.logs.String(), `"msg":"outbox publish failed, events stay queued"`)
	})
	if n, _ := tasks.Outbox().Pending(context.Background()); n != 1 {
		t.Fatalf("в outbox %d событий, want 1", n)
	}

	_, port, _ := net.SplitHostPort(kafkaAddr)
	portNum, _ := strconv.Atoi(port)
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.Ports(portNum))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	waitFor(t, 60*time.Second, "пустой outbox после возвращения Kafka", func() bool {
		n, _ := tasks.Outbox().Pending(context.Background())
		return n == 0
	})
	if !strings.Contains(svc.logs.String(), `"msg":"outbox publish recovered"`) {
		t.Error("в логе нет записи о восстановлении публикации")
	}

	// Событие действительно лежит в топике.
	got := make(chan event.Event, 1)
	consumer, err := kafka.NewConsumer([]string{kafkaAddr}, "check", event.Topic, event.TopicPartitions,
		func(_ context.Context, m kafka.Message) error {
			e, err := event.Decode(m.Value)
			if err != nil {
				return kafka.Skip(err)
			}
			select {
			case got <- e:
			default:
			}
			return nil
		}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go consumer.Run(ctx)
	select {
	case e := <-got:
		if e.ID != "task.created:1" || e.Title != "без брокера" {
			t.Errorf("событие в топике = %+v", e)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("события нет в топике")
	}
}

// ---------- настоящий PostgreSQL ----------

// cleanup очищает таблицы тестовой базы.
func cleanup(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx) //nolint:errcheck // тест
	if _, err := conn.Exec(ctx, `TRUNCATE users, tasks, outbox RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
}

// Полный запуск командой serve на настоящем PostgreSQL. Redis заменён на miniredis, Kafka — на kfake:
// оба говорят по настоящему протоколу. Нужна база: TASKFLOW_TEST_DSN.
// В CI тот же путь проверяет запуск через docker compose.
func TestServeWithPostgres(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_TEST_DSN")
	if dsn == "" {
		t.Skip("интеграционный тест: задай TASKFLOW_TEST_DSN")
	}
	mr := miniredis.RunT(t)
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	svc := startService(t, func(ctx context.Context, addr, adminAddr string, out io.Writer) error {
		return run(ctx, []string{"serve"}, getenv(map[string]string{
			"DATABASE_URL": dsn, "REDIS_URL": "redis://" + mr.Addr(), "JWT_SECRET": testSecret,
			"ADDR": addr, "ADMIN_ADDR": adminAddr, "REMIND_INTERVAL": "20ms", "LOG_LEVEL": "debug",
			"KAFKA_BROKERS": strings.Join(cluster.ListenAddrs(), ","), "OUTBOX_INTERVAL": "20ms",
			"NOTIFIER_ADDR": "127.0.0.1:1", "NOTIFIER_TIMEOUT": "300ms", // notifier не запущен
		}), out)
	})
	// Сценарий регистрирует пользователя заново — убираем следы прошлого запуска.
	cleanup(t, dsn)

	scenario(t, svc.base)

	// Оба события ушли из таблицы outbox в Kafka.
	waitFor(t, 20*time.Second, "пустой outbox", func() bool {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx) //nolint:errcheck // тест
		var total, pending int
		if err := conn.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE published_at IS NULL) FROM outbox`).Scan(&total, &pending); err != nil {
			t.Fatal(err)
		}
		return total == 2 && pending == 0
	})

	if err := run(context.Background(), []string{"healthcheck"}, getenv(map[string]string{"ADDR": strings.TrimPrefix(svc.base, "http://")}), io.Discard); err != nil {
		t.Errorf("healthcheck: %v", err)
	}
	// Без Redis сервис перестаёт быть готовым, но остаётся живым.
	mr.Close()
	if code := call(t, "GET", svc.base+"/readyz", "", "", nil); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz без Redis: status %d, want 503", code)
	}
	if code := call(t, "GET", svc.base+"/healthz", "", "", nil); code != http.StatusOK {
		t.Errorf("/healthz без Redis: status %d, want 200", code)
	}
	if err := svc.stop(); err != nil {
		t.Errorf("остановка: %v\n%s", err, svc.logs)
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
