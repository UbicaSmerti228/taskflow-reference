package main

import (
	"bytes"
	"context"
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

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kfake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/UbicaSmerti228/taskflow-reference/internal/config"
	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
	"github.com/UbicaSmerti228/taskflow-reference/internal/gen/notifierv1"
	"github.com/UbicaSmerti228/taskflow-reference/internal/kafka"
	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/memory"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

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
		{name: "неизвестная команда", args: []string{"consume"}, want: `неизвестная команда "consume"`},
		{name: "migrate без адреса базы", args: []string{"migrate"}, want: "DATABASE_URL"},
		{name: "serve без настроек", args: []string{"serve"}, want: "KAFKA_BROKERS"},
		{name: "serve с недоступной базой", args: []string{"serve"}, env: map[string]string{"DATABASE_URL": "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=2", "KAFKA_BROKERS": "127.0.0.1:1"}, want: "connect"},
		{name: "healthcheck без сервиса", args: []string{"healthcheck"}, env: map[string]string{"GRPC_ADDR": "127.0.0.1:1"}, want: "connect"},
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

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // порт нужен только на мгновение
	return ln.Addr().String()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// running — сервис, запущенный в тесте.
type running struct {
	grpcAddr, adminAddr string
	client              notifierv1.NotifierServiceClient
	logs                *syncBuffer
	stop                func() error
}

// startService запускает сервис и ждёт, пока его gRPC-сервер начнёт отвечать на проверку здоровья.
func startService(t *testing.T, start func(ctx context.Context, grpcAddr, adminAddr string, logs io.Writer) error) *running {
	t.Helper()
	svc := &running{grpcAddr: freeAddr(t), adminAddr: freeAddr(t), logs: &syncBuffer{}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- start(ctx, svc.grpcAddr, svc.adminAddr, svc.logs) }()

	waitFor(t, "ответ на проверку здоровья", func() bool {
		select {
		case err := <-done:
			t.Fatalf("сервис завершился при старте: %v\n%s", err, svc.logs)
		default:
		}
		return healthcheck(context.Background(), svc.grpcAddr) == nil
	})

	conn, err := grpc.NewClient(svc.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	svc.client = notifierv1.NewNotifierServiceClient(conn)

	stopped := false
	svc.stop = func() error {
		if stopped {
			return nil
		}
		stopped = true
		conn.Close() //nolint:errcheck // соединение больше не нужно
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

func newKafka(t *testing.T) (brokers []string, producer *kafka.Producer) {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	producer, err = kafka.NewProducer(cluster.ListenAddrs(), event.TopicPartitions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	return cluster.ListenAddrs(), producer
}

func publish(t *testing.T, p *kafka.Producer, events ...event.Event) {
	t.Helper()
	batch := make([]event.Record, len(events))
	for i, e := range events {
		rec, err := e.Record()
		if err != nil {
			t.Fatal(err)
		}
		batch[i] = rec
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := p.Publish(ctx, batch); err != nil {
		t.Fatal(err)
	}
}

func list(t *testing.T, svc *running, userID int64) []*notifierv1.Notification {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := svc.client.ListNotifications(ctx, &notifierv1.ListNotificationsRequest{UserId: userID, Limit: 100})
	if err != nil {
		t.Fatalf("ListNotifications: %v\n%s", err, svc.logs)
	}
	return resp.GetNotifications()
}

var (
	due    = time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	sample = task.Task{ID: 7, Title: "сдать отчёт", DueAt: &due, CreatedAt: time.Date(2026, 4, 30, 9, 0, 0, 0, time.UTC)}
)

// Сервис на хранилище в памяти и kfake: события из Kafka становятся уведомлениями,
// повторы и мусор их не портят, стрим получает новые уведомления.
func TestRunService(t *testing.T) {
	brokers, producer := newKafka(t)
	store := memory.NewNotifications()
	svc := startService(t, func(ctx context.Context, grpcAddr, adminAddr string, out io.Writer) error {
		cfg := config.Notifier{GRPCAddr: grpcAddr, AdminAddr: adminAddr, KafkaBrokers: brokers, KafkaGroup: "notifier", ShutdownTimeout: 5 * time.Second}
		return runService(ctx, cfg, logging.New(out, slog.LevelDebug), store)
	})

	// Стрим открыт до события: уведомление придёт в него, как только событие будет обработано.
	streamCtx, cancelStream := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelStream()
	stream, err := svc.client.WatchNotifications(streamCtx, &notifierv1.WatchNotificationsRequest{UserId: 3})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "подписка стрима", func() bool {
		_, body := fetch(t, "http://"+svc.adminAddr+"/metrics")
		return strings.Contains(body, "notifier_stream_subscribers 1")
	})

	created := event.TaskCreated(3, sample)
	publish(t, producer, created)
	pushed, err := stream.Recv()
	if err != nil {
		t.Fatalf("стрим: %v", err)
	}
	if n := pushed.GetNotification(); n.GetKind() != notifierv1.Kind_KIND_TASK_CREATED || n.GetTaskId() != 7 || n.GetTitle() != "сдать отчёт" {
		t.Errorf("из стрима пришло %v", n)
	}

	// То же событие ещё дважды, битое сообщение, событие незнакомого типа и напоминание.
	publish(t, producer, created, created)
	publish(t, producer, event.Event{ID: "x", Type: "task.archived", TaskID: 7, UserID: 3})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := producer.Publish(ctx, []event.Record{{EventID: "broken", Topic: event.Topic, Key: "7", Payload: []byte(`{не json`)}}); err != nil {
		t.Fatal(err)
	}
	publish(t, producer, event.TaskDue(3, sample, due))

	// События одной задачи лежат в одной партиции: напоминание обработано последним, значит всё до него — тоже.
	waitFor(t, "два уведомления", func() bool { return len(list(t, svc, 3)) == 2 })
	got := list(t, svc, 3)
	if got[0].GetKind() != notifierv1.Kind_KIND_TASK_DUE || got[1].GetKind() != notifierv1.Kind_KIND_TASK_CREATED {
		t.Errorf("виды уведомлений: %v, %v", got[0].GetKind(), got[1].GetKind())
	}

	_, body := fetch(t, "http://"+svc.adminAddr+"/metrics")
	for _, want := range []string{
		`notifier_events_total{result="saved",type="task.created"} 1`,
		`notifier_events_total{result="duplicate",type="task.created"} 2`,
		`notifier_events_total{result="saved",type="task.due"} 1`,
		`notifier_events_total{result="ignored",type="unknown"} 1`,
		`notifier_events_total{result="malformed",type="unknown"} 1`,
		`notifier_grpc_calls_total{code="OK",method="/taskflow.notifier.v1.NotifierService/ListNotifications"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("в /metrics нет строки %s", want)
		}
	}
	if code, _ := fetch(t, "http://"+svc.adminAddr+"/debug/pprof/heap"); code != http.StatusOK {
		t.Errorf("pprof на служебном порту: status %d", code)
	}

	if err := svc.stop(); err != nil {
		t.Errorf("остановка: %v", err)
	}
	if err := healthcheck(context.Background(), svc.grpcAddr); err == nil {
		t.Error("после остановки проверка здоровья проходит")
	}
	out := svc.logs.String()
	for _, want := range []string{`"msg":"grpc server started"`, `"msg":"kafka consumer started"`, `"msg":"notification saved"`, `"msg":"message skipped"`, `"msg":"grpc server stopped"`} {
		if !strings.Contains(out, want) {
			t.Errorf("в логе нет записи %s", want)
		}
	}
}

// Перезапуск: обработанные события не обрабатываются заново, а пришедшие за время простоя не теряются.
func TestRestartKeepsPosition(t *testing.T) {
	brokers, producer := newKafka(t)
	store := memory.NewNotifications()
	start := func() *running {
		return startService(t, func(ctx context.Context, grpcAddr, adminAddr string, out io.Writer) error {
			cfg := config.Notifier{GRPCAddr: grpcAddr, AdminAddr: adminAddr, KafkaBrokers: brokers, KafkaGroup: "notifier", ShutdownTimeout: 5 * time.Second}
			return runService(ctx, cfg, logging.New(out, slog.LevelDebug), store)
		})
	}

	first := start()
	publish(t, producer, event.TaskCreated(3, sample))
	waitFor(t, "первое уведомление", func() bool { return len(list(t, first, 3)) == 1 })
	if err := first.stop(); err != nil {
		t.Fatalf("остановка: %v", err)
	}

	// Событие пришло, пока сервис не работал.
	publish(t, producer, event.TaskDue(3, sample, due))

	second := start()
	waitFor(t, "второе уведомление после перезапуска", func() bool { return len(list(t, second, 3)) == 2 })
	_, body := fetch(t, "http://"+second.adminAddr+"/metrics")
	// Первое событие подтверждено до остановки: после перезапуска оно даже не читается.
	if strings.Contains(body, `type="task.created"`) {
		t.Error("после перезапуска уже обработанное событие прочитано снова")
	}
}

func TestRunServiceErrors(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // тест завершается
	brokers, _ := newKafka(t)
	log := logging.New(io.Discard, slog.LevelInfo)

	busy := config.Notifier{GRPCAddr: ln.Addr().String(), AdminAddr: freeAddr(t), KafkaBrokers: brokers, KafkaGroup: "g", ShutdownTimeout: time.Second}
	if err := runService(context.Background(), busy, log, memory.NewNotifications()); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Errorf("занятый порт gRPC: error = %v, want ошибку listen", err)
	}
	busyAdmin := config.Notifier{GRPCAddr: freeAddr(t), AdminAddr: ln.Addr().String(), KafkaBrokers: brokers, KafkaGroup: "g", ShutdownTimeout: time.Second}
	if err := runService(context.Background(), busyAdmin, log, memory.NewNotifications()); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Errorf("занятый служебный порт: error = %v, want ошибку listen", err)
	}
}

// Полный запуск командой serve на настоящем PostgreSQL. Нужна база: TASKFLOW_TEST_DSN.
func TestServeWithPostgres(t *testing.T) {
	dsn := os.Getenv("TASKFLOW_TEST_DSN")
	if dsn == "" {
		t.Skip("интеграционный тест: задай TASKFLOW_TEST_DSN")
	}
	brokers, producer := newKafka(t)

	var out bytes.Buffer
	if err := run(context.Background(), []string{"migrate"}, getenv(map[string]string{"DATABASE_URL": dsn}), &out); err != nil || !strings.Contains(out.String(), "Миграции применены") {
		t.Fatalf("migrate: %v, вывод %q", err, out.String())
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx) //nolint:errcheck // тест
	if _, err := conn.Exec(ctx, `TRUNCATE notifier.notifications, notifier.processed_events RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}

	svc := startService(t, func(ctx context.Context, grpcAddr, adminAddr string, out io.Writer) error {
		return run(ctx, []string{"serve"}, getenv(map[string]string{
			"DATABASE_URL": dsn, "KAFKA_BROKERS": strings.Join(brokers, ","), "GRPC_ADDR": grpcAddr, "ADMIN_ADDR": adminAddr, "LOG_LEVEL": "debug",
		}), out)
	})

	created := event.TaskCreated(3, sample)
	publish(t, producer, created, created, event.TaskDue(3, sample, due))
	waitFor(t, "два уведомления", func() bool { return len(list(t, svc, 3)) == 2 })

	var notifications, processed int
	if err := conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM notifier.notifications), (SELECT count(*) FROM notifier.processed_events)`).Scan(&notifications, &processed); err != nil {
		t.Fatal(err)
	}
	if notifications != 2 || processed != 2 {
		t.Errorf("в базе %d уведомлений и %d обработанных событий, want 2 и 2", notifications, processed)
	}
	if err := run(ctx, []string{"healthcheck"}, getenv(map[string]string{"GRPC_ADDR": svc.grpcAddr}), io.Discard); err != nil {
		t.Errorf("healthcheck: %v", err)
	}
	if err := svc.stop(); err != nil {
		t.Errorf("остановка: %v\n%s", err, svc.logs)
	}
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
