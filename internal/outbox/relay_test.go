package outbox

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
	"github.com/UbicaSmerti228/taskflow-reference/internal/memory"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// broker — брокер в памяти: запоминает опубликованное и умеет «падать».
type broker struct {
	mu   sync.Mutex
	got  []event.Record
	down bool
}

func (b *broker) Publish(_ context.Context, batch []event.Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.down {
		return errors.New("broker is down")
	}
	b.got = append(b.got, batch...)
	return nil
}

func (b *broker) setDown(down bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.down = down
}

func (b *broker) ids() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]string, len(b.got))
	for i, r := range b.got {
		ids[i] = r.EventID
	}
	return ids
}

// counters — метрики, которые можно прочитать в тесте.
type counters struct {
	mu                         sync.Mutex
	published, failed, pending int
}

func (c *counters) Published(n int) { c.mu.Lock(); c.published += n; c.mu.Unlock() }
func (c *counters) Failed()         { c.mu.Lock(); c.failed++; c.mu.Unlock() }
func (c *counters) Pending(n int)   { c.mu.Lock(); c.pending = n; c.mu.Unlock() }
func (c *counters) read() (published, failed, pending int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.published, c.failed, c.pending
}

// syncBuffer — буфер логов, в который Relay пишет из своей горутины.
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

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func createTasks(t *testing.T, tasks *memory.Tasks, n int) {
	t.Helper()
	for range n {
		if _, err := tasks.Create(context.Background(), 1, task.NewTask{Title: "задача"}); err != nil {
			t.Fatal(err)
		}
	}
}

// run запускает Relay и возвращает функцию, которая останавливает его и ждёт завершения.
func run(r *Relay) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}

func TestRelayPublishesInOrder(t *testing.T) {
	tasks, b, m := memory.NewTasks(), &broker{}, &counters{}
	createTasks(t, tasks, 7)

	// Пачка меньше очереди: Relay не ждёт следующего тика, а публикует, пока очередь не опустеет.
	stop := run(New(tasks.Outbox(), b, slog.New(slog.DiscardHandler), WithInterval(time.Hour), WithBatch(3), WithMetrics(m)))
	waitFor(t, "семь событий в брокере", func() bool { return len(b.ids()) == 7 })
	stop()

	for i, id := range b.ids() {
		if want := event.TaskCreated(1, task.Task{ID: int64(i + 1)}).ID; id != want {
			t.Fatalf("событие %d = %s, want %s", i, id, want)
		}
	}
	if published, failed, pending := m.read(); published != 7 || failed != 0 || pending != 0 {
		t.Errorf("метрики: опубликовано %d, отказов %d, в очереди %d; want 7, 0, 0", published, failed, pending)
	}
}

// Брокер лежит: задачи создаются, события копятся. Брокер поднялся: события ушли, ни одно не потеряно.
func TestRelaySurvivesBrokerOutage(t *testing.T) {
	tasks, b, m := memory.NewTasks(), &broker{down: true}, &counters{}
	logs := &syncBuffer{}
	log := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	stop := run(New(tasks.Outbox(), b, log, WithInterval(time.Millisecond), WithMetrics(m)))
	defer stop()

	createTasks(t, tasks, 3)
	waitFor(t, "несколько неудачных попыток", func() bool { _, failed, _ := m.read(); return failed >= 5 })
	if _, _, pending := m.read(); pending != 3 || len(b.ids()) != 0 {
		t.Fatalf("при лежащем брокере: в очереди %d, опубликовано %d; want 3 и 0", pending, len(b.ids()))
	}

	b.setDown(false)
	waitFor(t, "три события в брокере", func() bool { return len(b.ids()) == 3 })
	waitFor(t, "пустая очередь", func() bool { _, _, pending := m.read(); return pending == 0 })
	stop()

	out := logs.String()
	// Отказов было много, а предупреждение одно: лог не засоряется, пока брокер лежит.
	if n := strings.Count(out, "outbox publish failed"); n != 1 {
		t.Errorf("предупреждений об отказе в логе: %d, want 1", n)
	}
	if !strings.Contains(out, "outbox publish recovered") {
		t.Error("в логе нет записи о восстановлении")
	}
}

func TestRelayStoreFailure(t *testing.T) {
	tasks, b, m := memory.NewTasks(), &broker{}, &counters{}
	createTasks(t, tasks, 1)
	tasks.Outbox().Err = errors.New("database is down")

	stop := run(New(tasks.Outbox(), b, slog.New(slog.DiscardHandler), WithInterval(time.Millisecond), WithMetrics(m), WithTimeout(time.Second)))
	waitFor(t, "отказ хранилища в метриках", func() bool { _, failed, _ := m.read(); return failed > 0 })
	stop()
	if len(b.ids()) != 0 {
		t.Error("при отказе хранилища что-то опубликовано")
	}
}

// Без метрик и с некорректными настройками Relay тоже работает.
func TestRelayDefaults(t *testing.T) {
	tasks, b := memory.NewTasks(), &broker{}
	createTasks(t, tasks, 2)
	stop := run(New(tasks.Outbox(), b, slog.New(slog.DiscardHandler), WithInterval(0), WithBatch(0)))
	waitFor(t, "два события в брокере", func() bool { return len(b.ids()) == 2 })
	stop()
}
