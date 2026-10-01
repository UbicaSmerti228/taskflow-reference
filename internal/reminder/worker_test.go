package reminder

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeStore отдаёт задачи, пока по ним не поставлена отметка.
type fakeStore struct {
	mu       sync.Mutex
	tasks    []task.Task
	reminded map[int64]int // id → сколько раз поставлена отметка
	err      error
}

func newFakeStore(n int) *fakeStore {
	s := &fakeStore{reminded: map[int64]int{}}
	due := time.Now().Add(-time.Minute)
	for id := int64(1); id <= int64(n); id++ {
		s.tasks = append(s.tasks, task.Task{ID: id, Title: "задача", DueAt: &due})
	}
	return s
}

func (s *fakeStore) DueForReminder(context.Context, time.Time) ([]task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	var due []task.Task
	for _, t := range s.tasks {
		if s.reminded[t.ID] == 0 {
			due = append(due, t)
		}
	}
	return due, nil
}

func (s *fakeStore) MarkReminded(_ context.Context, id int64, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reminded[id]++
	return nil
}

func (s *fakeStore) remindedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.reminded)
}

// fakeNotifier считает вызовы и следит, сколько их идёт одновременно.
type fakeNotifier struct {
	calls   atomic.Int64
	running atomic.Int64
	peak    atomic.Int64
	delay   time.Duration
	fail    func(call int64) bool
}

func (n *fakeNotifier) Notify(ctx context.Context, _ task.Task) error {
	call := n.calls.Add(1)
	cur := n.running.Add(1)
	defer n.running.Add(-1)
	for {
		p := n.peak.Load()
		if cur <= p || n.peak.CompareAndSwap(p, cur) {
			break
		}
	}
	select {
	case <-time.After(n.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if n.fail != nil && n.fail(call) {
		return errors.New("сервис уведомлений недоступен")
	}
	return nil
}

// runUntil запускает воркер, ждёт выполнения условия и останавливает его.
func runUntil(t *testing.T, w *Worker, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(stopped)
	}()

	deadline := time.After(5 * time.Second)
	for !cond() {
		select {
		case <-deadline:
			cancel()
			t.Fatal("условие не выполнилось за 5 секунд")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены контекста")
	}
}

func TestWorkerRemindsEachTaskOnce(t *testing.T) {
	store, notifier := newFakeStore(20), &fakeNotifier{delay: time.Millisecond}
	w := New(store, notifier, time.Millisecond, 4, quiet)

	runUntil(t, w, func() bool { return store.remindedCount() == 20 })

	// Даём воркеру сделать ещё несколько обходов: повторных напоминаний быть не должно.
	runUntil(t, w, func() bool { return true })

	if got := notifier.calls.Load(); got != 20 {
		t.Errorf("Notify вызван %d раз, want 20", got)
	}
	for id, n := range store.reminded {
		if n != 1 {
			t.Errorf("задача %d отмечена %d раз, want 1", id, n)
		}
	}
}

func TestWorkerLimitsConcurrency(t *testing.T) {
	store, notifier := newFakeStore(30), &fakeNotifier{delay: 5 * time.Millisecond}
	w := New(store, notifier, time.Millisecond, 3, quiet)

	runUntil(t, w, func() bool { return store.remindedCount() == 30 })

	if peak := notifier.peak.Load(); peak > 3 {
		t.Errorf("одновременно работало %d напоминаний, want не больше 3", peak)
	} else if peak < 2 {
		t.Errorf("одновременно работало %d напоминаний, want параллельную работу", peak)
	}
}

func TestWorkerRetriesFailedNotification(t *testing.T) {
	store := newFakeStore(1)
	notifier := &fakeNotifier{fail: func(call int64) bool { return call <= 2 }} // первые две попытки падают
	w := New(store, notifier, time.Millisecond, 1, quiet)

	runUntil(t, w, func() bool { return store.remindedCount() == 1 })

	if got := notifier.calls.Load(); got != 3 {
		t.Errorf("Notify вызван %d раз, want 3: две неудачи и успех", got)
	}
}

func TestWorkerStopsWhileNotifying(t *testing.T) {
	store, notifier := newFakeStore(10), &fakeNotifier{delay: time.Hour} // уведомление «зависло»
	w := New(store, notifier, time.Millisecond, 2, quiet)

	runUntil(t, w, func() bool { return notifier.running.Load() == 2 })

	if got := store.remindedCount(); got != 0 {
		t.Errorf("отмечено %d задач, want 0: прерванное напоминание не считается отправленным", got)
	}
}

func TestWorkerSurvivesStoreError(t *testing.T) {
	store, notifier := newFakeStore(1), &fakeNotifier{}
	store.err = errors.New("файл недоступен")
	w := New(store, notifier, time.Millisecond, 1, quiet)

	runUntil(t, w, func() bool { return true })
	if got := notifier.calls.Load(); got != 0 {
		t.Fatalf("Notify вызван %d раз при ошибке хранилища, want 0", got)
	}

	// Хранилище починилось — воркер продолжает работу.
	store.mu.Lock()
	store.err = nil
	store.mu.Unlock()
	runUntil(t, w, func() bool { return store.remindedCount() == 1 })
}

func TestNewClampsArguments(t *testing.T) {
	w := New(newFakeStore(0), &fakeNotifier{}, 0, 0, quiet)
	if w.workers != 1 || w.interval <= 0 {
		t.Errorf("New(…, 0, 0): workers = %d, interval = %v; want 1 и положительный интервал", w.workers, w.interval)
	}
}

func TestLogNotifier(t *testing.T) {
	due := time.Now()
	if err := (LogNotifier{Log: quiet}).Notify(context.Background(), task.Task{ID: 1, Title: "задача", DueAt: &due}); err != nil {
		t.Errorf("Notify() error = %v, want nil", err)
	}
}
