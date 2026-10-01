// Package reminder напоминает о задачах, срок которых наступил.
package reminder

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// Store — то, что воркеру нужно от хранилища.
type Store interface {
	DueForReminder(ctx context.Context, now time.Time) ([]task.Task, error)
	MarkReminded(ctx context.Context, id int64, at time.Time) error
}

// Notifier отправляет одно напоминание. В этой версии оно пишется в лог,
// позже здесь появится отдельный сервис уведомлений.
type Notifier interface {
	Notify(ctx context.Context, t task.Task) error
}

// Worker раз в interval ищет просроченные задачи и раздаёт их пулу из workers горутин.
type Worker struct {
	store    Store
	notifier Notifier
	interval time.Duration
	workers  int
	now      func() time.Time
}

// New собирает воркер. Некорректные значения заменяются на минимальные рабочие.
func New(store Store, notifier Notifier, interval time.Duration, workers int) *Worker {
	return &Worker{
		store:    store,
		notifier: notifier,
		interval: max(interval, time.Millisecond),
		workers:  max(workers, 1),
		now:      time.Now,
	}
}

type job struct {
	task task.Task
	done func()
}

// Run работает, пока не отменён ctx, и возвращается, когда все горутины пула завершились.
func (w *Worker) Run(ctx context.Context) {
	jobs := make(chan job)

	var pool sync.WaitGroup
	for range w.workers {
		pool.Go(func() {
			for j := range jobs {
				w.remind(ctx, j.task)
				j.done()
			}
		})
	}
	// Канал закрывает отправитель, и только после этого ждём пул.
	defer func() {
		close(jobs)
		pool.Wait()
	}()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		w.scan(ctx, jobs)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// scan раздаёт пулу все просроченные задачи и ждёт, пока пачка обработается.
// Следующий обход начнётся только после этого, поэтому одна задача не попадёт в работу дважды.
func (w *Worker) scan(ctx context.Context, jobs chan<- job) {
	tasks, err := w.store.DueForReminder(ctx, w.now())
	if err != nil {
		log.Printf("reminder: %v", err)
		return
	}

	var batch sync.WaitGroup
	defer batch.Wait()
	for _, t := range tasks {
		batch.Add(1)
		select {
		case jobs <- job{task: t, done: batch.Done}:
		case <-ctx.Done():
			batch.Done()
			return
		}
	}
}

func (w *Worker) remind(ctx context.Context, t task.Task) {
	if err := w.notifier.Notify(ctx, t); err != nil {
		// Отметку не ставим: задача попадёт в следующий обход.
		log.Printf("reminder: notify task %d: %v", t.ID, err)
		return
	}
	// Отметку ставим даже при остановке сервиса: напоминание уже ушло.
	if err := w.store.MarkReminded(context.WithoutCancel(ctx), t.ID, w.now()); err != nil {
		log.Printf("reminder: %v", err)
	}
}

// LogNotifier пишет напоминание в лог.
type LogNotifier struct{}

// Notify печатает напоминание о задаче.
func (LogNotifier) Notify(_ context.Context, t task.Task) error {
	log.Printf("напоминание: задача %d «%s», срок был %s", t.ID, t.Title, t.DueAt.Format(time.RFC3339))
	return nil
}
