// Package instrument добавляет наблюдаемость к хранилищам, не меняя их код.
package instrument

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/service"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// Metrics получает длительность и исход каждого обращения к хранилищу. Реализация — в пакете metrics.
type Metrics interface {
	Call(op string, failed bool, took time.Duration)
}

type noMetrics struct{}

func (noMetrics) Call(string, bool, time.Duration) {}

// tasks — декоратор: реализует тот же интерфейс, что и обёрнутое хранилище,
// и до и после каждого вызова делает своё. Сервис не знает, что работает через обёртку.
type tasks struct {
	next    service.TaskRepo
	log     *slog.Logger
	slow    time.Duration
	metrics Metrics
	now     func() time.Time
}

// Tasks оборачивает хранилище задач: каждый вызов попадает в метрики и в лог с длительностью,
// медленные (дольше slow) и неудачные — с уровнем warn и error. metrics может быть nil.
func Tasks(next service.TaskRepo, log *slog.Logger, slow time.Duration, metrics Metrics) service.TaskRepo {
	if metrics == nil {
		metrics = noMetrics{}
	}
	return &tasks{next: next, log: log, slow: slow, metrics: metrics, now: time.Now}
}

// observe возвращает функцию, которую вызывают после операции: defer d.observe(...)(&err).
func (d *tasks) observe(ctx context.Context, op string) func(*error) {
	start := d.now()
	return func(errp *error) {
		took := d.now().Sub(start)
		err := *errp
		// «Не найдено» и отмена запроса клиентом — обычные исходы, а не сбой хранилища.
		failed := err != nil && !errors.Is(err, task.ErrNotFound) && !errors.Is(err, context.Canceled)
		d.metrics.Call(op, failed, took)
		switch {
		case failed:
			d.log.ErrorContext(ctx, "repository call failed", "op", op, "duration_ms", took.Milliseconds(), "err", err)
		case took >= d.slow:
			d.log.WarnContext(ctx, "slow repository call", "op", op, "duration_ms", took.Milliseconds())
		default:
			d.log.DebugContext(ctx, "repository call", "op", op, "duration_ms", took.Milliseconds())
		}
	}
}

func (d *tasks) Create(ctx context.Context, userID int64, in task.NewTask) (t task.Task, err error) {
	defer d.observe(ctx, "tasks.create")(&err)
	return d.next.Create(ctx, userID, in)
}

func (d *tasks) Get(ctx context.Context, userID, id int64) (t task.Task, err error) {
	defer d.observe(ctx, "tasks.get")(&err)
	return d.next.Get(ctx, userID, id)
}

func (d *tasks) List(ctx context.Context, userID int64, f task.Filter) (items []task.Task, total int, err error) {
	defer d.observe(ctx, "tasks.list")(&err)
	return d.next.List(ctx, userID, f)
}

func (d *tasks) Update(ctx context.Context, userID, id int64, p task.Patch) (t task.Task, err error) {
	defer d.observe(ctx, "tasks.update")(&err)
	return d.next.Update(ctx, userID, id, p)
}

func (d *tasks) Delete(ctx context.Context, userID, id int64) (err error) {
	defer d.observe(ctx, "tasks.delete")(&err)
	return d.next.Delete(ctx, userID, id)
}
