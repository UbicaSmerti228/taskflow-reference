// Package outbox публикует события, которые сервис записал в базу вместе с изменениями данных.
//
// Сервис не пишет в Kafka из обработчика запроса: брокер может быть недоступен, а запрос уже
// закоммичен — событие потерялось бы. Вместо этого событие ложится в таблицу той же транзакцией,
// а Relay переносит его в Kafka, когда та доступна.
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
)

// Store — что Relay нужно от хранилища событий.
type Store interface {
	// Drain отдаёт publish до limit неопубликованных событий в порядке записи и помечает их
	// опубликованными, если publish вернул nil. Возвращает число опубликованных событий.
	Drain(ctx context.Context, limit int, publish func(context.Context, []event.Record) error) (int, error)
	// Pending возвращает число событий, которые ещё ждут публикации.
	Pending(ctx context.Context) (int, error)
}

// Publisher отправляет пачку событий в брокер и возвращает nil, только когда брокер подтвердил все.
type Publisher interface {
	Publish(ctx context.Context, batch []event.Record) error
}

// Metrics получает числа о работе Relay. Реализация — в пакете metrics.
type Metrics interface {
	Published(n int)
	Failed()
	Pending(n int)
}

type noMetrics struct{}

func (noMetrics) Published(int) {}
func (noMetrics) Failed()       {}
func (noMetrics) Pending(int)   {}

// Relay раз в interval переносит события из хранилища в брокер.
type Relay struct {
	store    Store
	pub      Publisher
	interval time.Duration
	batch    int
	timeout  time.Duration
	log      *slog.Logger
	metrics  Metrics
	failing  bool // предыдущая попытка не удалась: повторные отказы пишем тише
}

// Option меняет одну настройку Relay.
type Option func(*Relay)

// WithInterval задаёт, как часто проверять новые события.
func WithInterval(d time.Duration) Option { return func(r *Relay) { r.interval = d } }

// WithBatch задаёт, сколько событий публиковать за один раз.
func WithBatch(n int) Option { return func(r *Relay) { r.batch = n } }

// WithTimeout ограничивает одну публикацию: на это время в базе открыта транзакция.
func WithTimeout(d time.Duration) Option { return func(r *Relay) { r.timeout = d } }

// WithMetrics подключает метрики.
func WithMetrics(m Metrics) Option { return func(r *Relay) { r.metrics = m } }

// New собирает Relay. По умолчанию: проверка раз в секунду, до 100 событий за раз, 10 секунд на публикацию.
func New(store Store, pub Publisher, log *slog.Logger, opts ...Option) *Relay {
	r := &Relay{store: store, pub: pub, interval: time.Second, batch: 100, timeout: 10 * time.Second, log: log, metrics: noMetrics{}}
	for _, opt := range opts {
		opt(r)
	}
	r.interval, r.batch = max(r.interval, time.Millisecond), max(r.batch, 1)
	return r
}

// Run работает, пока не отменён ctx. События, которые не успели уйти, останутся в хранилище
// и будут опубликованы после следующего запуска.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		r.drain(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// drain публикует всё, что накопилось: пачку за пачкой, пока очередь не опустеет.
func (r *Relay) drain(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := r.once(ctx)
		if err != nil {
			r.metrics.Failed()
			// Kafka может лежать долго: первый отказ — предупреждение, следующие подряд — только в debug.
			level := slog.LevelWarn
			if r.failing {
				level = slog.LevelDebug
			}
			r.log.Log(ctx, level, "outbox publish failed, events stay queued", "err", err)
			r.failing = true
			break
		}
		if r.failing {
			r.log.InfoContext(ctx, "outbox publish recovered")
			r.failing = false
		}
		if n > 0 {
			r.metrics.Published(n)
			r.log.DebugContext(ctx, "outbox events published", "count", n)
		}
		if n < r.batch {
			break
		}
	}
	if n, err := r.store.Pending(context.WithoutCancel(ctx)); err == nil {
		r.metrics.Pending(n)
	}
}

func (r *Relay) once(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return r.store.Drain(ctx, r.batch, r.pub.Publish)
}
