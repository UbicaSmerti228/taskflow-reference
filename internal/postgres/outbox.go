package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
)

// outboxLock — номер блокировки, под которой публикуются события. Число произвольное,
// важно только, чтобы оно не совпадало с другими advisory-блокировками в этой базе.
const outboxLock = 7_205_911_337

// Outbox читает события, которые репозиторий задач записал вместе с изменениями.
type Outbox struct {
	pool *pgxpool.Pool
}

// NewOutbox возвращает хранилище событий.
func NewOutbox(pool *pgxpool.Pool) *Outbox { return &Outbox{pool: pool} }

// addEvent записывает событие в рамках транзакции tx: оно появится в таблице, только если транзакция закоммитится.
func addEvent(ctx context.Context, tx pgx.Tx, e event.Event) error {
	rec, err := e.Record()
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (event_id, topic, key, payload) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (event_id) DO NOTHING`,
		rec.EventID, rec.Topic, rec.Key, rec.Payload)
	if err != nil {
		return fmt.Errorf("add event %s: %w", rec.EventID, err)
	}
	return nil
}

// Drain отдаёт publish до limit неопубликованных событий в порядке записи.
// Если publish вернул nil, события помечаются опубликованными; иначе они останутся в очереди.
// Возвращает число опубликованных событий.
//
// Публикует всегда один экземпляр сервиса: остальные не получают блокировку и возвращают ноль.
// Так события уходят в том порядке, в котором записаны.
func (o *Outbox) Drain(ctx context.Context, limit int, publish func(context.Context, []event.Record) error) (int, error) {
	published := 0
	err := pgx.BeginFunc(ctx, o.pool, func(tx pgx.Tx) error {
		// Блокировка снимается сама в конце транзакции, даже если процесс упадёт.
		var locked bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, int64(outboxLock)).Scan(&locked); err != nil {
			return err
		}
		if !locked {
			return nil
		}

		rows, err := tx.Query(ctx,
			`SELECT id, event_id, topic, key, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT $1`, limit)
		if err != nil {
			return err
		}
		var ids []int64
		batch, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (event.Record, error) {
			var (
				id  int64
				rec event.Record
			)
			err := row.Scan(&id, &rec.EventID, &rec.Topic, &rec.Key, &rec.Payload)
			ids = append(ids, id)
			return rec, err
		})
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}

		// Kafka подтвердила запись — только после этого ставим отметку. Если коммит ниже не пройдёт,
		// события уйдут второй раз: доставка «хотя бы один раз», дубликаты отбрасывает получатель.
		if err := publish(ctx, batch); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
			return err
		}
		published = len(batch)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("drain outbox: %w", err)
	}
	return published, nil
}

// Pending возвращает число событий, которые ещё не опубликованы.
func (o *Outbox) Pending(ctx context.Context) (int, error) {
	var n int
	if err := o.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending events: %w", err)
	}
	return n, nil
}
