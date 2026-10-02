package memory

import (
	"context"
	"sync"

	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
)

// Outbox хранит события в памяти. Ведёт себя как таблица outbox в PostgreSQL.
type Outbox struct {
	mu        sync.Mutex
	records   []event.Record
	published int  // сколько первых записей уже опубликовано
	draining  bool // публикация идёт: вторая не начнётся, пока не закончится первая

	Err error // если задано, все методы возвращают эту ошибку
}

// add записывает событие. Повторная запись события с тем же id ничего не меняет.
func (o *Outbox) add(e event.Event) error {
	rec, err := e.Record()
	if err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, r := range o.records {
		if r.EventID == rec.EventID {
			return nil
		}
	}
	o.records = append(o.records, rec)
	return nil
}

// Drain отдаёт publish до limit неопубликованных событий в порядке записи
// и помечает их опубликованными, если publish вернул nil.
func (o *Outbox) Drain(ctx context.Context, limit int, publish func(context.Context, []event.Record) error) (int, error) {
	o.mu.Lock()
	if o.Err != nil {
		o.mu.Unlock()
		return 0, o.Err
	}
	if o.draining {
		o.mu.Unlock()
		return 0, nil
	}
	end := min(o.published+max(limit, 0), len(o.records))
	batch := append([]event.Record(nil), o.records[o.published:end]...)
	if len(batch) == 0 {
		o.mu.Unlock()
		return 0, nil
	}
	o.draining = true
	o.mu.Unlock()

	// Публикация идёт без мьютекса: пока Kafka отвечает, задачи продолжают создаваться.
	err := publish(ctx, batch)

	o.mu.Lock()
	defer o.mu.Unlock()
	o.draining = false
	if err != nil {
		return 0, err
	}
	o.published = end
	return len(batch), nil
}

// Pending возвращает число событий, которые ещё не опубликованы.
func (o *Outbox) Pending(context.Context) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.Err != nil {
		return 0, o.Err
	}
	return len(o.records) - o.published, nil
}
