// Package resilience содержит приёмы устойчивости при вызове чужих сервисов: повторы и circuit breaker.
package resilience

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Retry повторяет вызов с растущей паузой. Нулевое значение делает одну попытку.
type Retry struct {
	Attempts int           // всего попыток, включая первую
	Base     time.Duration // пауза перед второй попыткой, дальше удваивается
	Max      time.Duration // потолок паузы; 0 — без потолка

	// Подменяются в тестах. nil — настоящие случайные числа и настоящее ожидание.
	Rand  func() float64
	Sleep func(ctx context.Context, d time.Duration) error
}

// permanent помечает ошибку, после которой повторять бессмысленно.
type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent оборачивает ошибку, которую повтор не исправит: например, ответ 400.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanent{err}
}

// Do вызывает fn, пока тот не вернёт nil, не кончатся попытки или не отменится ctx.
// Возвращает ошибку последней попытки.
func (r Retry) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	sleep := r.Sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	var err error
	for attempt := range max(r.Attempts, 1) {
		if attempt > 0 {
			if sleepErr := sleep(ctx, r.backoff(attempt)); sleepErr != nil {
				// Контекст отменён во время паузы: полезнее вернуть причину последней неудачи.
				return errors.Join(err, sleepErr)
			}
		}
		err = fn(ctx)
		if err == nil {
			return nil
		}
		var p permanent
		if errors.As(err, &p) {
			return p.err
		}
		if ctx.Err() != nil {
			return err
		}
	}
	return err
}

// backoff считает паузу перед попыткой с номером attempt (первая повторная — 1).
// Потолок растёт вдвое с каждой попыткой, сама пауза — случайная от нуля до потолка (full jitter):
// клиенты, упавшие одновременно, не придут повторно одной волной.
func (r Retry) backoff(attempt int) time.Duration {
	ceil := r.Base
	for range attempt - 1 {
		if r.Max > 0 && ceil >= r.Max {
			break
		}
		ceil *= 2
	}
	if r.Max > 0 {
		ceil = min(ceil, r.Max)
	}
	rnd := r.Rand
	if rnd == nil {
		rnd = rand.Float64
	}
	return time.Duration(rnd() * float64(ceil))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
