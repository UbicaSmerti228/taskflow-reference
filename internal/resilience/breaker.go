package resilience

import (
	"errors"
	"sync"
	"time"
)

// ErrOpen возвращается, когда breaker разомкнут и вызов не выполнялся.
var ErrOpen = errors.New("circuit breaker is open")

// Состояния breaker.
const (
	StateClosed   = "closed"    // вызовы проходят
	StateOpen     = "open"      // вызовы отклоняются сразу
	StateHalfOpen = "half-open" // проходит один пробный вызов
)

// Breaker перестаёт вызывать зависимость, которая отказывает подряд: вызовы отклоняются мгновенно,
// а не ждут таймаута. Это бережёт и свои горутины, и сам упавший сервис.
//
// После threshold отказов подряд breaker размыкается. Через cooldown он пропускает один пробный вызов:
// успех замыкает его обратно, отказ размыкает ещё на cooldown.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	mu       sync.Mutex
	failures int       // отказов подряд в замкнутом состоянии
	openedAt time.Time // ненулевое — breaker разомкнут с этого момента
	probing  bool      // пробный вызов уже идёт
}

// NewBreaker собирает breaker. Некорректные значения заменяются на минимальные рабочие.
func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{threshold: max(threshold, 1), cooldown: max(cooldown, time.Millisecond), now: time.Now}
}

// State возвращает текущее состояние — для логов и метрик.
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state()
}

func (b *Breaker) state() string {
	switch {
	case b.openedAt.IsZero():
		return StateClosed
	case b.now().Sub(b.openedAt) < b.cooldown:
		return StateOpen
	default:
		return StateHalfOpen
	}
}

// Do вызывает fn, если breaker это разрешает, и учитывает результат. Иначе возвращает ErrOpen.
func (b *Breaker) Do(fn func() error) error {
	if !b.allow() {
		return ErrOpen
	}
	err := fn()
	b.record(err == nil)
	return err
}

func (b *Breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state() {
	case StateClosed:
		return true
	case StateHalfOpen:
		if b.probing {
			return false // пробный вызов один: остальные ждут его результата
		}
		b.probing = true
		return true
	default:
		return false
	}
}

func (b *Breaker) record(ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ok {
		b.failures, b.openedAt, b.probing = 0, time.Time{}, false
		return
	}
	if b.probing {
		// Пробный вызов не удался: размыкаемся заново.
		b.probing, b.openedAt = false, b.now()
		return
	}
	if !b.openedAt.IsZero() {
		return // вызов начался до размыкания: его отказ уже ничего не меняет
	}
	b.failures++
	if b.failures >= b.threshold {
		b.openedAt = b.now()
	}
}
