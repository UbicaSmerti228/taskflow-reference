package resilience

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// recorder заменяет ожидание: паузы записываются, время не тратится.
type recorder struct{ slept []time.Duration }

func (r *recorder) sleep(_ context.Context, d time.Duration) error {
	r.slept = append(r.slept, d)
	return nil
}

func TestRetryStopsOnSuccess(t *testing.T) {
	rec := &recorder{}
	calls := 0
	err := Retry{Attempts: 5, Base: time.Second, Sleep: rec.sleep}.Do(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return errBoom
		}
		return nil
	})
	if err != nil || calls != 3 || len(rec.slept) != 2 {
		t.Errorf("err = %v, вызовов %d, пауз %d; want nil, 3, 2", err, calls, len(rec.slept))
	}
}

func TestRetryGivesUp(t *testing.T) {
	tests := []struct {
		name      string
		attempts  int
		wantCalls int
	}{
		{name: "три попытки", attempts: 3, wantCalls: 3},
		{name: "нулевое значение — одна попытка", attempts: 0, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			calls := 0
			err := Retry{Attempts: tt.attempts, Base: time.Second, Sleep: rec.sleep}.Do(context.Background(), func(context.Context) error {
				calls++
				return errBoom
			})
			if !errors.Is(err, errBoom) || calls != tt.wantCalls {
				t.Errorf("err = %v, вызовов %d; want boom, %d", err, calls, tt.wantCalls)
			}
		})
	}
}

func TestRetryPermanentIsNotRetried(t *testing.T) {
	calls := 0
	err := Retry{Attempts: 5, Sleep: (&recorder{}).sleep}.Do(context.Background(), func(context.Context) error {
		calls++
		return Permanent(errBoom)
	})
	if !errors.Is(err, errBoom) || calls != 1 {
		t.Errorf("err = %v, вызовов %d; want boom, 1", err, calls)
	}
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) != nil")
	}
}

// Паузы растут вдвое и упираются в потолок. Rand = 1 даёт верхнюю границу, Rand = 0.5 — её половину.
func TestRetryBackoff(t *testing.T) {
	tests := []struct {
		name string
		rand float64
		want []time.Duration
	}{
		{name: "верхняя граница", rand: 1, want: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 500 * time.Millisecond, 500 * time.Millisecond}},
		{name: "случайная доля", rand: 0.5, want: []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 250 * time.Millisecond, 250 * time.Millisecond}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			r := Retry{Attempts: 6, Base: 100 * time.Millisecond, Max: 500 * time.Millisecond, Rand: func() float64 { return tt.rand }, Sleep: rec.sleep}
			_ = r.Do(context.Background(), func(context.Context) error { return errBoom })
			if len(rec.slept) != len(tt.want) {
				t.Fatalf("пауз %d, want %d", len(rec.slept), len(tt.want))
			}
			for i, want := range tt.want {
				if rec.slept[i] != want {
					t.Errorf("пауза %d = %v, want %v", i+1, rec.slept[i], want)
				}
			}
		})
	}
}

// Настоящие случайные числа: пауза всегда в пределах от нуля до потолка.
func TestRetryJitterRange(t *testing.T) {
	r := Retry{Base: 100 * time.Millisecond, Max: time.Second}
	for range 1000 {
		if d := r.backoff(3); d < 0 || d > 400*time.Millisecond {
			t.Fatalf("backoff(3) = %v, want от 0 до 400ms", d)
		}
	}
}

func TestRetryStopsWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	// Пауза настоящая и длинная: тест пройдёт быстро, только если отмена её прерывает.
	err := Retry{Attempts: 5, Base: time.Hour, Rand: func() float64 { return 1 }}.Do(ctx, func(context.Context) error {
		calls++
		cancel()
		return errBoom
	})
	if !errors.Is(err, errBoom) || calls != 1 {
		t.Errorf("err = %v, вызовов %d; want boom, 1", err, calls)
	}

	ctx, cancel = context.WithCancel(context.Background())
	// Отмена приходит уже во время паузы: sleeping закрывается, когда пауза началась.
	sleeping := make(chan struct{})
	slowSleep := func(ctx context.Context, d time.Duration) error {
		close(sleeping)
		return sleepCtx(ctx, d)
	}
	done := make(chan error, 1)
	go func() {
		done <- Retry{Attempts: 5, Base: time.Hour, Sleep: slowSleep}.Do(ctx, func(context.Context) error { return errBoom })
	}()
	<-sleeping
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, errBoom) || !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want и boom, и context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("отмена контекста не прервала паузу")
	}
}

// clock — часы, которые двигает тест.
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newTestBreaker(threshold int, cooldown time.Duration) (*Breaker, *clock) {
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	b := NewBreaker(threshold, cooldown)
	b.now = c.now
	return b, c
}

func fail() error { return errBoom }
func ok() error   { return nil }

func TestBreakerOpensAfterThreshold(t *testing.T) {
	b, _ := newTestBreaker(3, time.Minute)
	for i := range 3 {
		if err := b.Do(fail); !errors.Is(err, errBoom) {
			t.Fatalf("вызов %d: err = %v, want boom", i+1, err)
		}
	}
	if b.State() != StateOpen {
		t.Fatalf("состояние %s, want open", b.State())
	}
	called := false
	err := b.Do(func() error { called = true; return nil })
	if !errors.Is(err, ErrOpen) || called {
		t.Errorf("разомкнутый breaker: err = %v, функция вызвана: %v; want ErrOpen, false", err, called)
	}
}

// Успех обнуляет счётчик: считаются только отказы подряд.
func TestBreakerCountsConsecutiveFailures(t *testing.T) {
	b, _ := newTestBreaker(3, time.Minute)
	for range 5 {
		_ = b.Do(fail)
		_ = b.Do(fail)
		_ = b.Do(ok)
	}
	if b.State() != StateClosed {
		t.Errorf("состояние %s, want closed", b.State())
	}
}

func TestBreakerRecovers(t *testing.T) {
	b, c := newTestBreaker(1, time.Minute)
	_ = b.Do(fail)

	c.t = c.t.Add(59 * time.Second)
	if err := b.Do(ok); !errors.Is(err, ErrOpen) {
		t.Fatalf("до конца паузы: err = %v, want ErrOpen", err)
	}

	c.t = c.t.Add(time.Second)
	if b.State() != StateHalfOpen {
		t.Fatalf("после паузы состояние %s, want half-open", b.State())
	}
	// Пробный вызов не удался: breaker разомкнут ещё на одну паузу.
	if err := b.Do(fail); !errors.Is(err, errBoom) {
		t.Fatalf("пробный вызов: err = %v, want boom", err)
	}
	if b.State() != StateOpen {
		t.Fatalf("после неудачной пробы состояние %s, want open", b.State())
	}

	c.t = c.t.Add(time.Minute)
	if err := b.Do(ok); err != nil {
		t.Fatalf("вторая проба: err = %v", err)
	}
	if b.State() != StateClosed {
		t.Errorf("после удачной пробы состояние %s, want closed", b.State())
	}
}

// В полуоткрытом состоянии проходит один вызов, остальные отклоняются, пока он не закончится.
func TestBreakerHalfOpenAllowsSingleProbe(t *testing.T) {
	b, c := newTestBreaker(1, time.Minute)
	_ = b.Do(fail)
	c.t = c.t.Add(time.Minute)

	started, release := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = b.Do(func() error {
			close(started)
			<-release
			return nil
		})
	})
	<-started
	if err := b.Do(ok); !errors.Is(err, ErrOpen) {
		t.Errorf("второй вызов во время пробы: err = %v, want ErrOpen", err)
	}
	close(release)
	wg.Wait()
	if b.State() != StateClosed {
		t.Errorf("состояние %s, want closed", b.State())
	}
}

func TestBreakerConcurrent(t *testing.T) {
	b := NewBreaker(0, 0) // некорректные значения заменяются на рабочие
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			if i%2 == 0 {
				_ = b.Do(fail)
			} else {
				_ = b.Do(ok)
			}
			_ = b.State()
		})
	}
	wg.Wait()
	// Главную проверку делает детектор гонок; здесь — что состояние осталось осмысленным.
	if st := b.State(); st != StateClosed && st != StateOpen && st != StateHalfOpen {
		t.Errorf("состояние %q", st)
	}
}
