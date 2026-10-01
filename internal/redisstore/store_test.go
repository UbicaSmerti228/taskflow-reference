package redisstore

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/UbicaSmerti228/taskflow-reference/internal/storetest"
)

// miniredis — Redis в памяти процесса: тесты не требуют Docker и умеют переводить часы.
func newStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	s, err := Connect(context.Background(), "redis://"+mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() }) //nolint:errcheck // тест завершается
	return s, mr
}

func TestStore(t *testing.T) {
	storetest.RunKVSuite(t, func(t *testing.T) (storetest.KV, func(time.Duration)) {
		s, mr := newStore(t)
		return s, mr.FastForward
	})
}

func TestPing(t *testing.T) {
	s, mr := newStore(t)
	if err := s.Ping(context.Background()); err != nil {
		t.Errorf("Ping() error = %v, want nil", err)
	}
	mr.Close()
	if err := s.Ping(context.Background()); err == nil {
		t.Error("Ping() после остановки Redis вернул nil, want ошибку")
	}
}

func TestErrorsWhenRedisIsDown(t *testing.T) {
	s, mr := newStore(t)
	mr.Close()
	ctx := context.Background()

	calls := map[string]func() error{
		"Get":           func() error { _, _, err := s.Get(ctx, "k"); return err },
		"Set":           func() error { return s.Set(ctx, "k", nil, time.Minute) },
		"Del":           func() error { return s.Del(ctx, "k") },
		"SaveRefresh":   func() error { return s.SaveRefresh(ctx, "h", 1, time.Minute) },
		"TakeRefresh":   func() error { _, _, err := s.TakeRefresh(ctx, "h"); return err },
		"DeleteRefresh": func() error { return s.DeleteRefresh(ctx, "h") },
		"Allow":         func() error { _, err := s.Allow(ctx, "k", 1, time.Minute); return err },
	}
	for name, call := range calls {
		if err := call(); err == nil {
			t.Errorf("%s при недоступном Redis вернул nil, want ошибку", name)
		}
	}
}

func TestConnectErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := Connect(ctx, "это не адрес"); err == nil {
		t.Error("Connect() с мусором вместо адреса вернул nil, want ошибку")
	}
	if _, err := Connect(ctx, "redis://127.0.0.1:1"); err == nil {
		t.Error("Connect() к закрытому порту вернул nil, want ошибку")
	}
}
