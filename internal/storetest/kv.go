package storetest

import (
	"context"
	"testing"
	"time"
)

// KV — методы хранилища «ключ → значение»: кэш, refresh-токены и счётчик запросов.
type KV interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) error

	SaveRefresh(ctx context.Context, tokenHash string, userID int64, ttl time.Duration) error
	TakeRefresh(ctx context.Context, tokenHash string) (int64, bool, error)
	DeleteRefresh(ctx context.Context, tokenHash string) error

	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error)
}

// RunKVSuite проверяет поведение хранилища. advance переводит его часы вперёд.
func RunKVSuite(t *testing.T, newKV func(t *testing.T) (kv KV, advance func(time.Duration))) {
	ctx := context.Background()

	t.Run("кэш", func(t *testing.T) {
		kv, advance := newKV(t)

		if _, ok, err := kv.Get(ctx, "task:1"); err != nil || ok {
			t.Fatalf("Get пустого ключа = %v, %v; want не найден", ok, err)
		}
		if err := kv.Set(ctx, "task:1", []byte("значение"), time.Minute); err != nil {
			t.Fatal(err)
		}
		if got, ok, err := kv.Get(ctx, "task:1"); err != nil || !ok || string(got) != "значение" {
			t.Errorf("Get = %q, %v, %v; want сохранённое значение", got, ok, err)
		}

		advance(59 * time.Second)
		if _, ok, _ := kv.Get(ctx, "task:1"); !ok {
			t.Error("ключ пропал раньше срока")
		}
		advance(2 * time.Second)
		if _, ok, _ := kv.Get(ctx, "task:1"); ok {
			t.Error("ключ жив после истечения срока")
		}

		for _, k := range []string{"a", "b", "c"} {
			if err := kv.Set(ctx, k, []byte(k), time.Minute); err != nil {
				t.Fatal(err)
			}
		}
		if err := kv.Del(ctx, "a", "b", "нет такого"); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := kv.Get(ctx, "a"); ok {
			t.Error("ключ a жив после Del")
		}
		if _, ok, _ := kv.Get(ctx, "c"); !ok {
			t.Error("Del удалил лишний ключ c")
		}
		if err := kv.Del(ctx); err != nil {
			t.Errorf("Del без ключей error = %v, want nil", err)
		}
	})

	t.Run("refresh-токены", func(t *testing.T) {
		kv, advance := newKV(t)

		if err := kv.SaveRefresh(ctx, "hash-1", 42, time.Hour); err != nil {
			t.Fatal(err)
		}
		if id, ok, err := kv.TakeRefresh(ctx, "hash-1"); err != nil || !ok || id != 42 {
			t.Fatalf("TakeRefresh = %d, %v, %v; want 42", id, ok, err)
		}
		// Токен одноразовый: второй раз его уже нет.
		if _, ok, err := kv.TakeRefresh(ctx, "hash-1"); err != nil || ok {
			t.Errorf("повторный TakeRefresh = %v, %v; want не найден", ok, err)
		}

		if err := kv.SaveRefresh(ctx, "hash-2", 7, time.Hour); err != nil {
			t.Fatal(err)
		}
		if err := kv.DeleteRefresh(ctx, "hash-2"); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := kv.TakeRefresh(ctx, "hash-2"); ok {
			t.Error("отозванный токен всё ещё работает")
		}

		if err := kv.SaveRefresh(ctx, "hash-3", 7, time.Hour); err != nil {
			t.Fatal(err)
		}
		advance(time.Hour + time.Second)
		if _, ok, _ := kv.TakeRefresh(ctx, "hash-3"); ok {
			t.Error("просроченный токен всё ещё работает")
		}
	})

	t.Run("ограничение частоты", func(t *testing.T) {
		kv, advance := newKV(t)

		for i := 1; i <= 3; i++ {
			if ok, err := kv.Allow(ctx, "login:1.2.3.4", 3, time.Minute); err != nil || !ok {
				t.Fatalf("запрос %d: Allow = %v, %v; want разрешён", i, ok, err)
			}
		}
		if ok, err := kv.Allow(ctx, "login:1.2.3.4", 3, time.Minute); err != nil || ok {
			t.Errorf("четвёртый запрос: Allow = %v, %v; want запрещён", ok, err)
		}
		// У другого ключа свой счётчик.
		if ok, _ := kv.Allow(ctx, "login:5.6.7.8", 3, time.Minute); !ok {
			t.Error("другой ключ заблокирован чужим счётчиком")
		}
		// Отклонённые запросы не продлевают окно.
		advance(30 * time.Second)
		if ok, _ := kv.Allow(ctx, "login:1.2.3.4", 3, time.Minute); ok {
			t.Error("запрос в середине окна разрешён, want запрещён")
		}
		advance(31 * time.Second)
		if ok, _ := kv.Allow(ctx, "login:1.2.3.4", 3, time.Minute); !ok {
			t.Error("после окончания окна запрос запрещён, want разрешён")
		}
	})
}
