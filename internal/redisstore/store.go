// Package redisstore хранит в Redis кэш, refresh-токены и счётчики запросов.
package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store — обёртка над клиентом Redis. Ключи разделены префиксами: cache:, refresh:, rate:.
type Store struct {
	rdb *redis.Client
}

// Connect подключается к Redis по адресу вида redis://host:6379/0 и проверяет, что он отвечает.
func Connect(ctx context.Context, url string) (*Store, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	// Кэш не должен подвешивать запросы: если Redis не отвечает за секунду, сервис идёт в базу.
	opts.DialTimeout, opts.ReadTimeout, opts.WriteTimeout = time.Second, time.Second, time.Second
	opts.MaxRetries, opts.DialerRetries = 1, 1
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close() //nolint:errcheck // возвращаем ошибку подключения, она важнее
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Store{rdb: rdb}, nil
}

// Close закрывает соединения.
func (s *Store) Close() error { return s.rdb.Close() }

// Ping проверяет, что Redis отвечает.
func (s *Store) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }

// Get читает значение из кэша. Второй результат — найден ли ключ.
func (s *Store) Get(ctx context.Context, key string) ([]byte, bool, error) {
	v, err := s.rdb.Get(ctx, "cache:"+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("cache get: %w", err)
	}
	return v, true, nil
}

// Set кладёт значение в кэш на время ttl.
func (s *Store) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := s.rdb.Set(ctx, "cache:"+key, value, ttl).Err(); err != nil {
		return fmt.Errorf("cache set: %w", err)
	}
	return nil
}

// Del удаляет ключи из кэша.
func (s *Store) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = "cache:" + k
	}
	if err := s.rdb.Del(ctx, full...).Err(); err != nil {
		return fmt.Errorf("cache del: %w", err)
	}
	return nil
}

// SaveRefresh запоминает refresh-токен (его хеш) пользователя на время ttl.
func (s *Store) SaveRefresh(ctx context.Context, tokenHash string, userID int64, ttl time.Duration) error {
	if err := s.rdb.Set(ctx, "refresh:"+tokenHash, userID, ttl).Err(); err != nil {
		return fmt.Errorf("save refresh token: %w", err)
	}
	return nil
}

// TakeRefresh возвращает владельца токена и сразу удаляет токен.
// GETDEL делает это одной командой: два одновременных запроса с одним токеном не пройдут оба.
func (s *Store) TakeRefresh(ctx context.Context, tokenHash string) (int64, bool, error) {
	v, err := s.rdb.GetDel(ctx, "refresh:"+tokenHash).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("take refresh token: %w", err)
	}
	id, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("take refresh token: %w", err)
	}
	return id, true, nil
}

// DeleteRefresh отзывает refresh-токен.
func (s *Store) DeleteRefresh(ctx context.Context, tokenHash string) error {
	if err := s.rdb.Del(ctx, "refresh:"+tokenHash).Err(); err != nil {
		return fmt.Errorf("delete refresh token: %w", err)
	}
	return nil
}

// Allow считает обращения по ключу и разрешает не больше limit за окно window.
// Счётчик и срок жизни ставятся в одной транзакции: ключ без срока не останется.
func (s *Store) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	k := "rate:" + key
	var count *redis.IntCmd
	_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		count = p.Incr(ctx, k)
		p.ExpireNX(ctx, k, window) // срок ставится только первому запросу окна и потом не продлевается
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("rate limit: %w", err)
	}
	return count.Val() <= int64(limit), nil
}
