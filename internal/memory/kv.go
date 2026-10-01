package memory

import (
	"context"
	"sync"
	"time"
)

type entry struct {
	value   []byte
	expires time.Time
}

// KV — хранилище «ключ → значение» со сроком жизни: замена Redis в юнит-тестах.
// Реализует кэш, хранилище refresh-токенов и счётчик для ограничения частоты запросов.
type KV struct {
	mu   sync.Mutex
	data map[string]entry
	now  time.Time

	Err error // если задано, все методы возвращают эту ошибку
}

// NewKV возвращает пустое хранилище.
func NewKV() *KV {
	return &KV{data: map[string]entry{}, now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

// Advance переводит часы хранилища вперёд: так тесты проверяют истечение срока без ожидания.
func (s *KV) Advance(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = s.now.Add(d)
}

// Len возвращает число живых ключей.
func (s *KV) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k := range s.data {
		if _, ok := s.get(k); ok {
			n++
		}
	}
	return n
}

func (s *KV) get(key string) ([]byte, bool) {
	e, ok := s.data[key]
	if !ok || !s.now.Before(e.expires) {
		delete(s.data, key)
		return nil, false
	}
	return e.value, true
}

// Get читает значение из кэша. Второй результат — найден ли ключ.
func (s *KV) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, false, s.Err
	}
	v, ok := s.get("cache:" + key)
	return v, ok, nil
}

// Set кладёт значение в кэш на время ttl.
func (s *KV) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	s.data["cache:"+key] = entry{value: value, expires: s.now.Add(ttl)}
	return nil
}

// Del удаляет ключи из кэша.
func (s *KV) Del(_ context.Context, keys ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	for _, k := range keys {
		delete(s.data, "cache:"+k)
	}
	return nil
}

// SaveRefresh запоминает refresh-токен (его хеш) пользователя на время ttl.
func (s *KV) SaveRefresh(_ context.Context, tokenHash string, userID int64, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	s.data["refresh:"+tokenHash] = entry{value: encodeID(userID), expires: s.now.Add(ttl)}
	return nil
}

// TakeRefresh возвращает владельца токена и сразу удаляет токен: использовать его можно один раз.
func (s *KV) TakeRefresh(_ context.Context, tokenHash string) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return 0, false, s.Err
	}
	v, ok := s.get("refresh:" + tokenHash)
	if !ok {
		return 0, false, nil
	}
	delete(s.data, "refresh:"+tokenHash)
	return decodeID(v), true, nil
}

// DeleteRefresh отзывает refresh-токен.
func (s *KV) DeleteRefresh(_ context.Context, tokenHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	delete(s.data, "refresh:"+tokenHash)
	return nil
}

// Allow считает обращения по ключу и разрешает не больше limit за окно window.
func (s *KV) Allow(_ context.Context, key string, limit int, window time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return false, s.Err
	}
	k := "rate:" + key
	v, ok := s.get(k)
	if !ok {
		s.data[k] = entry{value: encodeID(1), expires: s.now.Add(window)}
		return limit >= 1, nil
	}
	n := decodeID(v) + 1
	s.data[k] = entry{value: encodeID(n), expires: s.data[k].expires} // окно не продлевается
	return n <= int64(limit), nil
}

func encodeID(n int64) []byte {
	b := make([]byte, 8)
	for i := range b {
		b[i] = byte(n >> (8 * i))
	}
	return b
}

func decodeID(b []byte) int64 {
	var n int64
	for i := range b {
		n |= int64(b[i]) << (8 * i)
	}
	return n
}
