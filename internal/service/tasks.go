// Package service содержит бизнес-логику. Сервисы не знают ни про HTTP, ни про SQL:
// хранилища приходят к ним через интерфейсы, объявленные здесь же.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// TaskRepo — что сервису задач нужно от хранилища.
type TaskRepo interface {
	Create(ctx context.Context, userID int64, in task.NewTask) (task.Task, error)
	Get(ctx context.Context, userID, id int64) (task.Task, error)
	List(ctx context.Context, userID int64, f task.Filter) ([]task.Task, int, error)
	Update(ctx context.Context, userID, id int64, p task.Patch) (task.Task, error)
	Delete(ctx context.Context, userID, id int64) error
}

// Cache — кэш с временем жизни ключей.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) error
}

// Tasks — сервис задач: проверяет входные данные и кэширует чтение одной задачи.
type Tasks struct {
	repo  TaskRepo
	cache Cache
	ttl   time.Duration
	log   *slog.Logger
}

// NewTasks собирает сервис. ttl — сколько задача живёт в кэше.
func NewTasks(repo TaskRepo, cache Cache, ttl time.Duration, log *slog.Logger) *Tasks {
	return &Tasks{repo: repo, cache: cache, ttl: ttl, log: log}
}

// В ключе есть владелец: один пользователь не получит из кэша задачу другого.
func taskKey(userID, id int64) string { return fmt.Sprintf("task:%d:%d", userID, id) }

// Create проверяет данные и создаёт задачу.
func (s *Tasks) Create(ctx context.Context, userID int64, in task.NewTask) (task.Task, error) {
	in, err := in.Normalize()
	if err != nil {
		return task.Task{}, err
	}
	return s.repo.Create(ctx, userID, in)
}

// Get возвращает задачу: сначала из кэша, при промахе — из базы с записью в кэш (cache-aside).
// Недоступный кэш не ломает запрос: сервис пишет предупреждение и идёт в базу.
func (s *Tasks) Get(ctx context.Context, userID, id int64) (task.Task, error) {
	key := taskKey(userID, id)

	if raw, ok, err := s.cache.Get(ctx, key); err != nil {
		s.log.WarnContext(ctx, "cache get failed", "err", err)
	} else if ok {
		var t task.Task
		if err := json.Unmarshal(raw, &t); err == nil {
			return t, nil
		}
		s.log.WarnContext(ctx, "cache holds broken value", "key", key)
	}

	t, err := s.repo.Get(ctx, userID, id)
	if err != nil {
		return task.Task{}, err
	}
	if raw, err := json.Marshal(t); err == nil {
		if err := s.cache.Set(ctx, key, raw, s.ttl); err != nil {
			s.log.WarnContext(ctx, "cache set failed", "err", err)
		}
	}
	return t, nil
}

// List возвращает страницу задач. Списки не кэшируются: фильтров много, а инвалидация сложная.
func (s *Tasks) List(ctx context.Context, userID int64, f task.Filter) ([]task.Task, int, error) {
	return s.repo.List(ctx, userID, f)
}

// Update проверяет патч, меняет задачу и удаляет её из кэша.
func (s *Tasks) Update(ctx context.Context, userID, id int64, p task.Patch) (task.Task, error) {
	p, err := p.Normalize()
	if err != nil {
		return task.Task{}, err
	}
	t, err := s.repo.Update(ctx, userID, id, p)
	if err != nil {
		return task.Task{}, err
	}
	s.invalidate(ctx, userID, id)
	return t, nil
}

// Delete удаляет задачу и запись о ней в кэше.
func (s *Tasks) Delete(ctx context.Context, userID, id int64) error {
	if err := s.repo.Delete(ctx, userID, id); err != nil {
		return err
	}
	s.invalidate(ctx, userID, id)
	return nil
}

// invalidate удаляет ключ, а не перезаписывает: две параллельные записи не оставят в кэше старое значение.
// Если удалить не вышло, устаревшие данные проживут не дольше ttl.
func (s *Tasks) invalidate(ctx context.Context, userID, id int64) {
	if err := s.cache.Del(ctx, taskKey(userID, id)); err != nil {
		s.log.WarnContext(ctx, "cache invalidation failed", "err", err)
	}
}
