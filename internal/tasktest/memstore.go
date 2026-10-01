// Package tasktest содержит хранилище в памяти и общий набор проверок для любых хранилищ задач.
// Юнит-тесты HTTP-слоя и CLI работают с хранилищем в памяти и не требуют базы.
package tasktest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// MemStore хранит задачи в памяти. Ведёт себя так же, как хранилище в PostgreSQL:
// это проверяет общий набор тестов RunStoreSuite.
type MemStore struct {
	mu     sync.Mutex
	tasks  []task.Task
	lastID int64

	Err error // если задано, все методы возвращают эту ошибку
}

// NewMemStore возвращает пустое хранилище.
func NewMemStore() *MemStore { return &MemStore{} }

// Create создаёт задачу.
func (s *MemStore) Create(_ context.Context, in task.NewTask) (task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return task.Task{}, s.Err
	}
	in, err := in.Normalize()
	if err != nil {
		return task.Task{}, err
	}
	s.lastID++
	t := task.Task{ID: s.lastID, Title: in.Title, DueAt: in.DueAt, CreatedAt: time.Now().UTC()}
	s.tasks = append(s.tasks, t)
	return t, nil
}

// Get возвращает задачу по id или task.ErrNotFound.
func (s *MemStore) Get(_ context.Context, id int64) (task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return task.Task{}, s.Err
	}
	if i := s.index(id); i >= 0 {
		return s.tasks[i], nil
	}
	return task.Task{}, fmt.Errorf("get task %d: %w", id, task.ErrNotFound)
}

// List возвращает страницу задач и число задач, подходящих под фильтр.
func (s *MemStore) List(_ context.Context, f task.Filter) ([]task.Task, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, 0, s.Err
	}
	total := 0
	page := []task.Task{}
	for _, t := range s.tasks {
		if f.Done != nil && t.Done != *f.Done {
			continue
		}
		total++
		if t.ID > f.AfterID {
			page = append(page, t)
		}
	}
	start := min(max(f.Offset, 0), len(page))
	end := len(page)
	if f.Limit > 0 {
		end = min(start+f.Limit, len(page))
	}
	return page[start:end], total, nil
}

// Update применяет патч к задаче.
func (s *MemStore) Update(_ context.Context, id int64, p task.Patch) (task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return task.Task{}, s.Err
	}
	p, err := p.Normalize()
	if err != nil {
		return task.Task{}, err
	}
	i := s.index(id)
	if i < 0 {
		return task.Task{}, fmt.Errorf("update task %d: %w", id, task.ErrNotFound)
	}
	if p.Title != nil {
		s.tasks[i].Title = *p.Title
	}
	if p.Done != nil {
		s.tasks[i].Done = *p.Done
	}
	if p.DueAt != nil {
		s.tasks[i].DueAt = p.DueAt
		s.tasks[i].RemindedAt = nil
	}
	return s.tasks[i], nil
}

// Delete удаляет задачу.
func (s *MemStore) Delete(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	i := s.index(id)
	if i < 0 {
		return fmt.Errorf("delete task %d: %w", id, task.ErrNotFound)
	}
	s.tasks = append(s.tasks[:i], s.tasks[i+1:]...)
	return nil
}

// DueForReminder возвращает задачи, о которых пора напомнить.
func (s *MemStore) DueForReminder(_ context.Context, now time.Time) ([]task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, s.Err
	}
	var due []task.Task
	for _, t := range s.tasks {
		if !t.Done && t.RemindedAt == nil && t.DueAt != nil && !t.DueAt.After(now) {
			due = append(due, t)
		}
	}
	return due, nil
}

// MarkReminded запоминает, что напоминание отправлено.
func (s *MemStore) MarkReminded(_ context.Context, id int64, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	i := s.index(id)
	if i < 0 {
		return fmt.Errorf("mark task %d reminded: %w", id, task.ErrNotFound)
	}
	at = at.UTC()
	s.tasks[i].RemindedAt = &at
	return nil
}

func (s *MemStore) index(id int64) int {
	for i := range s.tasks {
		if s.tasks[i].ID == id {
			return i
		}
	}
	return -1
}
