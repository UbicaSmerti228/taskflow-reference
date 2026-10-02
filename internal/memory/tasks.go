// Package memory содержит хранилища в памяти. На них работают юнит-тесты сервисов и HTTP-слоя:
// база и Redis для таких тестов не нужны. Поведение совпадает с настоящими хранилищами —
// это проверяют общие наборы тестов из пакета storetest.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

type ownedTask struct {
	task.Task
	userID int64
}

// Tasks хранит задачи в памяти.
type Tasks struct {
	mu     sync.Mutex
	tasks  []ownedTask
	lastID int64
	outbox *Outbox

	Err error // если задано, все методы возвращают эту ошибку
}

// NewTasks возвращает пустое хранилище задач.
func NewTasks() *Tasks { return &Tasks{outbox: &Outbox{}} }

// Outbox возвращает события, которые хранилище записало вместе с изменениями задач.
func (s *Tasks) Outbox() *Outbox { return s.outbox }

// Create создаёт задачу пользователя.
func (s *Tasks) Create(_ context.Context, userID int64, in task.NewTask) (task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return task.Task{}, s.Err
	}
	s.lastID++
	t := task.Task{ID: s.lastID, Title: in.Title, DueAt: in.DueAt, CreatedAt: time.Now().UTC()}
	if err := s.outbox.add(event.TaskCreated(userID, t)); err != nil {
		return task.Task{}, err
	}
	s.tasks = append(s.tasks, ownedTask{Task: t, userID: userID})
	return t, nil
}

// Get возвращает задачу пользователя или task.ErrNotFound. Чужая задача тоже «не найдена».
func (s *Tasks) Get(_ context.Context, userID, id int64) (task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return task.Task{}, s.Err
	}
	if i := s.index(userID, id); i >= 0 {
		return s.tasks[i].Task, nil
	}
	return task.Task{}, fmt.Errorf("get task %d: %w", id, task.ErrNotFound)
}

// List возвращает страницу задач пользователя и число задач, подходящих под фильтр.
func (s *Tasks) List(_ context.Context, userID int64, f task.Filter) ([]task.Task, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, 0, s.Err
	}
	total := 0
	page := []task.Task{}
	for _, t := range s.tasks {
		if t.userID != userID || (f.Done != nil && t.Done != *f.Done) {
			continue
		}
		total++
		if t.ID > f.AfterID {
			page = append(page, t.Task)
		}
	}
	start := min(max(f.Offset, 0), len(page))
	end := len(page)
	if f.Limit > 0 {
		end = min(start+f.Limit, len(page))
	}
	return page[start:end], total, nil
}

// Update применяет патч к задаче пользователя.
func (s *Tasks) Update(_ context.Context, userID, id int64, p task.Patch) (task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return task.Task{}, s.Err
	}
	i := s.index(userID, id)
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
	return s.tasks[i].Task, nil
}

// Delete удаляет задачу пользователя.
func (s *Tasks) Delete(_ context.Context, userID, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	i := s.index(userID, id)
	if i < 0 {
		return fmt.Errorf("delete task %d: %w", id, task.ErrNotFound)
	}
	s.tasks = append(s.tasks[:i], s.tasks[i+1:]...)
	return nil
}

// DueForReminder возвращает задачи всех пользователей, о которых пора напомнить.
func (s *Tasks) DueForReminder(_ context.Context, now time.Time) ([]task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, s.Err
	}
	var due []task.Task
	for _, t := range s.tasks {
		if !t.Done && t.RemindedAt == nil && t.DueAt != nil && !t.DueAt.After(now) {
			due = append(due, t.Task)
		}
	}
	return due, nil
}

// MarkReminded запоминает, что напоминание отправлено, и записывает событие «срок наступил».
func (s *Tasks) MarkReminded(_ context.Context, id int64, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	for i := range s.tasks {
		if s.tasks[i].ID == id {
			at = at.UTC()
			if err := s.outbox.add(event.TaskDue(s.tasks[i].userID, s.tasks[i].Task, at)); err != nil {
				return err
			}
			s.tasks[i].RemindedAt = &at
			return nil
		}
	}
	return fmt.Errorf("mark task %d reminded: %w", id, task.ErrNotFound)
}

func (s *Tasks) index(userID, id int64) int {
	for i := range s.tasks {
		if s.tasks[i].ID == id && s.tasks[i].userID == userID {
			return i
		}
	}
	return -1
}
