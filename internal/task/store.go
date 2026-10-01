package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// FileStore хранит задачи в одном JSON-файле.
// Каждая операция читает файл целиком и целиком его перезаписывает:
// для учебного трекера этого хватает, на этапе 3 хранилище переедет в PostgreSQL.
type FileStore struct {
	path string
	now  func() time.Time

	mu sync.Mutex // HTTP-сервер и воркер вызывают методы из разных горутин
}

// NewFileStore возвращает хранилище в файле path. Файл создаётся при первой записи.
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path, now: time.Now}
}

// Create создаёт задачу.
func (s *FileStore) Create(in NewTask) (Task, error) {
	title := strings.TrimSpace(in.Title)
	if title == "" {
		return Task{}, ErrEmptyTitle
	}

	var created Task
	err := s.change(func(tasks []Task) ([]Task, error) {
		created = Task{ID: nextID(tasks), Title: title, DueAt: in.DueAt, CreatedAt: s.now().UTC()}
		return append(tasks, created), nil
	})
	if err != nil {
		return Task{}, fmt.Errorf("create task: %w", err)
	}
	return created, nil
}

// Get возвращает задачу по id или ErrNotFound.
func (s *FileStore) Get(id int) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks, err := s.load()
	if err != nil {
		return Task{}, fmt.Errorf("get task %d: %w", id, err)
	}
	for _, t := range tasks {
		if t.ID == id {
			return t, nil
		}
	}
	return Task{}, fmt.Errorf("get task %d: %w", id, ErrNotFound)
}

// List возвращает страницу задач в порядке создания и общее число задач, подходящих под фильтр.
func (s *FileStore) List(f Filter) ([]Task, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks, err := s.load()
	if err != nil {
		return nil, 0, fmt.Errorf("list tasks: %w", err)
	}

	matched := make([]Task, 0, len(tasks))
	for _, t := range tasks {
		if f.Done == nil || t.Done == *f.Done {
			matched = append(matched, t)
		}
	}
	total := len(matched)

	start := min(max(f.Offset, 0), total)
	end := total
	if f.Limit > 0 {
		end = min(start+f.Limit, total)
	}
	return matched[start:end], total, nil
}

// Update применяет патч к задаче. Новый срок сбрасывает отметку о напоминании.
func (s *FileStore) Update(id int, p Patch) (Task, error) {
	if p.Empty() {
		return Task{}, ErrEmptyPatch
	}
	if p.Title != nil {
		title := strings.TrimSpace(*p.Title)
		if title == "" {
			return Task{}, ErrEmptyTitle
		}
		p.Title = &title
	}

	var updated Task
	err := s.change(func(tasks []Task) ([]Task, error) {
		for i := range tasks {
			if tasks[i].ID != id {
				continue
			}
			if p.Title != nil {
				tasks[i].Title = *p.Title
			}
			if p.Done != nil {
				tasks[i].Done = *p.Done
			}
			if p.DueAt != nil {
				tasks[i].DueAt = p.DueAt
				tasks[i].RemindedAt = nil
			}
			updated = tasks[i]
			return tasks, nil
		}
		return nil, ErrNotFound
	})
	if err != nil {
		return Task{}, fmt.Errorf("update task %d: %w", id, err)
	}
	return updated, nil
}

// Delete удаляет задачу. Для неизвестного id возвращает ErrNotFound.
func (s *FileStore) Delete(id int) error {
	err := s.change(func(tasks []Task) ([]Task, error) {
		for i := range tasks {
			if tasks[i].ID == id {
				return append(tasks[:i], tasks[i+1:]...), nil
			}
		}
		return nil, ErrNotFound
	})
	if err != nil {
		return fmt.Errorf("delete task %d: %w", id, err)
	}
	return nil
}

// DueForReminder возвращает невыполненные задачи, срок которых наступил, а напоминание ещё не отправлено.
func (s *FileStore) DueForReminder(now time.Time) ([]Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks, err := s.load()
	if err != nil {
		return nil, fmt.Errorf("due tasks: %w", err)
	}
	var due []Task
	for _, t := range tasks {
		if !t.Done && t.RemindedAt == nil && t.DueAt != nil && !t.DueAt.After(now) {
			due = append(due, t)
		}
	}
	return due, nil
}

// MarkReminded запоминает, что напоминание по задаче отправлено.
func (s *FileStore) MarkReminded(id int, at time.Time) error {
	err := s.change(func(tasks []Task) ([]Task, error) {
		for i := range tasks {
			if tasks[i].ID == id {
				at = at.UTC()
				tasks[i].RemindedAt = &at
				return tasks, nil
			}
		}
		return nil, ErrNotFound
	})
	if err != nil {
		return fmt.Errorf("mark task %d reminded: %w", id, err)
	}
	return nil
}

// change читает задачи, даёт функции их изменить и сохраняет результат.
// Всё под одним захватом мьютекса: между чтением и записью никто не вклинится.
func (s *FileStore) change(fn func([]Task) ([]Task, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks, err := s.load()
	if err != nil {
		return err
	}
	tasks, err = fn(tasks)
	if err != nil {
		return err
	}
	return s.save(tasks)
}

func nextID(tasks []Task) int {
	id := 0
	for _, t := range tasks {
		id = max(id, t.ID)
	}
	return id + 1
}

func (s *FileStore) load() ([]Task, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return []Task{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", s.path, err)
	}
	tasks := []Task{}
	if err := json.Unmarshal(data, &tasks); err != nil {
		return nil, fmt.Errorf("parse %s: %w", s.path, err)
	}
	return tasks, nil
}

// save пишет во временный файл и переименовывает его:
// если программа упадёт посреди записи, старый файл останется целым.
func (s *FileStore) save(tasks []Task) error {
	data, err := json.MarshalIndent(tasks, "", "  ")
	if err != nil {
		return fmt.Errorf("encode tasks: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".tasks-*.json")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmp.Name()) //nolint:errcheck // после успешного Rename файла уже нет

	if _, err := tmp.Write(data); err != nil {
		tmp.Close() //nolint:errcheck // возвращаем ошибку записи, она важнее
		return fmt.Errorf("write %s: %w", tmp.Name(), err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp.Name(), err)
	}
	if err := os.Rename(tmp.Name(), s.path); err != nil {
		return fmt.Errorf("replace %s: %w", s.path, err)
	}
	return nil
}
