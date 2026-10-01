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

	mu sync.Mutex // HTTP-сервер вызывает методы из разных горутин
}

// NewFileStore возвращает хранилище в файле path. Файл создаётся при первой записи.
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path, now: time.Now}
}

// Add создаёт задачу с заголовком title.
func (s *FileStore) Add(title string) (Task, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return Task{}, ErrEmptyTitle
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tasks, err := s.load()
	if err != nil {
		return Task{}, fmt.Errorf("add task: %w", err)
	}
	t := Task{ID: nextID(tasks), Title: title, CreatedAt: s.now().UTC()}
	if err := s.save(append(tasks, t)); err != nil {
		return Task{}, fmt.Errorf("add task: %w", err)
	}
	return t, nil
}

// List возвращает все задачи в порядке создания.
func (s *FileStore) List() ([]Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks, err := s.load()
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	return tasks, nil
}

// Done отмечает задачу выполненной. Для неизвестного id возвращает ErrNotFound.
func (s *FileStore) Done(id int) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks, err := s.load()
	if err != nil {
		return Task{}, fmt.Errorf("done task %d: %w", id, err)
	}
	for i := range tasks {
		if tasks[i].ID != id {
			continue
		}
		tasks[i].Done = true
		if err := s.save(tasks); err != nil {
			return Task{}, fmt.Errorf("done task %d: %w", id, err)
		}
		return tasks[i], nil
	}
	return Task{}, fmt.Errorf("done task %d: %w", id, ErrNotFound)
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
