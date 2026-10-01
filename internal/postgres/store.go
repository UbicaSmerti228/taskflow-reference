// Package postgres хранит задачи в PostgreSQL.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// DemoUser — владелец всех задач, пока в проекте нет аутентификации.
const DemoUser = "demo@taskflow.local"

const taskColumns = "id, title, done, due_at, reminded_at, created_at"

// Store — репозиторий задач на pgx. Во всех запросах только плейсхолдеры.
type Store struct {
	pool   *pgxpool.Pool
	userID int64
}

// Connect открывает пул соединений и проверяет, что база отвечает.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// NewStore возвращает репозиторий. Схема к этому моменту должна быть создана миграциями.
func NewStore(ctx context.Context, pool *pgxpool.Pool) (*Store, error) {
	var userID int64
	err := pool.QueryRow(ctx, `SELECT id FROM users WHERE email = $1`, DemoUser).Scan(&userID)
	if err != nil {
		return nil, fmt.Errorf("find demo user: %w", err)
	}
	return &Store{pool: pool, userID: userID}, nil
}

// Create создаёт задачу.
func (s *Store) Create(ctx context.Context, in task.NewTask) (task.Task, error) {
	in, err := in.Normalize()
	if err != nil {
		return task.Task{}, err
	}
	rows, err := s.pool.Query(ctx,
		`INSERT INTO tasks (user_id, title, due_at) VALUES ($1, $2, $3) RETURNING `+taskColumns,
		s.userID, in.Title, in.DueAt)
	if err != nil {
		return task.Task{}, fmt.Errorf("create task: %w", err)
	}
	t, err := pgx.CollectExactlyOneRow(rows, scanTask)
	if err != nil {
		return task.Task{}, fmt.Errorf("create task: %w", err)
	}
	return t, nil
}

// Get возвращает задачу по id или task.ErrNotFound.
func (s *Store) Get(ctx context.Context, id int64) (task.Task, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE id = $1 AND user_id = $2`, id, s.userID)
	if err != nil {
		return task.Task{}, fmt.Errorf("get task %d: %w", id, err)
	}
	t, err := pgx.CollectExactlyOneRow(rows, scanTask)
	if err != nil {
		return task.Task{}, fmt.Errorf("get task %d: %w", id, notFound(err))
	}
	return t, nil
}

// List возвращает страницу задач по возрастанию id и число задач, подходящих под фильтр.
func (s *Store) List(ctx context.Context, f task.Filter) ([]task.Task, int, error) {
	var limit *int // NULL в LIMIT означает «без ограничения»
	if f.Limit > 0 {
		limit = &f.Limit
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+taskColumns+`
		   FROM tasks
		  WHERE user_id = $1
		    AND ($2::boolean IS NULL OR done = $2)
		    AND id > $3
		  ORDER BY id
		  LIMIT $4 OFFSET $5`,
		s.userID, f.Done, f.AfterID, limit, max(f.Offset, 0))
	if err != nil {
		return nil, 0, fmt.Errorf("list tasks: %w", err)
	}
	tasks, err := pgx.CollectRows(rows, scanTask)
	if err != nil {
		return nil, 0, fmt.Errorf("list tasks: %w", err)
	}

	var total int
	err = s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE user_id = $1 AND ($2::boolean IS NULL OR done = $2)`,
		s.userID, f.Done).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("count tasks: %w", err)
	}
	return tasks, total, nil
}

// Update применяет патч к задаче. Новый срок сбрасывает отметку о напоминании.
func (s *Store) Update(ctx context.Context, id int64, p task.Patch) (task.Task, error) {
	p, err := p.Normalize()
	if err != nil {
		return task.Task{}, err
	}
	// COALESCE оставляет прежнее значение там, где в патче nil: запрос один на все сочетания полей.
	rows, err := s.pool.Query(ctx,
		`UPDATE tasks
		    SET title       = COALESCE($3, title),
		        done        = COALESCE($4, done),
		        due_at      = COALESCE($5, due_at),
		        reminded_at = CASE WHEN $5::timestamptz IS NULL THEN reminded_at END
		  WHERE id = $1 AND user_id = $2
		  RETURNING `+taskColumns,
		id, s.userID, p.Title, p.Done, p.DueAt)
	if err != nil {
		return task.Task{}, fmt.Errorf("update task %d: %w", id, err)
	}
	t, err := pgx.CollectExactlyOneRow(rows, scanTask)
	if err != nil {
		return task.Task{}, fmt.Errorf("update task %d: %w", id, notFound(err))
	}
	return t, nil
}

// Delete удаляет задачу. Для неизвестного id возвращает task.ErrNotFound.
func (s *Store) Delete(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM tasks WHERE id = $1 AND user_id = $2`, id, s.userID)
	if err != nil {
		return fmt.Errorf("delete task %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("delete task %d: %w", id, task.ErrNotFound)
	}
	return nil
}

// DueForReminder возвращает невыполненные задачи всех пользователей, срок которых наступил,
// а напоминание ещё не отправлено. За один обход отдаётся не больше 100 задач.
func (s *Store) DueForReminder(ctx context.Context, now time.Time) ([]task.Task, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+taskColumns+`
		   FROM tasks
		  WHERE NOT done AND reminded_at IS NULL AND due_at <= $1
		  ORDER BY due_at
		  LIMIT 100`, now)
	if err != nil {
		return nil, fmt.Errorf("due tasks: %w", err)
	}
	tasks, err := pgx.CollectRows(rows, scanTask)
	if err != nil {
		return nil, fmt.Errorf("due tasks: %w", err)
	}
	return tasks, nil
}

// MarkReminded запоминает, что напоминание по задаче отправлено.
func (s *Store) MarkReminded(ctx context.Context, id int64, at time.Time) error {
	tag, err := s.pool.Exec(ctx, `UPDATE tasks SET reminded_at = $2 WHERE id = $1`, id, at)
	if err != nil {
		return fmt.Errorf("mark task %d reminded: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark task %d reminded: %w", id, task.ErrNotFound)
	}
	return nil
}

func scanTask(row pgx.CollectableRow) (task.Task, error) {
	var t task.Task
	if err := row.Scan(&t.ID, &t.Title, &t.Done, &t.DueAt, &t.RemindedAt, &t.CreatedAt); err != nil {
		return task.Task{}, err
	}
	// pgx отдаёт время в часовом поясе процесса. Наружу отдаём UTC, чтобы ответ API не зависел от сервера.
	t.CreatedAt = t.CreatedAt.UTC()
	t.DueAt, t.RemindedAt = utc(t.DueAt), utc(t.RemindedAt)
	return t, nil
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// notFound переводит ошибку драйвера в ошибку предметной области:
// код выше репозитория не должен знать про pgx.
func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return task.ErrNotFound
	}
	return err
}
