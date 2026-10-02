// Package pgstore хранит уведомления сервиса notifier в PostgreSQL.
package pgstore

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
	"github.com/UbicaSmerti228/taskflow-reference/internal/postgres"
)

// Schema — схема PostgreSQL, в которой лежат таблицы notifier и его журнал миграций.
const Schema = "notifier"

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrate создаёт схему notifier, если её нет, и накатывает в неё новые миграции.
func Migrate(ctx context.Context, dsn string) error {
	files, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	return postgres.MigrateSchema(ctx, dsn, Schema, files)
}

// Connect открывает пул соединений, которые работают в схеме notifier.
func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	return postgres.ConnectSchema(ctx, dsn, Schema)
}

// Store — хранилище уведомлений.
type Store struct {
	pool *pgxpool.Pool
}

// New возвращает хранилище уведомлений.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Save сохраняет уведомление, если событие eventID ещё не обрабатывалось.
// Для уже обработанного события возвращает saved = false и ничего не меняет.
//
// Отметка об обработке и уведомление пишутся одной транзакцией: не бывает ни отметки без уведомления,
// ни двух уведомлений по одному событию. Это и делает обработчик идемпотентным.
func (s *Store) Save(ctx context.Context, eventID string, n notification.Notification) (notification.Notification, bool, error) {
	saved := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // событие уже обработано: дубликат
		}
		err = tx.QueryRow(ctx,
			`INSERT INTO notifications (user_id, kind, task_id, title, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			n.UserID, n.Kind, n.TaskID, n.Title, n.CreatedAt).Scan(&n.ID)
		if err != nil {
			return err
		}
		saved = true
		return nil
	})
	if err != nil {
		return notification.Notification{}, false, fmt.Errorf("save notification for event %s: %w", eventID, err)
	}
	if !saved {
		return notification.Notification{}, false, nil
	}
	n.CreatedAt = n.CreatedAt.UTC()
	return n, true, nil
}

// List возвращает до limit уведомлений пользователя, сначала новые. beforeID > 0 — только с id меньше него.
func (s *Store) List(ctx context.Context, userID int64, limit int, beforeID int64) ([]notification.Notification, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, user_id, kind, task_id, title, created_at
		   FROM notifications
		  WHERE user_id = $1 AND ($3 = 0 OR id < $3)
		  ORDER BY id DESC
		  LIMIT $2`,
		userID, limit, beforeID)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	items, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (notification.Notification, error) {
		var n notification.Notification
		err := row.Scan(&n.ID, &n.UserID, &n.Kind, &n.TaskID, &n.Title, &n.CreatedAt)
		n.CreatedAt = n.CreatedAt.UTC()
		return n, err
	})
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	return items, nil
}
