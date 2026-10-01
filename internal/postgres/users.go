package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/UbicaSmerti228/taskflow-reference/internal/user"
)

const uniqueViolation = "23505" // код ошибки PostgreSQL: нарушено ограничение уникальности

// Users — репозиторий пользователей.
type Users struct {
	pool *pgxpool.Pool
}

// NewUsers возвращает репозиторий пользователей.
func NewUsers(pool *pgxpool.Pool) *Users { return &Users{pool: pool} }

// Create сохраняет пользователя. Уникальность email гарантирует база:
// два одновременных запроса с одним адресом не пройдут оба, как прошли бы через проверку в коде.
func (s *Users) Create(ctx context.Context, email, passwordHash string) (user.User, error) {
	rows, err := s.pool.Query(ctx,
		`INSERT INTO users (email, password_hash) VALUES ($1, $2)
		 RETURNING id, email, password_hash, created_at`, email, passwordHash)
	if err != nil {
		return user.User{}, fmt.Errorf("create user: %w", err)
	}
	u, err := pgx.CollectExactlyOneRow(rows, scanUser)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return user.User{}, fmt.Errorf("create user: %w", user.ErrEmailTaken)
		}
		return user.User{}, fmt.Errorf("create user: %w", err)
	}
	return u, nil
}

// ByEmail ищет пользователя по email или возвращает user.ErrNotFound.
func (s *Users) ByEmail(ctx context.Context, email string) (user.User, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, email, password_hash, created_at FROM users WHERE email = $1`, email)
	if err != nil {
		return user.User{}, fmt.Errorf("find user: %w", err)
	}
	u, err := pgx.CollectExactlyOneRow(rows, scanUser)
	if errors.Is(err, pgx.ErrNoRows) {
		return user.User{}, fmt.Errorf("find user: %w", user.ErrNotFound)
	}
	if err != nil {
		return user.User{}, fmt.Errorf("find user: %w", err)
	}
	return u, nil
}

func scanUser(row pgx.CollectableRow) (user.User, error) {
	var u user.User
	if err := row.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt); err != nil {
		return user.User{}, err
	}
	u.CreatedAt = u.CreatedAt.UTC()
	return u, nil
}
