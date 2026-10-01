package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/UbicaSmerti228/taskflow-reference/internal/user"
)

// UserStore — методы хранилища пользователей.
type UserStore interface {
	Create(ctx context.Context, email, passwordHash string) (user.User, error)
	ByEmail(ctx context.Context, email string) (user.User, error)
}

// RunUserSuite проверяет поведение хранилища пользователей. newStore возвращает пустое хранилище.
func RunUserSuite(t *testing.T, newStore func(t *testing.T) UserStore) {
	ctx := context.Background()
	s := newStore(t)

	created, err := s.Create(ctx, "ann@example.com", "hash-1")
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == 0 || created.Email != "ann@example.com" || created.PasswordHash != "hash-1" || created.CreatedAt.IsZero() {
		t.Errorf("Create() = %+v, want заполненного пользователя", created)
	}

	got, err := s.ByEmail(ctx, "ann@example.com")
	if err != nil || got.ID != created.ID || got.PasswordHash != "hash-1" {
		t.Errorf("ByEmail() = %+v, %v; want пользователя %d с хешем", got, err, created.ID)
	}

	if _, err := s.ByEmail(ctx, "nobody@example.com"); !errors.Is(err, user.ErrNotFound) {
		t.Errorf("ByEmail(неизвестный) error = %v, want ErrNotFound", err)
	}
	if _, err := s.Create(ctx, "ann@example.com", "hash-2"); !errors.Is(err, user.ErrEmailTaken) {
		t.Errorf("Create(занятый email) error = %v, want ErrEmailTaken", err)
	}

	second, err := s.Create(ctx, "bob@example.com", "hash-3")
	if err != nil || second.ID == created.ID {
		t.Errorf("второй пользователь = %+v, %v; want нового с другим id", second, err)
	}
}
