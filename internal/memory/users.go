package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/user"
)

// Users хранит пользователей в памяти.
type Users struct {
	mu    sync.Mutex
	users []user.User

	Err error // если задано, все методы возвращают эту ошибку
}

// NewUsers возвращает пустое хранилище пользователей.
func NewUsers() *Users { return &Users{} }

// Create сохраняет пользователя. Занятый email даёт user.ErrEmailTaken.
func (s *Users) Create(_ context.Context, email, passwordHash string) (user.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return user.User{}, s.Err
	}
	for _, u := range s.users {
		if u.Email == email {
			return user.User{}, fmt.Errorf("create user: %w", user.ErrEmailTaken)
		}
	}
	u := user.User{ID: int64(len(s.users) + 1), Email: email, PasswordHash: passwordHash, CreatedAt: time.Now().UTC()}
	s.users = append(s.users, u)
	return u, nil
}

// ByEmail ищет пользователя по email или возвращает user.ErrNotFound.
func (s *Users) ByEmail(_ context.Context, email string) (user.User, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return user.User{}, s.Err
	}
	for _, u := range s.users {
		if u.Email == email {
			return u, nil
		}
	}
	return user.User{}, fmt.Errorf("user %s: %w", email, user.ErrNotFound)
}
