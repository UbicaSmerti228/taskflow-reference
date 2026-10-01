// Package user описывает пользователя сервиса.
package user

import (
	"errors"
	"time"
)

// User — учётная запись. Хеш пароля никогда не попадает в JSON.
type User struct {
	ID           int64     `json:"id"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"-"`
	CreatedAt    time.Time `json:"created_at"`
}

// Ошибки хранилища пользователей.
var (
	ErrNotFound   = errors.New("user not found")
	ErrEmailTaken = errors.New("email is already taken")
)
