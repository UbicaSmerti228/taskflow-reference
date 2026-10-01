package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/UbicaSmerti228/taskflow-reference/internal/user"
)

// Ошибки аутентификации. Они намеренно не уточняют причину:
// «неверный email» и «неверный пароль» для клиента выглядят одинаково.
var (
	ErrInvalidCredentials = errors.New("invalid email or password")
	ErrInvalidToken       = errors.New("invalid or expired token")
	ErrWeakPassword       = errors.New("password must be 8 to 72 bytes long")
	ErrInvalidEmail       = errors.New("invalid email")
)

// UserRepo — что сервису аутентификации нужно от хранилища пользователей.
type UserRepo interface {
	Create(ctx context.Context, email, passwordHash string) (user.User, error)
	ByEmail(ctx context.Context, email string) (user.User, error)
}

// RefreshStore хранит refresh-токены на стороне сервера, поэтому их можно отозвать.
type RefreshStore interface {
	SaveRefresh(ctx context.Context, tokenHash string, userID int64, ttl time.Duration) error
	TakeRefresh(ctx context.Context, tokenHash string) (userID int64, ok bool, err error)
	DeleteRefresh(ctx context.Context, tokenHash string) error
}

// AuthConfig — настройки сервиса аутентификации.
type AuthConfig struct {
	Secret     []byte        // ключ подписи access-токенов
	AccessTTL  time.Duration // короткий: украденный токен быстро перестаёт работать
	RefreshTTL time.Duration
	BcryptCost int              // 0 — значение по умолчанию; тесты ставят минимальное, чтобы не ждать
	Now        func() time.Time // nil — time.Now
}

// Tokens — пара токенов, которую получает клиент.
type Tokens struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"` // время жизни access-токена в секундах
}

// Auth — регистрация, вход и работа с токенами.
type Auth struct {
	users   UserRepo
	refresh RefreshStore
	cfg     AuthConfig
	// dummyHash сравнивается с паролем, когда пользователя нет:
	// ответ занимает столько же времени, и по скорости нельзя угадать, зарегистрирован ли email.
	dummyHash []byte
}

// NewAuth собирает сервис аутентификации.
func NewAuth(users UserRepo, refresh RefreshStore, cfg AuthConfig) (*Auth, error) {
	if len(cfg.Secret) < 32 {
		return nil, errors.New("jwt secret must be at least 32 bytes")
	}
	if cfg.BcryptCost == 0 {
		cfg.BcryptCost = bcrypt.DefaultCost
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	dummy, err := bcrypt.GenerateFromPassword([]byte("no such user"), cfg.BcryptCost)
	if err != nil {
		return nil, fmt.Errorf("prepare dummy hash: %w", err)
	}
	return &Auth{users: users, refresh: refresh, cfg: cfg, dummyHash: dummy}, nil
}

func normalizeEmail(s string) (string, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	at := strings.LastIndex(s, "@")
	if at < 1 || at == len(s)-1 || strings.ContainsAny(s, " \t\r\n") {
		return "", ErrInvalidEmail
	}
	return s, nil
}

// Register создаёт пользователя. В базу попадает только bcrypt-хеш пароля.
func (a *Auth) Register(ctx context.Context, email, password string) (user.User, error) {
	email, err := normalizeEmail(email)
	if err != nil {
		return user.User{}, err
	}
	// bcrypt учитывает только первые 72 байта: более длинный пароль отклоняем, а не обрезаем молча.
	if len(password) < 8 || len(password) > 72 {
		return user.User{}, ErrWeakPassword
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), a.cfg.BcryptCost)
	if err != nil {
		return user.User{}, fmt.Errorf("hash password: %w", err)
	}
	return a.users.Create(ctx, email, string(hash))
}

// Login проверяет пароль и выдаёт пару токенов.
func (a *Auth) Login(ctx context.Context, email, password string) (Tokens, error) {
	email, err := normalizeEmail(email)
	if err != nil {
		return Tokens{}, ErrInvalidCredentials
	}
	u, err := a.users.ByEmail(ctx, email)
	if errors.Is(err, user.ErrNotFound) {
		_ = bcrypt.CompareHashAndPassword(a.dummyHash, []byte(password))
		return Tokens{}, ErrInvalidCredentials
	}
	if err != nil {
		return Tokens{}, err
	}
	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)) != nil {
		return Tokens{}, ErrInvalidCredentials
	}
	return a.issue(ctx, u.ID)
}

// Refresh меняет refresh-токен на новую пару. Старый токен перестаёт работать (ротация):
// если его украли и использовали, настоящий владелец заметит это по отказу.
func (a *Auth) Refresh(ctx context.Context, refreshToken string) (Tokens, error) {
	userID, ok, err := a.refresh.TakeRefresh(ctx, hashToken(refreshToken))
	if err != nil {
		return Tokens{}, err
	}
	if !ok {
		return Tokens{}, ErrInvalidToken
	}
	return a.issue(ctx, userID)
}

// Logout отзывает refresh-токен. Access-токен доживёт свои минуты: он нигде не хранится.
func (a *Auth) Logout(ctx context.Context, refreshToken string) error {
	return a.refresh.DeleteRefresh(ctx, hashToken(refreshToken))
}

// VerifyAccess проверяет подпись и срок access-токена и возвращает id пользователя.
func (a *Auth) VerifyAccess(token string) (int64, error) {
	var claims jwt.RegisteredClaims
	_, err := jwt.ParseWithClaims(token, &claims,
		func(*jwt.Token) (any, error) { return a.cfg.Secret, nil },
		jwt.WithValidMethods([]string{"HS256"}), // иначе токен с alg=none прошёл бы проверку
		jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(a.cfg.Now),
	)
	if err != nil {
		return 0, ErrInvalidToken
	}
	id, err := strconv.ParseInt(claims.Subject, 10, 64)
	if err != nil || id < 1 {
		return 0, ErrInvalidToken
	}
	return id, nil
}

func (a *Auth) issue(ctx context.Context, userID int64) (Tokens, error) {
	now := a.cfg.Now()
	access, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   strconv.FormatInt(userID, 10),
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(a.cfg.AccessTTL)),
	}).SignedString(a.cfg.Secret)
	if err != nil {
		return Tokens{}, fmt.Errorf("sign access token: %w", err)
	}

	// Refresh-токен — просто случайная строка: её смысл хранится на сервере.
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return Tokens{}, fmt.Errorf("generate refresh token: %w", err)
	}
	refresh := hex.EncodeToString(raw)
	if err := a.refresh.SaveRefresh(ctx, hashToken(refresh), userID, a.cfg.RefreshTTL); err != nil {
		return Tokens{}, err
	}
	return Tokens{AccessToken: access, RefreshToken: refresh, ExpiresIn: int(a.cfg.AccessTTL.Seconds())}, nil
}

// hashToken: в хранилище лежит хеш токена, а не сам токен. Утечка хранилища не выдаёт рабочих токенов.
func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
