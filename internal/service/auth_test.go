package service

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/UbicaSmerti228/taskflow-reference/internal/memory"
	"github.com/UbicaSmerti228/taskflow-reference/internal/user"
)

var testSecret = []byte("0123456789abcdef0123456789abcdef")

type authEnv struct {
	*Auth
	users *memory.Users
	kv    *memory.KV
	now   time.Time
}

func newAuth(t *testing.T) *authEnv {
	t.Helper()
	env := &authEnv{users: memory.NewUsers(), kv: memory.NewKV(), now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	auth, err := NewAuth(env.users, env.kv, AuthConfig{
		Secret:     testSecret,
		AccessTTL:  15 * time.Minute,
		RefreshTTL: 30 * 24 * time.Hour,
		BcryptCost: bcrypt.MinCost, // настоящая стоимость сделала бы тесты медленными
		Now:        func() time.Time { return env.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	env.Auth = auth
	return env
}

// advance переводит вперёд и часы сервиса, и часы хранилища токенов.
func (e *authEnv) advance(d time.Duration) {
	e.now = e.now.Add(d)
	e.kv.Advance(d)
}

func (e *authEnv) mustLogin(t *testing.T) Tokens {
	t.Helper()
	if _, err := e.Register(ctx, "ann@example.com", "correct horse"); err != nil {
		t.Fatal(err)
	}
	tokens, err := e.Login(ctx, "ann@example.com", "correct horse")
	if err != nil {
		t.Fatal(err)
	}
	return tokens
}

func TestNewAuthRejectsShortSecret(t *testing.T) {
	if _, err := NewAuth(memory.NewUsers(), memory.NewKV(), AuthConfig{Secret: []byte("короткий")}); err == nil {
		t.Error("NewAuth с коротким секретом вернул nil, want ошибку")
	}
}

func TestRegister(t *testing.T) {
	tests := []struct {
		name     string
		email    string
		password string
		wantErr  error
	}{
		{name: "успех", email: "bob@example.com", password: "correct horse"},
		{name: "email приводится к нижнему регистру", email: "  Carl@Example.COM ", password: "correct horse"},
		{name: "занятый email", email: "ann@example.com", password: "correct horse", wantErr: user.ErrEmailTaken},
		{name: "занятый email в другом регистре", email: "ANN@example.com", password: "correct horse", wantErr: user.ErrEmailTaken},
		{name: "без собаки", email: "ann.example.com", password: "correct horse", wantErr: ErrInvalidEmail},
		{name: "пустое имя", email: "@example.com", password: "correct horse", wantErr: ErrInvalidEmail},
		{name: "пробел внутри", email: "a b@example.com", password: "correct horse", wantErr: ErrInvalidEmail},
		{name: "короткий пароль", email: "dan@example.com", password: "1234567", wantErr: ErrWeakPassword},
		{name: "пароль длиннее 72 байт", email: "dan@example.com", password: strings.Repeat("a", 73), wantErr: ErrWeakPassword},
	}
	env := newAuth(t)
	if _, err := env.Register(ctx, "ann@example.com", "correct horse"); err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := env.Register(ctx, tt.email, tt.password)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Register() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if u.Email != strings.ToLower(strings.TrimSpace(tt.email)) {
				t.Errorf("email = %q, want в нижнем регистре без пробелов", u.Email)
			}
			// В хранилище лежит bcrypt-хеш, а не пароль.
			if u.PasswordHash == tt.password || !strings.HasPrefix(u.PasswordHash, "$2") {
				t.Errorf("PasswordHash = %q, want bcrypt-хеш", u.PasswordHash)
			}
		})
	}
}

func TestSamePasswordGivesDifferentHashes(t *testing.T) {
	env := newAuth(t)
	a, _ := env.Register(ctx, "a@example.com", "qwerty123")
	b, _ := env.Register(ctx, "b@example.com", "qwerty123")
	if a.PasswordHash == b.PasswordHash {
		t.Error("у двух пользователей с одним паролем одинаковые хеши: соль не работает")
	}
}

func TestLogin(t *testing.T) {
	env := newAuth(t)
	env.mustLogin(t)

	// Неверный email и неверный пароль неотличимы: иначе можно узнать, кто зарегистрирован.
	for name, creds := range map[string][2]string{
		"неверный пароль":   {"ann@example.com", "wrong horse"},
		"неизвестный email": {"nobody@example.com", "correct horse"},
		"кривой email":      {"не email", "correct horse"},
		"пустой пароль":     {"ann@example.com", ""},
	} {
		if _, err := env.Login(ctx, creds[0], creds[1]); !errors.Is(err, ErrInvalidCredentials) {
			t.Errorf("%s: error = %v, want ErrInvalidCredentials", name, err)
		}
	}

	tokens, err := env.Login(ctx, " ANN@example.com ", "correct horse")
	if err != nil {
		t.Fatalf("вход с email в другом регистре: %v", err)
	}
	if tokens.ExpiresIn != 900 || tokens.AccessToken == "" || len(tokens.RefreshToken) != 64 {
		t.Errorf("tokens = %+v, want expires_in 900 и оба токена", tokens)
	}
}

func TestVerifyAccess(t *testing.T) {
	env := newAuth(t)
	tokens := env.mustLogin(t)

	id, err := env.VerifyAccess(tokens.AccessToken)
	if err != nil || id != 1 {
		t.Fatalf("VerifyAccess = %d, %v; want 1", id, err)
	}

	sign := func(method jwt.SigningMethod, key any, claims jwt.RegisteredClaims) string {
		s, err := jwt.NewWithClaims(method, claims).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	valid := jwt.RegisteredClaims{Subject: "1", ExpiresAt: jwt.NewNumericDate(env.now.Add(time.Hour))}

	bad := map[string]string{
		"мусор":               "не токен",
		"пустая строка":       "",
		"чужая подпись":       sign(jwt.SigningMethodHS256, []byte("другой секрет на тридцать два ба"), valid),
		"alg=none":            sign(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, valid),
		"другой алгоритм":     sign(jwt.SigningMethodHS512, testSecret, valid),
		"без срока":           sign(jwt.SigningMethodHS256, testSecret, jwt.RegisteredClaims{Subject: "1"}),
		"subject не число":    sign(jwt.SigningMethodHS256, testSecret, jwt.RegisteredClaims{Subject: "admin", ExpiresAt: valid.ExpiresAt}),
		"подделанный payload": tokens.AccessToken[:len(tokens.AccessToken)-4] + "AAAA",
	}
	for name, token := range bad {
		if _, err := env.VerifyAccess(token); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: error = %v, want ErrInvalidToken", name, err)
		}
	}

	env.advance(14 * time.Minute)
	if _, err := env.VerifyAccess(tokens.AccessToken); err != nil {
		t.Errorf("токен отклонён за минуту до истечения: %v", err)
	}
	env.advance(2 * time.Minute)
	if _, err := env.VerifyAccess(tokens.AccessToken); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("просроченный токен: error = %v, want ErrInvalidToken", err)
	}
}

func TestRefreshRotatesToken(t *testing.T) {
	env := newAuth(t)
	first := env.mustLogin(t)

	env.advance(time.Minute)
	second, err := env.Refresh(ctx, first.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if second.RefreshToken == first.RefreshToken || second.AccessToken == first.AccessToken {
		t.Error("Refresh вернул прежние токены, want новую пару")
	}
	if id, err := env.VerifyAccess(second.AccessToken); err != nil || id != 1 {
		t.Errorf("новый access-токен: id = %d, error = %v", id, err)
	}

	// Старый refresh-токен одноразовый.
	if _, err := env.Refresh(ctx, first.RefreshToken); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("повторный Refresh старым токеном: error = %v, want ErrInvalidToken", err)
	}
	if _, err := env.Refresh(ctx, "выдуманный"); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("Refresh выдуманным токеном: error = %v, want ErrInvalidToken", err)
	}
	if _, err := env.Refresh(ctx, second.RefreshToken); err != nil {
		t.Errorf("Refresh новым токеном: %v", err)
	}
}

func TestRefreshTokenExpires(t *testing.T) {
	env := newAuth(t)
	tokens := env.mustLogin(t)
	env.advance(30*24*time.Hour + time.Second)
	if _, err := env.Refresh(ctx, tokens.RefreshToken); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("Refresh через 30 дней: error = %v, want ErrInvalidToken", err)
	}
}

func TestLogoutRevokesRefreshToken(t *testing.T) {
	env := newAuth(t)
	tokens := env.mustLogin(t)
	if err := env.Logout(ctx, tokens.RefreshToken); err != nil {
		t.Fatal(err)
	}
	if _, err := env.Refresh(ctx, tokens.RefreshToken); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("Refresh после Logout: error = %v, want ErrInvalidToken", err)
	}
	if err := env.Logout(ctx, tokens.RefreshToken); err != nil {
		t.Errorf("повторный Logout: %v, want nil", err)
	}
}

// В хранилище лежит хеш refresh-токена: сам токен из него не восстановить.
func TestRefreshTokenIsStoredHashed(t *testing.T) {
	env := newAuth(t)
	tokens := env.mustLogin(t)
	if _, ok, _ := env.kv.TakeRefresh(ctx, tokens.RefreshToken); ok {
		t.Error("refresh-токен лежит в хранилище в открытом виде")
	}
	if id, ok, _ := env.kv.TakeRefresh(ctx, hashToken(tokens.RefreshToken)); !ok || id != 1 {
		t.Errorf("по хешу токена найден пользователь %d (%v), want 1", id, ok)
	}
}

func TestAuthStoreErrors(t *testing.T) {
	boom := errors.New("хранилище недоступно")

	t.Run("пользователи", func(t *testing.T) {
		env := newAuth(t)
		env.users.Err = boom
		if _, err := env.Register(ctx, "ann@example.com", "correct horse"); !errors.Is(err, boom) {
			t.Errorf("Register error = %v, want %v", err, boom)
		}
		// Сбой хранилища — это 500, а не «неверный пароль».
		if _, err := env.Login(ctx, "ann@example.com", "correct horse"); !errors.Is(err, boom) {
			t.Errorf("Login error = %v, want %v", err, boom)
		}
	})

	t.Run("токены", func(t *testing.T) {
		env := newAuth(t)
		tokens := env.mustLogin(t)
		env.kv.Err = boom
		if _, err := env.Login(ctx, "ann@example.com", "correct horse"); !errors.Is(err, boom) {
			t.Errorf("Login error = %v, want %v", err, boom)
		}
		if _, err := env.Refresh(ctx, tokens.RefreshToken); !errors.Is(err, boom) {
			t.Errorf("Refresh error = %v, want %v", err, boom)
		}
		if err := env.Logout(ctx, tokens.RefreshToken); !errors.Is(err, boom) {
			t.Errorf("Logout error = %v, want %v", err, boom)
		}
	})
}

func TestAccessTokenHasNoSecrets(t *testing.T) {
	env := newAuth(t)
	tokens := env.mustLogin(t)

	var claims jwt.MapClaims
	if _, _, err := jwt.NewParser().ParseUnverified(tokens.AccessToken, &claims); err != nil {
		t.Fatal(err)
	}
	for k := range claims {
		if k != "sub" && k != "exp" && k != "iat" {
			t.Errorf("в payload токена лишнее поле %q", k)
		}
	}
	if sub, _ := claims.GetSubject(); sub != strconv.Itoa(1) {
		t.Errorf("sub = %q, want 1", sub)
	}
}
