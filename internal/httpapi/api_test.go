package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/memory"
	"github.com/UbicaSmerti228/taskflow-reference/internal/service"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// env — API на настоящих сервисах и хранилищах в памяти: тесты проверяют весь путь запроса без базы и Redis.
type env struct {
	*API
	t     *testing.T
	tasks *memory.Tasks
	users *memory.Users
	kv    *memory.KV
	logs  *bytes.Buffer
	ready map[string]Check
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, tasks: memory.NewTasks(), users: memory.NewUsers(), kv: memory.NewKV(), logs: &bytes.Buffer{}, ready: map[string]Check{}}
	log := logging.New(e.logs, slog.LevelDebug)

	auth, err := service.NewAuth(e.users, e.kv, service.AuthConfig{
		Secret:     []byte("0123456789abcdef0123456789abcdef"),
		AccessTTL:  15 * time.Minute,
		RefreshTTL: 30 * 24 * time.Hour,
		BcryptCost: bcrypt.MinCost,
	})
	if err != nil {
		t.Fatal(err)
	}
	api, err := New(Deps{
		Tasks:      service.NewTasks(e.tasks, e.kv, time.Minute, log),
		Auth:       auth,
		Limiter:    e.kv,
		Log:        log,
		Ready:      e.ready,
		LoginLimit: 1000, // тест ограничения частоты ставит своё значение
	})
	if err != nil {
		t.Fatal(err)
	}
	e.API = api
	return e
}

// do отправляет запрос. token может быть пустым.
func (e *env) do(method, path, token, body string) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

// login регистрирует пользователя и возвращает его токены.
func (e *env) login(email string) service.Tokens {
	e.t.Helper()
	creds := fmt.Sprintf(`{"email":%q,"password":"correct horse"}`, email)
	if rec := e.do("POST", "/api/v1/auth/register", "", creds); rec.Code != http.StatusCreated {
		e.t.Fatalf("register: status %d, тело: %s", rec.Code, rec.Body)
	}
	rec := e.do("POST", "/api/v1/auth/login", "", creds)
	if rec.Code != http.StatusOK {
		e.t.Fatalf("login: status %d, тело: %s", rec.Code, rec.Body)
	}
	return decode[service.Tokens](e.t, rec)
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("тело не разбирается как JSON: %v; тело: %s", err, rec.Body)
	}
	return v
}

// wantError проверяет статус, код ошибки и единый формат ответа.
func wantError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string, fields ...string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; тело: %s", rec.Code, status, rec.Body)
	}
	got := decode[errorBody](t, rec)
	if got.Error.Code != code || got.Error.Message == "" || got.Error.RequestID == "" {
		t.Errorf("error = %+v, want код %q, сообщение и request_id", got.Error, code)
	}
	var names []string
	for _, f := range got.Error.Fields {
		names = append(names, f.Field)
	}
	if fmt.Sprint(names) != fmt.Sprint(fields) {
		t.Errorf("поля с ошибками = %v, want %v", names, fields)
	}
}

// ---------- аутентификация ----------

func TestRegisterAndLogin(t *testing.T) {
	e := newEnv(t)

	rec := e.do("POST", "/api/v1/auth/register", "", `{"email":"Ann@Example.com","password":"correct horse"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("register: status %d, тело: %s", rec.Code, rec.Body)
	}
	if body := rec.Body.String(); !strings.Contains(body, `"email":"ann@example.com"`) || strings.Contains(body, "password") || strings.Contains(body, "$2") {
		t.Errorf("ответ регистрации = %s, want email в нижнем регистре и ни слова о пароле", body)
	}

	rec = e.do("POST", "/api/v1/auth/login", "", `{"email":"ann@example.com","password":"correct horse"}`)
	tokens := decode[service.Tokens](t, rec)
	if rec.Code != http.StatusOK || tokens.AccessToken == "" || tokens.RefreshToken == "" || tokens.ExpiresIn != 900 {
		t.Errorf("login: status %d, tokens %+v", rec.Code, tokens)
	}
}

func TestAuthErrors(t *testing.T) {
	e := newEnv(t)
	e.login("ann@example.com")

	tests := []struct {
		name   string
		path   string
		body   string
		status int
		code   string
		fields []string
	}{
		{name: "занятый email", path: "register", body: `{"email":"ann@example.com","password":"correct horse"}`, status: 409, code: "conflict"},
		// Обе ошибки возвращаются сразу, а не по одной.
		{name: "неверный email и короткий пароль", path: "register", body: `{"email":"не email","password":"123"}`, status: 400, code: "invalid_argument", fields: []string{"email", "password"}},
		{name: "нет пароля", path: "register", body: `{"email":"bob@example.com"}`, status: 400, code: "invalid_argument", fields: []string{"password"}},
		{name: "лишнее поле", path: "register", body: `{"email":"bob@example.com","password":"correct horse","role":"admin"}`, status: 400, code: "invalid_argument"},
		{name: "битый JSON", path: "register", body: `{"email":`, status: 400, code: "invalid_argument"},
		{name: "неверный пароль", path: "login", body: `{"email":"ann@example.com","password":"wrong horse"}`, status: 401, code: "invalid_credentials"},
		{name: "неизвестный email", path: "login", body: `{"email":"nobody@example.com","password":"correct horse"}`, status: 401, code: "invalid_credentials"},
		{name: "выдуманный refresh-токен", path: "refresh", body: `{"refresh_token":"выдуманный"}`, status: 401, code: "invalid_token"},
		{name: "refresh без токена", path: "refresh", body: `{}`, status: 400, code: "invalid_argument", fields: []string{"refresh_token"}},
		{name: "logout без токена", path: "logout", body: `{}`, status: 400, code: "invalid_argument", fields: []string{"refresh_token"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantError(t, e.do("POST", "/api/v1/auth/"+tt.path, "", tt.body), tt.status, tt.code, tt.fields...)
		})
	}
}

// Неверный пароль и неизвестный email дают один и тот же ответ: по нему не узнать, кто зарегистрирован.
func TestLoginDoesNotRevealWhoIsRegistered(t *testing.T) {
	e := newEnv(t)
	e.login("ann@example.com")

	message := func(body string) string {
		return decode[errorBody](t, e.do("POST", "/api/v1/auth/login", "", body)).Error.Message
	}
	wrongPassword := message(`{"email":"ann@example.com","password":"wrong horse"}`)
	unknownEmail := message(`{"email":"nobody@example.com","password":"wrong horse"}`)
	if wrongPassword != unknownEmail {
		t.Errorf("сообщения различаются: %q и %q", wrongPassword, unknownEmail)
	}
}

func TestRefreshAndLogout(t *testing.T) {
	e := newEnv(t)
	first := e.login("ann@example.com")

	rec := e.do("POST", "/api/v1/auth/refresh", "", fmt.Sprintf(`{"refresh_token":%q}`, first.RefreshToken))
	if rec.Code != http.StatusOK {
		t.Fatalf("refresh: status %d, тело: %s", rec.Code, rec.Body)
	}
	second := decode[service.Tokens](t, rec)

	// Ротация: старый refresh-токен больше не работает, новый access-токен принимается.
	wantError(t, e.do("POST", "/api/v1/auth/refresh", "", fmt.Sprintf(`{"refresh_token":%q}`, first.RefreshToken)), 401, "invalid_token")
	if rec := e.do("GET", "/api/v1/tasks", second.AccessToken, ""); rec.Code != http.StatusOK {
		t.Errorf("запрос с новым access-токеном: status %d", rec.Code)
	}

	if rec := e.do("POST", "/api/v1/auth/logout", "", fmt.Sprintf(`{"refresh_token":%q}`, second.RefreshToken)); rec.Code != http.StatusNoContent {
		t.Fatalf("logout: status %d, тело: %s", rec.Code, rec.Body)
	}
	wantError(t, e.do("POST", "/api/v1/auth/refresh", "", fmt.Sprintf(`{"refresh_token":%q}`, second.RefreshToken)), 401, "invalid_token")
}

func TestTasksRequireToken(t *testing.T) {
	e := newEnv(t)
	tokens := e.login("ann@example.com")

	for name, req := range map[string]*http.Request{
		"без заголовка":       httptest.NewRequest("GET", "/api/v1/tasks", nil),
		"не Bearer":           withHeader(httptest.NewRequest("GET", "/api/v1/tasks", nil), "Authorization", "Basic YTpi"),
		"пустой токен":        withHeader(httptest.NewRequest("GET", "/api/v1/tasks", nil), "Authorization", "Bearer "),
		"мусор вместо токена": withHeader(httptest.NewRequest("GET", "/api/v1/tasks", nil), "Authorization", "Bearer abc.def.ghi"),
		// Refresh-токен не подходит на место access-токена.
		"refresh вместо access": withHeader(httptest.NewRequest("POST", "/api/v1/tasks", strings.NewReader(`{"title":"a"}`)), "Authorization", "Bearer "+tokens.RefreshToken),
	} {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized || !strings.HasPrefix(rec.Header().Get("WWW-Authenticate"), "Bearer") {
			t.Errorf("%s: status %d, WWW-Authenticate %q; want 401 и Bearer", name, rec.Code, rec.Header().Get("WWW-Authenticate"))
		}
	}
}

func withHeader(r *http.Request, name, value string) *http.Request {
	r.Header.Set(name, value)
	return r
}

func TestLoginIsRateLimited(t *testing.T) {
	e := newEnv(t)
	e.deps.LoginLimit = 5
	body := `{"email":"ann@example.com","password":"wrong horse"}`

	for i := 1; i <= 5; i++ {
		if rec := e.do("POST", "/api/v1/auth/login", "", body); rec.Code != http.StatusUnauthorized {
			t.Fatalf("попытка %d: status %d, want 401", i, rec.Code)
		}
	}
	rec := e.do("POST", "/api/v1/auth/login", "", body)
	wantError(t, rec, http.StatusTooManyRequests, "rate_limited")
	if rec.Header().Get("Retry-After") != "60" {
		t.Errorf("Retry-After = %q, want 60", rec.Header().Get("Retry-After"))
	}

	// Через минуту окно заканчивается.
	e.kv.Advance(time.Minute + time.Second)
	if rec := e.do("POST", "/api/v1/auth/login", "", body); rec.Code != http.StatusUnauthorized {
		t.Errorf("после окна: status %d, want 401", rec.Code)
	}
}

// ---------- задачи ----------

func TestTaskCRUD(t *testing.T) {
	e := newEnv(t)
	token := e.login("ann@example.com").AccessToken

	rec := e.do("POST", "/api/v1/tasks", token, `{"title":"  купить молоко ","due_at":"2026-05-01T10:00:00Z"}`)
	if rec.Code != http.StatusCreated || rec.Header().Get("Location") != "/api/v1/tasks/1" {
		t.Fatalf("create: status %d, Location %q, тело: %s", rec.Code, rec.Header().Get("Location"), rec.Body)
	}
	created := decode[task.Task](t, rec)
	if created.Title != "купить молоко" || created.DueAt == nil {
		t.Errorf("создана задача %+v, want обрезанный заголовок и срок", created)
	}

	rec = e.do("PATCH", "/api/v1/tasks/1", token, `{"title":"купить кефир","done":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("patch: status %d, тело: %s", rec.Code, rec.Body)
	}
	got := decode[task.Task](t, e.do("GET", "/api/v1/tasks/1", token, ""))
	if got.Title != "купить кефир" || !got.Done {
		t.Errorf("после PATCH задача = %+v, want новый заголовок и done = true", got)
	}

	rec = e.do("DELETE", "/api/v1/tasks/1", token, "")
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Errorf("delete: status %d, тело %q; want 204 без тела", rec.Code, rec.Body)
	}
	wantError(t, e.do("GET", "/api/v1/tasks/1", token, ""), 404, "not_found")
	wantError(t, e.do("DELETE", "/api/v1/tasks/1", token, ""), 404, "not_found")
}

func TestTaskErrors(t *testing.T) {
	e := newEnv(t)
	token := e.login("ann@example.com").AccessToken
	e.do("POST", "/api/v1/tasks", token, `{"title":"задача"}`)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		status int
		code   string
		fields []string
	}{
		{name: "нет заголовка", method: "POST", path: "/api/v1/tasks", body: `{}`, status: 400, code: "invalid_argument", fields: []string{"title"}},
		{name: "заголовок из пробелов", method: "POST", path: "/api/v1/tasks", body: `{"title":"   "}`, status: 400, code: "invalid_argument", fields: []string{"title"}},
		{name: "заголовок длиннее 200 символов", method: "POST", path: "/api/v1/tasks", body: `{"title":"` + strings.Repeat("я", 201) + `"}`, status: 400, code: "invalid_argument", fields: []string{"title"}},
		{name: "битый JSON", method: "POST", path: "/api/v1/tasks", body: `{"title":`, status: 400, code: "invalid_argument"},
		{name: "лишнее поле", method: "POST", path: "/api/v1/tasks", body: `{"title":"a","user_id":2}`, status: 400, code: "invalid_argument"},
		{name: "неверная дата", method: "POST", path: "/api/v1/tasks", body: `{"title":"a","due_at":"завтра"}`, status: 400, code: "invalid_argument"},
		{name: "слишком большое тело", method: "POST", path: "/api/v1/tasks", body: `{"title":"` + strings.Repeat("a", maxBody) + `"}`, status: 413, code: "too_large"},
		{name: "задачи нет", method: "GET", path: "/api/v1/tasks/9", status: 404, code: "not_found"},
		{name: "id не число", method: "GET", path: "/api/v1/tasks/abc", status: 400, code: "invalid_argument", fields: []string{"id"}},
		{name: "id меньше единицы", method: "DELETE", path: "/api/v1/tasks/0", status: 400, code: "invalid_argument", fields: []string{"id"}},
		{name: "пустой патч", method: "PATCH", path: "/api/v1/tasks/1", body: `{}`, status: 400, code: "invalid_argument"},
		{name: "патч с пустым заголовком", method: "PATCH", path: "/api/v1/tasks/1", body: `{"title":" "}`, status: 400, code: "invalid_argument", fields: []string{"title"}},
		{name: "патч несуществующей задачи", method: "PATCH", path: "/api/v1/tasks/9", body: `{"done":true}`, status: 404, code: "not_found"},
		{name: "limit не число", method: "GET", path: "/api/v1/tasks?limit=many", status: 400, code: "invalid_argument", fields: []string{"limit"}},
		{name: "limit больше максимума", method: "GET", path: "/api/v1/tasks?limit=101", status: 400, code: "invalid_argument", fields: []string{"limit"}},
		{name: "отрицательный offset", method: "GET", path: "/api/v1/tasks?offset=-1", status: 400, code: "invalid_argument", fields: []string{"offset"}},
		{name: "after_id не число", method: "GET", path: "/api/v1/tasks?after_id=x", status: 400, code: "invalid_argument", fields: []string{"after_id"}},
		{name: "done не булево", method: "GET", path: "/api/v1/tasks?done=maybe", status: 400, code: "invalid_argument", fields: []string{"done"}},
		{name: "метод не поддерживается", method: "PUT", path: "/api/v1/tasks", status: 405, code: "method_not_allowed"},
		{name: "неизвестный путь", method: "GET", path: "/api/v2/tasks", status: 404, code: "not_found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantError(t, e.do(tt.method, tt.path, token, tt.body), tt.status, tt.code, tt.fields...)
		})
	}
}

// Пользователь не может прочитать, изменить или удалить чужую задачу, подставив её id.
func TestTasksAreIsolatedBetweenUsers(t *testing.T) {
	e := newEnv(t)
	ann, bob := e.login("ann@example.com").AccessToken, e.login("bob@example.com").AccessToken
	e.do("POST", "/api/v1/tasks", ann, `{"title":"секрет Анны"}`)
	e.do("GET", "/api/v1/tasks/1", ann, "") // задача попала в кэш

	wantError(t, e.do("GET", "/api/v1/tasks/1", bob, ""), 404, "not_found")
	wantError(t, e.do("PATCH", "/api/v1/tasks/1", bob, `{"done":true}`), 404, "not_found")
	wantError(t, e.do("DELETE", "/api/v1/tasks/1", bob, ""), 404, "not_found")

	if list := decode[listResponse](t, e.do("GET", "/api/v1/tasks", bob, "")); list.Total != 0 || len(list.Items) != 0 {
		t.Errorf("Боб видит чужие задачи: %+v", list)
	}
	if got := decode[task.Task](t, e.do("GET", "/api/v1/tasks/1", ann, "")); got.Title != "секрет Анны" || got.Done {
		t.Errorf("задача Анны изменилась: %+v", got)
	}
}

func TestListTasks(t *testing.T) {
	e := newEnv(t)
	token := e.login("ann@example.com").AccessToken
	for i := 1; i <= 25; i++ {
		e.do("POST", "/api/v1/tasks", token, fmt.Sprintf(`{"title":"задача %d"}`, i))
	}
	for _, id := range []int{1, 2, 3} {
		e.do("PATCH", fmt.Sprintf("/api/v1/tasks/%d", id), token, `{"done":true}`)
	}

	tests := []struct {
		name      string
		query     string
		wantFirst int64
		wantLen   int
		wantTotal int
		wantNext  int64
	}{
		{name: "по умолчанию 20 задач", query: "", wantFirst: 1, wantLen: 20, wantTotal: 25, wantNext: 20},
		{name: "вторая страница по смещению", query: "?limit=10&offset=10", wantFirst: 11, wantLen: 10, wantTotal: 25, wantNext: 20},
		{name: "вторая страница по курсору", query: "?limit=10&after_id=10", wantFirst: 11, wantLen: 10, wantTotal: 25, wantNext: 20},
		{name: "неполная последняя страница", query: "?limit=10&after_id=20", wantFirst: 21, wantLen: 5, wantTotal: 25},
		{name: "только выполненные", query: "?done=true", wantFirst: 1, wantLen: 3, wantTotal: 3},
		{name: "невыполненные", query: "?done=false&limit=5", wantFirst: 4, wantLen: 5, wantTotal: 22, wantNext: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := e.do("GET", "/api/v1/tasks"+tt.query, token, "")
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, тело: %s", rec.Code, rec.Body)
			}
			got := decode[listResponse](t, rec)
			if len(got.Items) != tt.wantLen || got.Total != tt.wantTotal || got.NextAfterID != tt.wantNext {
				t.Fatalf("items = %d, total = %d, next_after_id = %d; want %d, %d, %d",
					len(got.Items), got.Total, got.NextAfterID, tt.wantLen, tt.wantTotal, tt.wantNext)
			}
			if got.Items[0].ID != tt.wantFirst {
				t.Errorf("первая задача id = %d, want %d", got.Items[0].ID, tt.wantFirst)
			}
		})
	}

	empty := e.do("GET", "/api/v1/tasks?after_id=100", token, "")
	if !strings.Contains(empty.Body.String(), `"items":[]`) {
		t.Errorf("пустой список должен приходить как [], а не null: %s", empty.Body)
	}
}

// ---------- сквозные вещи ----------

func TestInternalErrorIsHidden(t *testing.T) {
	e := newEnv(t)
	token := e.login("ann@example.com").AccessToken
	e.tasks.Err = errors.New("pq: connection to 10.0.0.5 refused")

	for _, req := range []struct{ method, path, body string }{
		{"GET", "/api/v1/tasks", ""},
		{"POST", "/api/v1/tasks", `{"title":"задача"}`},
		{"GET", "/api/v1/tasks/1", ""},
		{"PATCH", "/api/v1/tasks/1", `{"done":true}`},
		{"DELETE", "/api/v1/tasks/1", ""},
	} {
		rec := e.do(req.method, req.path, token, req.body)
		wantError(t, rec, http.StatusInternalServerError, "internal")
		if strings.Contains(rec.Body.String(), "10.0.0.5") {
			t.Errorf("%s %s: внутренняя ошибка попала в ответ: %s", req.method, req.path, rec.Body)
		}
	}
	// Зато в логе она есть — вместе с request_id.
	if logs := e.logs.String(); !strings.Contains(logs, "10.0.0.5") || !strings.Contains(logs, `"request_id"`) {
		t.Error("внутренняя ошибка не записана в лог с request_id")
	}
}

type panicTasks struct{ TaskService }

func (panicTasks) List(context.Context, int64, task.Filter) ([]task.Task, int, error) {
	panic("nil pointer в хендлере")
}

func TestPanicBecomes500(t *testing.T) {
	e := newEnv(t)
	token := e.login("ann@example.com").AccessToken
	e.deps.Tasks = panicTasks{e.deps.Tasks}

	wantError(t, e.do("GET", "/api/v1/tasks", token, ""), http.StatusInternalServerError, "internal")
	if !strings.Contains(e.logs.String(), `"msg":"panic"`) {
		t.Error("паника не записана в лог")
	}
	// Сервис пережил панику и продолжает отвечать.
	if rec := e.do("GET", "/healthz", "", ""); rec.Code != http.StatusOK {
		t.Errorf("после паники /healthz: status %d", rec.Code)
	}
}

func TestRequestID(t *testing.T) {
	e := newEnv(t)

	rec := e.do("GET", "/healthz", "", "")
	generated := rec.Header().Get("X-Request-ID")
	if len(generated) != 36 {
		t.Errorf("сгенерированный X-Request-ID = %q, want UUID", generated)
	}

	req := withHeader(httptest.NewRequest("GET", "/api/v1/tasks", nil), "X-Request-ID", "trace-123")
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if rec.Header().Get("X-Request-ID") != "trace-123" || decode[errorBody](t, rec).Error.RequestID != "trace-123" {
		t.Errorf("переданный X-Request-ID не сохранён: заголовок %q, тело %s", rec.Header().Get("X-Request-ID"), rec.Body)
	}

	// Подозрительный заголовок заменяется: он не должен попасть в логи как есть.
	req = withHeader(httptest.NewRequest("GET", "/healthz", nil), "X-Request-ID", "a\"}\n{\"msg\":\"fake")
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); len(got) != 36 {
		t.Errorf("небезопасный X-Request-ID пропущен: %q", got)
	}
}

func TestAccessLog(t *testing.T) {
	e := newEnv(t)
	token := e.login("ann@example.com").AccessToken
	e.do("POST", "/api/v1/tasks", token, `{"title":"задача"}`)
	e.logs.Reset()

	e.do("GET", "/api/v1/tasks/1", token, "")

	var rec map[string]any
	line, _, _ := strings.Cut(strings.TrimSpace(e.logs.String()), "\n")
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("строка лога не JSON: %v: %s", err, line)
	}
	// В логе шаблон маршрута, а не путь с id, и нет токена.
	if rec["msg"] != "request" || rec["method"] != "GET" || rec["route"] != "/api/v1/tasks/:id" || rec["status"] != float64(200) ||
		rec["user_id"] != float64(1) || rec["request_id"] == nil || rec["duration_ms"] == nil {
		t.Errorf("запись лога = %v", rec)
	}
	if strings.Contains(e.logs.String(), token) || strings.Contains(e.logs.String(), "correct horse") {
		t.Error("в лог попал токен или пароль")
	}
}

func TestHealth(t *testing.T) {
	e := newEnv(t)
	dbDown := false
	e.ready["postgres"] = func(context.Context) error {
		if dbDown {
			return errors.New("connection refused")
		}
		return nil
	}
	e.ready["redis"] = func(context.Context) error { return nil }

	if rec := e.do("GET", "/readyz", "", ""); rec.Code != http.StatusOK {
		t.Errorf("/readyz: status %d, want 200", rec.Code)
	}

	dbDown = true
	rec := e.do("GET", "/readyz", "", "")
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"failed":["postgres"]`) || strings.Contains(rec.Body.String(), "refused") {
		t.Errorf("/readyz без базы: status %d, тело %s; want 503, имя зависимости и без текста ошибки", rec.Code, rec.Body)
	}
	// Процесс жив, даже когда база лежит: перезапускать его незачем.
	if rec := e.do("GET", "/healthz", "", ""); rec.Code != http.StatusOK {
		t.Errorf("/healthz без базы: status %d, want 200", rec.Code)
	}

	dbDown = false
	e.StartShutdown()
	if rec := e.do("GET", "/readyz", "", ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz при остановке: status %d, want 503", rec.Code)
	}
}

// Сломанный ограничитель частоты не мешает войти.
func TestBrokenLimiterDoesNotBlockLogin(t *testing.T) {
	e := newEnv(t)
	e.login("ann@example.com")
	e.deps.Limiter = brokenLimiter{}

	if rec := e.do("POST", "/api/v1/auth/login", "", `{"email":"ann@example.com","password":"correct horse"}`); rec.Code != http.StatusOK {
		t.Errorf("вход при сломанном ограничителе: status %d, тело: %s", rec.Code, rec.Body)
	}
}

type brokenLimiter struct{}

func (brokenLimiter) Allow(context.Context, string, int, time.Duration) (bool, error) {
	return false, errors.New("redis недоступен")
}

func TestNewRejectsBadProxies(t *testing.T) {
	if _, err := New(Deps{TrustedProxies: []string{"не адрес"}}); err == nil {
		t.Error("New с неверным адресом прокси вернул nil, want ошибку")
	}
}
