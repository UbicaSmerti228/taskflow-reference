// Package httpapi — HTTP-слой: разбирает запрос, зовёт сервис и превращает результат в ответ.
// Бизнес-логики здесь нет.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"

	"github.com/UbicaSmerti228/taskflow-reference/internal/service"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
	"github.com/UbicaSmerti228/taskflow-reference/internal/user"
)

// TaskService — что хендлерам нужно от сервиса задач.
type TaskService interface {
	Create(ctx context.Context, userID int64, in task.NewTask) (task.Task, error)
	Get(ctx context.Context, userID, id int64) (task.Task, error)
	List(ctx context.Context, userID int64, f task.Filter) ([]task.Task, int, error)
	Update(ctx context.Context, userID, id int64, p task.Patch) (task.Task, error)
	Delete(ctx context.Context, userID, id int64) error
}

// AuthService — что хендлерам нужно от сервиса аутентификации.
type AuthService interface {
	Register(ctx context.Context, email, password string) (user.User, error)
	Login(ctx context.Context, email, password string) (service.Tokens, error)
	Refresh(ctx context.Context, refreshToken string) (service.Tokens, error)
	Logout(ctx context.Context, refreshToken string) error
	VerifyAccess(token string) (int64, error)
}

// Limiter ограничивает частоту запросов по ключу.
type Limiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error)
}

// Check проверяет одну зависимость сервиса, например базу.
type Check func(ctx context.Context) error

// Deps — зависимости HTTP-слоя. Их собирает main.
type Deps struct {
	Tasks          TaskService
	Auth           AuthService
	Limiter        Limiter
	Log            *slog.Logger
	Ready          map[string]Check // проверки для /readyz: имя зависимости → проверка
	LoginLimit     int              // попыток входа и регистрации в минуту с одного IP
	TrustedProxies []string
}

// API — обработчик со всеми маршрутами.
type API struct {
	http.Handler
	deps         Deps
	shuttingDown atomic.Bool
}

const maxBody = 1 << 20 // 1 МБ

func init() {
	gin.SetMode(gin.ReleaseMode)
	// Строгий разбор JSON: неизвестное поле в теле — ошибка, а не молчаливый пропуск.
	binding.EnableDecoderDisallowUnknownFields = true
	// В ошибках валидации называем поле так, как его видит клиент: по тегу json.
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		v.RegisterTagNameFunc(func(f reflect.StructField) string {
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			return name
		})
	}
}

// New собирает роутер. Порядок middleware важен: каждый следующий работает внутри предыдущего.
func New(deps Deps) (*API, error) {
	a := &API{deps: deps}

	r := gin.New()
	r.HandleMethodNotAllowed = true
	// Без списка доверенных прокси заголовку X-Forwarded-For не верим: иначе клиент подставит любой IP.
	if err := r.SetTrustedProxies(deps.TrustedProxies); err != nil {
		return nil, err
	}

	r.Use(
		a.requestID, // 1. у запроса появляется id — он попадёт во все логи ниже
		a.accessLog, // 2. лог пишется после ответа и видит итоговый статус, в том числе 500 от recover
		a.recover,   // 3. паника в хендлере превращается в 500, сервис продолжает работать
		a.limitBody, // 4. тело больше 1 МБ не читается
	)
	r.NoRoute(func(c *gin.Context) { fail(c, http.StatusNotFound, "not_found", "no such route") })
	r.NoMethod(func(c *gin.Context) { fail(c, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed") })

	r.GET("/healthz", a.healthz)
	r.GET("/readyz", a.readyz)

	v1 := r.Group("/api/v1")

	auth := v1.Group("/auth")
	auth.POST("/register", a.rateLimit("register"), a.register)
	auth.POST("/login", a.rateLimit("login"), a.login)
	auth.POST("/refresh", a.refresh)
	auth.POST("/logout", a.logout)

	tasks := v1.Group("/tasks", a.requireUser) // всё ниже доступно только с access-токеном
	tasks.GET("", a.listTasks)
	tasks.POST("", a.createTask)
	tasks.GET("/:id", a.getTask)
	tasks.PATCH("/:id", a.updateTask)
	tasks.DELETE("/:id", a.deleteTask)

	a.Handler = r
	return a, nil
}

// StartShutdown переводит /readyz в 503: балансировщик перестаёт слать новые запросы,
// пока сервер дообрабатывает текущие.
func (a *API) StartShutdown() { a.shuttingDown.Store(true) }
