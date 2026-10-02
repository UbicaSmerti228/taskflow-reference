package httpapi

import (
	"net/http"
	"regexp"
	"runtime/debug"
	"strings"
	"time"
	"uuid"

	"github.com/gin-gonic/gin"

	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
)

const userIDKey = "user_id"

var safeID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// requestID берёт id из заголовка X-Request-ID или создаёт новый, кладёт его в контекст и в ответ.
func (a *API) requestID(c *gin.Context) {
	id := c.GetHeader("X-Request-ID")
	if !safeID.MatchString(id) { // чужой заголовок попадает в логи: пропускаем только безопасные символы
		id = uuid.NewV7().String()
	}
	c.Request = c.Request.WithContext(logging.WithRequestID(c.Request.Context(), id))
	c.Header("X-Request-ID", id)
	c.Next()
}

// observe пишет одну запись в лог и одно наблюдение в метрики на запрос.
func (a *API) observe(c *gin.Context) {
	start := time.Now()
	c.Next()
	took := time.Since(start)

	// Шаблон маршрута, а не путь с id: по нему группируются и логи, и метрики.
	// Несуществующие пути сведены к одному значению — иначе сканер портов создал бы тысячи рядов метрик.
	route := c.FullPath()
	if route == "" {
		route = "unmatched"
	}
	if a.deps.Metrics != nil {
		a.deps.Metrics.Request(c.Request.Method, route, c.Writer.Status(), took)
	}
	attrs := []any{
		"method", c.Request.Method,
		"route", route,
		"status", c.Writer.Status(),
		"duration_ms", took.Milliseconds(),
		"ip", c.ClientIP(),
	}
	if id, ok := c.Get(userIDKey); ok {
		attrs = append(attrs, "user_id", id)
	}

	ctx := c.Request.Context()
	switch status := c.Writer.Status(); {
	case status >= 500:
		a.deps.Log.ErrorContext(ctx, "request", attrs...)
	case route == "/healthz" || route == "/readyz":
		a.deps.Log.DebugContext(ctx, "request", attrs...) // пробы приходят каждые несколько секунд
	default:
		a.deps.Log.InfoContext(ctx, "request", attrs...) // 4xx — ошибка клиента, для нас это штатная работа
	}
}

// recover перехватывает панику в хендлере и отвечает 500.
func (a *API) recover(c *gin.Context) {
	defer func() {
		if r := recover(); r != nil {
			a.deps.Log.ErrorContext(c.Request.Context(), "panic", "panic", r, "stack", string(debug.Stack()))
			fail(c, http.StatusInternalServerError, "internal", "internal error")
		}
	}()
	c.Next()
}

func (a *API) limitBody(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBody)
	c.Next()
}

// requireUser проверяет access-токен и кладёт id пользователя в контекст запроса.
func (a *API) requireUser(c *gin.Context) {
	token, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
	if !ok || token == "" {
		c.Header("WWW-Authenticate", "Bearer")
		fail(c, http.StatusUnauthorized, "unauthenticated", "access token is required")
		return
	}
	userID, err := a.deps.Auth.VerifyAccess(token)
	if err != nil {
		c.Header("WWW-Authenticate", `Bearer error="invalid_token"`)
		fail(c, http.StatusUnauthorized, "invalid_token", "invalid or expired token")
		return
	}
	c.Set(userIDKey, userID)
	c.Next()
}

func userID(c *gin.Context) int64 { return c.GetInt64(userIDKey) }

// rateLimit ограничивает число запросов в минуту с одного IP: так перебор паролей становится медленным.
func (a *API) rateLimit(name string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ok, err := a.deps.Limiter.Allow(c.Request.Context(), name+":"+c.ClientIP(), a.deps.LoginLimit, time.Minute)
		if err != nil {
			// Ограничитель недоступен — пропускаем запрос: вход важнее, чем защита от перебора на эти минуты.
			a.deps.Log.WarnContext(c.Request.Context(), "rate limiter failed", "err", err)
		} else if !ok {
			c.Header("Retry-After", "60")
			fail(c, http.StatusTooManyRequests, "rate_limited", "too many attempts, try again in a minute")
			return
		}
		c.Next()
	}
}
