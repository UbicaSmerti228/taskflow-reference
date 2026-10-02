package httpapi

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"

	"github.com/UbicaSmerti228/taskflow-reference/internal/logging"
	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
	"github.com/UbicaSmerti228/taskflow-reference/internal/service"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
	"github.com/UbicaSmerti228/taskflow-reference/internal/user"
)

// Все ошибки API приходят в одном формате.
type errorBody struct {
	Error errorInfo `json:"error"`
}

type errorInfo struct {
	Code      string       `json:"code"`
	Message   string       `json:"message"`
	Fields    []fieldError `json:"fields,omitempty"` // какие поля не прошли валидацию
	RequestID string       `json:"request_id"`       // по нему запрос находится в логах
}

type fieldError struct {
	Field string `json:"field"`
	Rule  string `json:"rule"`
}

func fail(c *gin.Context, status int, code, msg string, fields ...fieldError) {
	c.AbortWithStatusJSON(status, errorBody{Error: errorInfo{
		Code: code, Message: msg, Fields: fields, RequestID: logging.RequestID(c.Request.Context()),
	}})
}

// failErr переводит ошибку сервиса в ответ. Это единственное место, где ошибки превращаются в статусы.
func (a *API) failErr(c *gin.Context, err error) {
	switch {
	case errors.Is(err, task.ErrNotFound):
		fail(c, http.StatusNotFound, "not_found", "task not found")
	case errors.Is(err, task.ErrEmptyTitle):
		fail(c, http.StatusBadRequest, "invalid_argument", "title is required", fieldError{"title", "required"})
	case errors.Is(err, task.ErrTitleTooLong):
		fail(c, http.StatusBadRequest, "invalid_argument", "title is too long", fieldError{"title", "max"})
	case errors.Is(err, task.ErrEmptyPatch):
		fail(c, http.StatusBadRequest, "invalid_argument", "nothing to update")
	case errors.Is(err, user.ErrEmailTaken):
		// Формат запроса верный, мешает состояние системы — это 409, а не 400.
		fail(c, http.StatusConflict, "conflict", "email is already taken")
	case errors.Is(err, service.ErrInvalidEmail):
		fail(c, http.StatusBadRequest, "invalid_argument", "invalid email", fieldError{"email", "email"})
	case errors.Is(err, service.ErrWeakPassword):
		fail(c, http.StatusBadRequest, "invalid_argument", "password must be 8 to 72 bytes long", fieldError{"password", "min"})
	case errors.Is(err, service.ErrInvalidCredentials):
		fail(c, http.StatusUnauthorized, "invalid_credentials", "invalid email or password")
	case errors.Is(err, notification.ErrUnavailable):
		// Соседний сервис не отвечает. Это не ошибка клиента и не поломка этого сервиса: остальное API работает.
		a.deps.Log.WarnContext(c.Request.Context(), "notifier is unavailable", "err", err)
		c.Header("Retry-After", "5")
		fail(c, http.StatusServiceUnavailable, "notifications_unavailable", "notifications are temporarily unavailable")
	case errors.Is(err, service.ErrInvalidToken):
		fail(c, http.StatusUnauthorized, "invalid_token", "invalid or expired token")
	default:
		// Подробности — в лог, клиенту — общее сообщение и request_id.
		a.deps.Log.ErrorContext(c.Request.Context(), "internal error", "err", err)
		fail(c, http.StatusInternalServerError, "internal", "internal error")
	}
}

// bind разбирает и проверяет JSON-тело. При ошибке сам отвечает клиенту и возвращает false.
func bind(c *gin.Context, dst any) bool {
	err := c.ShouldBindJSON(dst)
	if err == nil {
		return true
	}
	var tooBig *http.MaxBytesError
	var invalid validator.ValidationErrors
	switch {
	case errors.As(err, &tooBig):
		fail(c, http.StatusRequestEntityTooLarge, "too_large", "request body is too large")
	case errors.As(err, &invalid):
		// Отдаём сразу все поля с ошибками: клиент подсветит их за один раз.
		fields := make([]fieldError, len(invalid))
		for i, fe := range invalid {
			fields[i] = fieldError{Field: fe.Field(), Rule: fe.Tag()}
		}
		fail(c, http.StatusBadRequest, "invalid_argument", "validation failed", fields...)
	default:
		fail(c, http.StatusBadRequest, "invalid_argument", "invalid JSON body")
	}
	return false
}
