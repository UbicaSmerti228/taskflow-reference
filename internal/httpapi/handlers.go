package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

const (
	defaultLimit = 20
	maxLimit     = 100
)

// ---------- здоровье ----------

// healthz отвечает, пока процесс жив. Зависимости здесь не проверяются:
// иначе при падении базы оркестратор начал бы перезапускать все экземпляры.
func (a *API) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// readyz отвечает 200, когда сервис готов принимать запросы: база и Redis доступны.
func (a *API) readyz(c *gin.Context) {
	if a.shuttingDown.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "shutting_down"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	var failed []string
	for name, check := range a.deps.Ready {
		if err := check(ctx); err != nil {
			a.deps.Log.WarnContext(ctx, "dependency is not ready", "dependency", name, "err", err)
			failed = append(failed, name)
		}
	}
	if len(failed) > 0 {
		sort.Strings(failed)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "failed": failed})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// ---------- аутентификация ----------

type credentials struct {
	Email    string `json:"email" binding:"required,email,max=254"`
	Password string `json:"password" binding:"required,min=8,max=72"`
}

func (a *API) register(c *gin.Context) {
	var in credentials
	if !bind(c, &in) {
		return
	}
	u, err := a.deps.Auth.Register(c.Request.Context(), in.Email, in.Password)
	if err != nil {
		a.failErr(c, err)
		return
	}
	c.JSON(http.StatusCreated, u)
}

func (a *API) login(c *gin.Context) {
	// Здесь правила мягче, чем при регистрации: любой неверный ввод должен дать одно и то же «неверный email или пароль».
	var in struct {
		Email    string `json:"email" binding:"required"`
		Password string `json:"password" binding:"required"`
	}
	if !bind(c, &in) {
		return
	}
	tokens, err := a.deps.Auth.Login(c.Request.Context(), in.Email, in.Password)
	if err != nil {
		a.failErr(c, err)
		return
	}
	c.JSON(http.StatusOK, tokens)
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

func (a *API) refresh(c *gin.Context) {
	var in refreshRequest
	if !bind(c, &in) {
		return
	}
	tokens, err := a.deps.Auth.Refresh(c.Request.Context(), in.RefreshToken)
	if err != nil {
		a.failErr(c, err)
		return
	}
	c.JSON(http.StatusOK, tokens)
}

func (a *API) logout(c *gin.Context) {
	var in refreshRequest
	if !bind(c, &in) {
		return
	}
	if err := a.deps.Auth.Logout(c.Request.Context(), in.RefreshToken); err != nil {
		a.failErr(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// ---------- задачи ----------

type listResponse struct {
	Items  []task.Task `json:"items"`
	Total  int         `json:"total"`
	Limit  int         `json:"limit"`
	Offset int         `json:"offset"`
	// NextAfterID — курсор следующей страницы: передай его в after_id. Его нет, если страница неполная.
	NextAfterID int64 `json:"next_after_id,omitempty"`
}

func (a *API) listTasks(c *gin.Context) {
	f := task.Filter{Limit: defaultLimit}

	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxLimit {
			fail(c, http.StatusBadRequest, "invalid_argument", fmt.Sprintf("limit must be a number from 1 to %d", maxLimit), fieldError{"limit", "range"})
			return
		}
		f.Limit = n
	}
	if v := c.Query("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			fail(c, http.StatusBadRequest, "invalid_argument", "offset must be a non-negative number", fieldError{"offset", "min"})
			return
		}
		f.Offset = n
	}
	if v := c.Query("after_id"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			fail(c, http.StatusBadRequest, "invalid_argument", "after_id must be a non-negative number", fieldError{"after_id", "min"})
			return
		}
		f.AfterID = n
	}
	if v := c.Query("done"); v != "" {
		done, err := strconv.ParseBool(v)
		if err != nil {
			fail(c, http.StatusBadRequest, "invalid_argument", "done must be true or false", fieldError{"done", "boolean"})
			return
		}
		f.Done = &done
	}

	tasks, total, err := a.deps.Tasks.List(c.Request.Context(), userID(c), f)
	if err != nil {
		a.failErr(c, err)
		return
	}
	resp := listResponse{Items: tasks, Total: total, Limit: f.Limit, Offset: f.Offset}
	if len(tasks) == f.Limit {
		resp.NextAfterID = tasks[len(tasks)-1].ID
	}
	c.JSON(http.StatusOK, resp)
}

func (a *API) createTask(c *gin.Context) {
	var in struct {
		Title string     `json:"title" binding:"required,max=200"`
		DueAt *time.Time `json:"due_at"`
	}
	if !bind(c, &in) {
		return
	}
	t, err := a.deps.Tasks.Create(c.Request.Context(), userID(c), task.NewTask{Title: in.Title, DueAt: in.DueAt})
	if err != nil {
		a.failErr(c, err)
		return
	}
	c.Header("Location", fmt.Sprintf("/api/v1/tasks/%d", t.ID))
	c.JSON(http.StatusCreated, t)
}

func (a *API) getTask(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	// Владелец передаётся в сервис: чужая задача для пользователя «не существует» (404, а не 403),
	// так по ответу нельзя даже узнать, что такой id есть.
	t, err := a.deps.Tasks.Get(c.Request.Context(), userID(c), id)
	if err != nil {
		a.failErr(c, err)
		return
	}
	c.JSON(http.StatusOK, t)
}

func (a *API) updateTask(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	var in struct {
		Title *string    `json:"title" binding:"omitempty,max=200"`
		Done  *bool      `json:"done"`
		DueAt *time.Time `json:"due_at"`
	}
	if !bind(c, &in) {
		return
	}
	t, err := a.deps.Tasks.Update(c.Request.Context(), userID(c), id, task.Patch{Title: in.Title, Done: in.Done, DueAt: in.DueAt})
	if err != nil {
		a.failErr(c, err)
		return
	}
	c.JSON(http.StatusOK, t)
}

func (a *API) deleteTask(c *gin.Context) {
	id, ok := pathID(c)
	if !ok {
		return
	}
	if err := a.deps.Tasks.Delete(c.Request.Context(), userID(c), id); err != nil {
		a.failErr(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// ---------- уведомления ----------

type notificationsResponse struct {
	Items []notification.Notification `json:"items"`
	// NextBeforeID — курсор следующей страницы: передай его в before_id. Его нет, если страница неполная.
	NextBeforeID int64 `json:"next_before_id,omitempty"`
}

// listNotifications отдаёт уведомления пользователя. Их хранит сервис notifier: хендлер идёт к нему по gRPC.
func (a *API) listNotifications(c *gin.Context) {
	limit := notification.DefaultLimit
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > notification.MaxLimit {
			fail(c, http.StatusBadRequest, "invalid_argument", fmt.Sprintf("limit must be a number from 1 to %d", notification.MaxLimit), fieldError{"limit", "range"})
			return
		}
		limit = n
	}
	var beforeID int64
	if v := c.Query("before_id"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			fail(c, http.StatusBadRequest, "invalid_argument", "before_id must be a non-negative number", fieldError{"before_id", "min"})
			return
		}
		beforeID = n
	}

	// Пользователь берётся из токена, а не из запроса: чужие уведомления запросить нельзя.
	items, err := a.deps.Notifications.List(c.Request.Context(), userID(c), limit, beforeID)
	if err != nil {
		a.failErr(c, err)
		return
	}
	resp := notificationsResponse{Items: items}
	if resp.Items == nil {
		resp.Items = []notification.Notification{} // в JSON — пустой массив, а не null
	}
	if len(items) == limit {
		resp.NextBeforeID = items[len(items)-1].ID
	}
	c.JSON(http.StatusOK, resp)
}

func pathID(c *gin.Context) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id < 1 {
		fail(c, http.StatusBadRequest, "invalid_argument", "id must be a positive number", fieldError{"id", "min"})
		return 0, false
	}
	return id, true
}
