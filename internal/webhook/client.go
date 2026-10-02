// Package webhook отправляет напоминания во внешний сервис по HTTP.
// Чужой сервис может тормозить и падать, поэтому каждый вызов ограничен таймаутом,
// временные отказы повторяются, а серия отказов размыкает circuit breaker.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/resilience"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// Имена заголовков запроса.
const (
	HeaderEventID   = "X-Taskflow-Event-Id"  // один и тот же у всех повторов одного события
	HeaderSignature = "X-Taskflow-Signature" // sha256=<HMAC тела>, если задан секрет
)

// Event — тело запроса.
type Event struct {
	Event  string     `json:"event"`
	TaskID int64      `json:"task_id"`
	Title  string     `json:"title"`
	DueAt  *time.Time `json:"due_at,omitempty"`
}

// Client отправляет события на один адрес. Безопасен для одновременного использования.
type Client struct {
	url     string
	secret  []byte
	timeout time.Duration
	http    *http.Client
	retry   resilience.Retry
	breaker *resilience.Breaker
}

// Option меняет одну настройку клиента.
type Option func(*Client)

// WithSecret включает подпись тела: получатель проверяет, что запрос пришёл от TaskFlow.
func WithSecret(secret []byte) Option { return func(c *Client) { c.secret = secret } }

// WithTimeout ограничивает одну попытку целиком: соединение, запрос и чтение ответа.
func WithTimeout(d time.Duration) Option { return func(c *Client) { c.timeout = d } }

// WithRetry задаёт правила повторов.
func WithRetry(r resilience.Retry) Option { return func(c *Client) { c.retry = r } }

// WithBreaker задаёт circuit breaker.
func WithBreaker(b *resilience.Breaker) Option { return func(c *Client) { c.breaker = b } }

// WithHTTPClient подменяет HTTP-клиент, например в тестах.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// New собирает клиент. По умолчанию: 3 секунды на попытку, 3 попытки с паузой до 2 секунд,
// breaker размыкается после 5 отказов подряд на 30 секунд.
func New(url string, opts ...Option) *Client {
	c := &Client{
		url:     url,
		timeout: 3 * time.Second,
		// Свой клиент, а не http.DefaultClient: тот общий на весь процесс.
		// Перенаправления не выполняем: адрес задан в настройках, и уводить запрос в другое место некому.
		http:    &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		retry:   resilience.Retry{Attempts: 3, Base: 200 * time.Millisecond, Max: 2 * time.Second},
		breaker: resilience.NewBreaker(5, 30*time.Second),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Notify отправляет напоминание о задаче. Возвращает nil, только когда получатель ответил 2xx.
//
// Доставка «хотя бы один раз»: ответ мог потеряться после того, как получатель обработал запрос,
// и тогда событие придёт повторно. Получатель отличает повторы по заголовку X-Taskflow-Event-Id.
func (c *Client) Notify(ctx context.Context, t task.Task) error {
	body, err := json.Marshal(Event{Event: "task.due", TaskID: t.ID, Title: t.Title, DueAt: t.DueAt})
	if err != nil {
		return err
	}
	id := eventID(t)

	return c.retry.Do(ctx, func(ctx context.Context) error {
		var rejected error
		err := c.breaker.Do(func() error {
			err := c.send(ctx, id, body)
			if errors.Is(err, errRejected) {
				// Получатель жив и ответил отказом: для breaker это не сбой.
				rejected = err
				return nil
			}
			return err
		})
		// Отказ получателя и разомкнутый breaker повтор не исправит.
		if rejected != nil {
			return resilience.Permanent(rejected)
		}
		if errors.Is(err, resilience.ErrOpen) {
			return resilience.Permanent(err)
		}
		return err
	})
}

// eventID одинаков для всех попыток доставить одно напоминание:
// у задачи одно напоминание на один срок.
func eventID(t task.Task) string {
	id := "task.due:" + strconv.FormatInt(t.ID, 10)
	if t.DueAt != nil {
		id += ":" + strconv.FormatInt(t.DueAt.Unix(), 10)
	}
	return id
}

// errRejected — получатель понял запрос и отказал: повтор даст тот же ответ.
var errRejected = errors.New("webhook rejected")

func (c *Client) send(ctx context.Context, id string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("%w: %w", errRejected, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderEventID, id)
	if len(c.secret) > 0 {
		req.Header.Set(HeaderSignature, "sha256="+Sign(c.secret, body))
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // ответ уже получен
	// Тело дочитываем, чтобы соединение вернулось в пул, но не больше 64 КБ: чужому ответу не доверяем.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500:
		return fmt.Errorf("webhook: status %d", resp.StatusCode)
	default:
		return fmt.Errorf("%w: status %d", errRejected, resp.StatusCode)
	}
}

// Sign считает подпись тела: HMAC-SHA256 в шестнадцатеричном виде.
// Получатель считает её тем же секретом и сравнивает через hmac.Equal.
func Sign(secret, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
