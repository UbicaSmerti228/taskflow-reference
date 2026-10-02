package notifier

import (
	"sync"

	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
)

// subscriberBuffer — сколько уведомлений подписчик может не успеть прочитать, прежде чем его отключат.
const subscriberBuffer = 16

// Hub раздаёт новые уведомления подписчикам внутри процесса: тем, кто держит открытый стрим.
type Hub struct {
	mu   sync.Mutex
	subs map[int64]map[chan notification.Notification]struct{} // пользователь → его подписчики
}

// NewHub возвращает пустой Hub.
func NewHub() *Hub {
	return &Hub{subs: map[int64]map[chan notification.Notification]struct{}{}}
}

// Subscribe подписывает на уведомления пользователя. Канал закроется, если подписчик читает слишком медленно.
// cancel отписывает; вызывать его нужно обязательно, иначе подписка останется в памяти.
func (h *Hub) Subscribe(userID int64) (ch <-chan notification.Notification, cancel func()) {
	c := make(chan notification.Notification, subscriberBuffer)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[userID] == nil {
		h.subs[userID] = map[chan notification.Notification]struct{}{}
	}
	h.subs[userID][c] = struct{}{}
	return c, func() { h.remove(userID, c) }
}

// Publish отдаёт уведомление всем подписчикам его владельца и никогда не блокируется:
// чтение из Kafka не должно зависеть от скорости клиентов.
func (h *Hub) Publish(n notification.Notification) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.subs[n.UserID] {
		select {
		case c <- n:
		default:
			// Буфер полон: подписчик не успевает. Отключаем его — клиент переподключится
			// и возьмёт пропущенное через ListNotifications.
			h.drop(n.UserID, c)
		}
	}
}

func (h *Hub) remove(userID int64, c chan notification.Notification) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.drop(userID, c)
}

// drop вызывается под мьютексом. Повторный вызов для того же канала безопасен.
func (h *Hub) drop(userID int64, c chan notification.Notification) {
	if _, ok := h.subs[userID][c]; !ok {
		return
	}
	delete(h.subs[userID], c)
	if len(h.subs[userID]) == 0 {
		delete(h.subs, userID)
	}
	close(c)
}

// Subscribers возвращает число открытых подписок — для метрик и тестов.
func (h *Hub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, set := range h.subs {
		n += len(set)
	}
	return n
}
