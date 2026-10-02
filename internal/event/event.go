// Package event описывает события, которыми сервисы обмениваются через Kafka.
// Это контракт между TaskFlow и notifier: поля можно добавлять, но нельзя переименовывать и удалять.
package event

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// Topic — топик, в который TaskFlow публикует события задач.
const Topic = "taskflow.tasks"

// TopicPartitions — число партиций топика. Столько экземпляров notifier могут читать его одновременно.
// Увеличивать его у работающего топика нельзя без последствий: ключи начнут попадать в другие партиции,
// и порядок событий одной задачи на время нарушится.
const TopicPartitions = 3

// Типы событий.
const (
	TypeTaskCreated = "task.created"
	TypeTaskDue     = "task.due"
)

// Event — событие задачи. В Kafka оно лежит как JSON.
type Event struct {
	// ID один и тот же при любой повторной доставке: по нему получатель отбрасывает дубликаты.
	ID         string     `json:"event_id"`
	Type       string     `json:"type"`
	TaskID     int64      `json:"task_id"`
	UserID     int64      `json:"user_id"`
	Title      string     `json:"title"`
	DueAt      *time.Time `json:"due_at,omitempty"`
	OccurredAt time.Time  `json:"occurred_at"`
}

// TaskCreated — событие «задача создана». У задачи оно одно, поэтому id события строится из id задачи.
func TaskCreated(userID int64, t task.Task) Event {
	return Event{
		ID:         TypeTaskCreated + ":" + strconv.FormatInt(t.ID, 10),
		Type:       TypeTaskCreated,
		TaskID:     t.ID,
		UserID:     userID,
		Title:      t.Title,
		DueAt:      t.DueAt,
		OccurredAt: t.CreatedAt,
	}
}

// TaskDue — событие «срок задачи наступил». Срок входит в id: у перенесённой задачи будет новое напоминание,
// а повторная попытка напомнить о том же сроке даст тот же id.
func TaskDue(userID int64, t task.Task, now time.Time) Event {
	id := TypeTaskDue + ":" + strconv.FormatInt(t.ID, 10)
	if t.DueAt != nil {
		id += ":" + strconv.FormatInt(t.DueAt.Unix(), 10)
	}
	return Event{ID: id, Type: TypeTaskDue, TaskID: t.ID, UserID: userID, Title: t.Title, DueAt: t.DueAt, OccurredAt: now.UTC()}
}

// Key — ключ сообщения Kafka. События одной задачи попадают в одну партицию и читаются в порядке записи.
func (e Event) Key() string { return strconv.FormatInt(e.TaskID, 10) }

// Record — событие в том виде, в котором оно лежит в outbox и уходит в Kafka.
type Record struct {
	EventID string
	Topic   string
	Key     string
	Payload []byte
}

// Record упаковывает событие для отправки.
func (e Event) Record() (Record, error) {
	payload, err := json.Marshal(e)
	if err != nil {
		return Record{}, fmt.Errorf("encode event %s: %w", e.ID, err)
	}
	return Record{EventID: e.ID, Topic: Topic, Key: e.Key(), Payload: payload}, nil
}

// Decode разбирает событие из сообщения Kafka и проверяет обязательные поля.
func Decode(payload []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(payload, &e); err != nil {
		return Event{}, fmt.Errorf("decode event: %w", err)
	}
	if e.ID == "" || e.Type == "" || e.TaskID < 1 || e.UserID < 1 {
		return Event{}, fmt.Errorf("decode event: missing required fields in %q", payload)
	}
	return e, nil
}
