package event

import (
	"strings"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

var (
	created = time.Date(2026, 4, 30, 9, 0, 0, 0, time.UTC)
	due     = time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	sample  = task.Task{ID: 17, Title: "сдать отчёт", DueAt: &due, CreatedAt: created}
)

func TestTaskCreated(t *testing.T) {
	e := TaskCreated(3, sample)
	if e.ID != "task.created:17" || e.Type != TypeTaskCreated || e.TaskID != 17 || e.UserID != 3 ||
		e.Title != "сдать отчёт" || !e.OccurredAt.Equal(created) || e.Key() != "17" {
		t.Errorf("TaskCreated() = %+v", e)
	}
}

// Идентификатор напоминания зависит от срока: тот же срок — тот же id, новый срок — новый id.
func TestTaskDueID(t *testing.T) {
	now := time.Date(2026, 5, 1, 10, 0, 5, 0, time.UTC)
	first, again := TaskDue(3, sample, now), TaskDue(3, sample, now.Add(time.Minute))
	if first.ID != "task.due:17:1777629600" || first.ID != again.ID {
		t.Errorf("id напоминаний %q и %q, want одинаковые task.due:17:1777629600", first.ID, again.ID)
	}

	moved := sample
	later := due.Add(24 * time.Hour)
	moved.DueAt = &later
	if TaskDue(3, moved, now).ID == first.ID {
		t.Error("у перенесённой задачи id напоминания не изменился")
	}
	if id := TaskDue(3, task.Task{ID: 5}, now).ID; id != "task.due:5" {
		t.Errorf("id напоминания без срока = %q", id)
	}
}

func TestRecordRoundTrip(t *testing.T) {
	want := TaskCreated(3, sample)
	rec, err := want.Record()
	if err != nil {
		t.Fatal(err)
	}
	if rec.EventID != want.ID || rec.Topic != Topic || rec.Key != "17" {
		t.Errorf("Record() = %+v", rec)
	}
	got, err := Decode(rec.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != want.ID || got.Type != want.Type || got.TaskID != want.TaskID || got.UserID != want.UserID ||
		got.Title != want.Title || !got.DueAt.Equal(*want.DueAt) || !got.OccurredAt.Equal(want.OccurredAt) {
		t.Errorf("Decode() = %+v, want %+v", got, want)
	}
}

func TestDecodeRejectsBrokenPayload(t *testing.T) {
	tests := map[string]string{
		"не JSON":           `{не json`,
		"нет id события":    `{"type":"task.created","task_id":1,"user_id":1}`,
		"нет типа":          `{"event_id":"x","task_id":1,"user_id":1}`,
		"нет задачи":        `{"event_id":"x","type":"task.created","user_id":1}`,
		"нет пользователя":  `{"event_id":"x","type":"task.created","task_id":1}`,
		"пустой объект":     `{}`,
		"число вместо id":   `{"event_id":5,"type":"task.created","task_id":1,"user_id":1}`,
		"отрицательный id":  `{"event_id":"x","type":"task.created","task_id":-1,"user_id":1}`,
		"пустое сообщение":  ``,
		"массив":            `[]`,
		"строка в task_id":  `{"event_id":"x","type":"task.created","task_id":"1","user_id":1}`,
		"нулевой владелец":  `{"event_id":"x","type":"task.created","task_id":1,"user_id":0}`,
		"null":              `null`,
		"неизвестное время": `{"event_id":"x","type":"task.created","task_id":1,"user_id":1,"due_at":"завтра"}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode([]byte(payload)); err == nil || !strings.Contains(err.Error(), "decode event") {
				t.Errorf("Decode(%s) error = %v, want ошибку decode event", payload, err)
			}
		})
	}
	// Незнакомые поля не мешают: контракт можно расширять.
	if _, err := Decode([]byte(`{"event_id":"x","type":"task.created","task_id":1,"user_id":1,"priority":"high"}`)); err != nil {
		t.Errorf("событие с новым полем: %v", err)
	}
}
