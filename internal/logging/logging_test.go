package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestRequestIDIsAddedToEveryRecord(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, slog.LevelInfo).With("service", "taskflow")
	ctx := WithRequestID(context.Background(), "req-42")

	log.InfoContext(ctx, "запрос обработан", "status", 200)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("лог не является JSON: %v: %s", err, buf.String())
	}
	if rec["request_id"] != "req-42" || rec["service"] != "taskflow" || rec["status"] != float64(200) || rec["msg"] != "запрос обработан" {
		t.Errorf("запись = %v, want request_id, service, status и msg", rec)
	}
}

func TestNoRequestIDWithoutContext(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, slog.LevelInfo).WithGroup("g").Info("старт")
	if bytes.Contains(buf.Bytes(), []byte("request_id")) {
		t.Errorf("request_id появился без контекста запроса: %s", buf.String())
	}
}

func TestLevel(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, slog.LevelWarn)
	log.Info("не должно попасть в лог")
	if buf.Len() != 0 {
		t.Errorf("сообщение уровня info записано при уровне warn: %s", buf.String())
	}
	log.Warn("попадает")
	if buf.Len() == 0 {
		t.Error("сообщение уровня warn не записано")
	}
}

func TestRequestIDEmptyByDefault(t *testing.T) {
	if id := RequestID(context.Background()); id != "" {
		t.Errorf("RequestID пустого контекста = %q, want пустую строку", id)
	}
}
