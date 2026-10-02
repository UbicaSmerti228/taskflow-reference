package service

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/memory"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// Запуск: go test -bench=. -benchmem -run='^$' ./internal/service
// Что показали замеры и что из них следует, записано в docs/perf.md.

var sink string

// Ключ кэша строится на каждое чтение задачи.
func BenchmarkTaskKey(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sink = taskKey(42, 123456)
	}
}

// Горячий путь сервиса: чтение задачи, которая уже лежит в кэше.
func BenchmarkTasksGetCached(b *testing.B) {
	svc := NewTasks(memory.NewTasks(), memory.NewKV(), time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))
	due := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	created, err := svc.Create(ctx, 42, task.NewTask{Title: "сдать квартальный отчёт", DueAt: &due})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := svc.Get(ctx, 42, created.ID); err != nil { // первый вызов кладёт задачу в кэш
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := svc.Get(ctx, 42, created.ID); err != nil {
			b.Fatal(err)
		}
	}
}
