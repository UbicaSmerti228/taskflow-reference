package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

func TestRun(t *testing.T) {
	store := task.NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))

	// Шаги идут по порядку и работают с одним файлом.
	steps := []struct {
		name      string
		args      []string
		wantOut   string
		wantErr   string
		wantUsage bool
	}{
		{name: "пустой список", args: []string{"list"}, wantOut: "Задач нет."},
		{name: "добавление", args: []string{"add", "купить", "молоко"}, wantOut: "Добавлена задача 1: купить молоко"},
		{name: "список", args: []string{"list"}, wantOut: "[ ] 1  купить молоко"},
		{name: "выполнение", args: []string{"done", "1"}, wantOut: "Выполнена задача 1"},
		{name: "список после выполнения", args: []string{"list"}, wantOut: "[x] 1  купить молоко"},
		{name: "неизвестный id", args: []string{"done", "7"}, wantErr: "задачи с id 7 нет"},
		{name: "id не число", args: []string{"done", "abc"}, wantErr: "id должен быть числом", wantUsage: true},
		{name: "done без id", args: []string{"done"}, wantErr: "укажи id", wantUsage: true},
		{name: "пустой заголовок", args: []string{"add"}, wantErr: "заголовок", wantUsage: true},
		{name: "неизвестная команда", args: []string{"remove"}, wantErr: "неизвестная команда", wantUsage: true},
		{name: "нет аргументов", args: nil, wantErr: "неверные аргументы", wantUsage: true},
		{name: "неверный флаг serve", args: []string{"serve", "-port", "1"}, wantErr: "port", wantUsage: true},
	}
	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			var out strings.Builder
			err := run(context.Background(), st.args, store, &out)

			if st.wantErr == "" {
				if err != nil {
					t.Fatalf("run(%v) error = %v", st.args, err)
				}
				if !strings.Contains(out.String(), st.wantOut) {
					t.Errorf("run(%v) напечатал %q, want строку с %q", st.args, out.String(), st.wantOut)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), st.wantErr) {
				t.Fatalf("run(%v) error = %v, want ошибку с %q", st.args, err, st.wantErr)
			}
			if got := errors.Is(err, errUsage); got != st.wantUsage {
				t.Errorf("run(%v): errors.Is(err, errUsage) = %v, want %v", st.args, got, st.wantUsage)
			}
		})
	}
}

func TestServe(t *testing.T) {
	store := task.NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	past := time.Now().Add(-time.Minute)
	if _, err := store.Create(task.NewTask{Title: "просрочена", DueAt: &past}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		// Порт 0 — система сама выберет свободный, адрес узнаём из вывода.
		done <- run(ctx, []string{"serve", "-addr", "127.0.0.1:0", "-remind-interval", "5ms"}, store, pw)
		pw.Close() //nolint:errcheck // у PipeWriter Close всегда возвращает nil
	}()

	lines := bufio.NewScanner(pr)
	if !lines.Scan() {
		t.Fatalf("сервер ничего не напечатал: %v", <-done)
	}
	addr := strings.TrimPrefix(lines.Text(), "Сервер слушает ")

	resp, err := http.Get("http://" + addr + "/tasks")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close() //nolint:errcheck // тело не читаем
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /tasks status = %d, want 200", resp.StatusCode)
	}

	// Воркер напоминаний работает рядом с сервером.
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := store.Get(1)
		if err != nil {
			t.Fatal(err)
		}
		if got.RemindedAt != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("воркер не отправил напоминание за 5 секунд")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	if !lines.Scan() || lines.Text() != "Сервер остановлен" {
		t.Errorf("после отмены напечатано %q, want «Сервер остановлен»", lines.Text())
	}
	if err := <-done; err != nil {
		t.Errorf("serve вернул %v, want nil", err)
	}
	if _, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		t.Error("после остановки порт всё ещё принимает соединения")
	}
}

func TestServeBusyPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // тест завершается

	store := task.NewFileStore(filepath.Join(t.TempDir(), "tasks.json"))
	err = run(context.Background(), []string{"serve", "-addr", ln.Addr().String()}, store, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Errorf("serve на занятом порту вернул %v, want ошибку listen", err)
	}
}
