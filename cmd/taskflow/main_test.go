package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
	"github.com/UbicaSmerti228/taskflow-reference/internal/tasktest"
)

// testApp собирает приложение на хранилище в памяти и считает обращения к зависимостям.
type testApp struct {
	*app
	store      *tasktest.MemStore
	migrations int
	closed     int
}

func newTestApp(out io.Writer) *testApp {
	ta := &testApp{store: tasktest.NewMemStore()}
	ta.app = &app{
		out:     out,
		migrate: func(context.Context) error { ta.migrations++; return nil },
		open: func(context.Context) (store, func(), error) {
			return ta.store, func() { ta.closed++ }, nil
		},
	}
	return ta
}

func TestRun(t *testing.T) {
	var out strings.Builder
	ta := newTestApp(&out)

	// Шаги идут по порядку и работают с одним хранилищем.
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
		{name: "миграции", args: []string{"migrate"}, wantOut: "Миграции применены"},
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
			out.Reset()
			err := ta.run(context.Background(), st.args)

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
	if ta.migrations != 1 {
		t.Errorf("миграции запускались %d раз, want 1: только по команде migrate", ta.migrations)
	}
}

func TestRunStoreErrors(t *testing.T) {
	boom := errors.New("база недоступна")

	t.Run("хранилище не открылось", func(t *testing.T) {
		ta := newTestApp(io.Discard)
		ta.open = func(context.Context) (store, func(), error) { return nil, nil, boom }
		for _, args := range [][]string{{"add", "a"}, {"list"}, {"done", "1"}} {
			if err := ta.run(context.Background(), args); !errors.Is(err, boom) {
				t.Errorf("run(%v) error = %v, want %v", args, err, boom)
			}
		}
	})

	t.Run("запрос упал, соединения закрыты", func(t *testing.T) {
		ta := newTestApp(io.Discard)
		ta.store.Err = boom
		for _, args := range [][]string{{"add", "a"}, {"list"}, {"done", "1"}} {
			if err := ta.run(context.Background(), args); !errors.Is(err, boom) {
				t.Errorf("run(%v) error = %v, want %v", args, err, boom)
			}
		}
		if ta.closed != 3 {
			t.Errorf("хранилище закрыто %d раз, want 3", ta.closed)
		}
	})

	t.Run("миграции упали — сервер не стартует", func(t *testing.T) {
		ta := newTestApp(io.Discard)
		ta.migrate = func(context.Context) error { return boom }
		opened := false
		ta.open = func(context.Context) (store, func(), error) { opened = true; return ta.store, func() {}, nil }

		for _, args := range [][]string{{"migrate"}, {"serve", "-addr", "127.0.0.1:0"}} {
			if err := ta.run(context.Background(), args); !errors.Is(err, boom) {
				t.Errorf("run(%v) error = %v, want %v", args, err, boom)
			}
		}
		if opened {
			t.Error("хранилище открыто, хотя миграции упали")
		}
	})
}

func TestNewAppRequiresDatabaseURL(t *testing.T) {
	a := newApp("", io.Discard)
	for _, args := range [][]string{{"list"}, {"migrate"}, {"serve"}} {
		err := a.run(context.Background(), args)
		if !errors.Is(err, errUsage) || !strings.Contains(err.Error(), "DATABASE_URL") {
			t.Errorf("run(%v) без DATABASE_URL: error = %v, want подсказку про DATABASE_URL", args, err)
		}
	}
}

func TestServe(t *testing.T) {
	pr, pw := io.Pipe()
	ta := newTestApp(pw)
	past := time.Now().Add(-time.Minute)
	if _, err := ta.store.Create(context.Background(), task.NewTask{Title: "просрочена", DueAt: &past}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		// Порт 0 — система сама выберет свободный, адрес узнаём из вывода.
		done <- ta.run(ctx, []string{"serve", "-addr", "127.0.0.1:0", "-remind-interval", "5ms"})
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
		got, err := ta.store.Get(context.Background(), 1)
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
	if ta.migrations != 1 || ta.closed != 1 {
		t.Errorf("миграции: %d, закрытий хранилища: %d; want 1 и 1", ta.migrations, ta.closed)
	}
}

func TestServeBusyPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck // тест завершается

	err = newTestApp(io.Discard).run(context.Background(), []string{"serve", "-addr", ln.Addr().String()})
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Errorf("serve на занятом порту вернул %v, want ошибку listen", err)
	}
}
