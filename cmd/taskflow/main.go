// Команда taskflow — трекер задач: CLI и HTTP-сервер поверх одного JSON-файла.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/httpapi"
	"github.com/UbicaSmerti228/taskflow-reference/internal/reminder"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

const usage = `Использование:
  taskflow add <заголовок>    добавить задачу
  taskflow list               показать задачи
  taskflow done <id>          отметить задачу выполненной
  taskflow serve [флаги]      запустить HTTP-сервер и воркер напоминаний
    -addr :8080               адрес сервера
    -remind-interval 30s      как часто искать просроченные задачи
    -workers 4                сколько напоминаний отправлять одновременно

Файл с задачами задаётся переменной TASKFLOW_FILE, по умолчанию tasks.json.`

var errUsage = errors.New("неверные аргументы")

func main() {
	path := os.Getenv("TASKFLOW_FILE")
	if path == "" {
		path = "tasks.json"
	}
	// SIGINT и SIGTERM отменяют контекст: сервер успевает завершить текущие запросы.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], task.NewFileStore(path), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "ошибка:", err)
		if errors.Is(err, errUsage) {
			fmt.Fprintln(os.Stderr, usage)
		}
		os.Exit(1)
	}
}

// run выполняет одну команду. Вынесена из main, чтобы её можно было тестировать.
func run(ctx context.Context, args []string, store *task.FileStore, out io.Writer) error {
	if len(args) == 0 {
		return errUsage
	}
	switch cmd, rest := args[0], args[1:]; cmd {
	case "add":
		t, err := store.Create(task.NewTask{Title: strings.Join(rest, " ")})
		if errors.Is(err, task.ErrEmptyTitle) {
			return fmt.Errorf("%w: у задачи должен быть заголовок", errUsage)
		}
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "Добавлена задача %d: %s\n", t.ID, t.Title)
		return err

	case "list":
		tasks, _, err := store.List(task.Filter{})
		if err != nil {
			return err
		}
		if len(tasks) == 0 {
			_, err = fmt.Fprintln(out, "Задач нет.")
			return err
		}
		for _, t := range tasks {
			mark := " "
			if t.Done {
				mark = "x"
			}
			if _, err := fmt.Fprintf(out, "[%s] %d  %s\n", mark, t.ID, t.Title); err != nil {
				return err
			}
		}
		return nil

	case "done":
		if len(rest) != 1 {
			return fmt.Errorf("%w: укажи id задачи", errUsage)
		}
		id, err := strconv.Atoi(rest[0])
		if err != nil {
			return fmt.Errorf("%w: id должен быть числом, а не %q", errUsage, rest[0])
		}
		done := true
		t, err := store.Update(id, task.Patch{Done: &done})
		if errors.Is(err, task.ErrNotFound) {
			return fmt.Errorf("задачи с id %d нет", id)
		}
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "Выполнена задача %d: %s\n", t.ID, t.Title)
		return err

	case "serve":
		fs := flag.NewFlagSet("serve", flag.ContinueOnError)
		addr := fs.String("addr", ":8080", "адрес, на котором слушает сервер")
		interval := fs.Duration("remind-interval", 30*time.Second, "как часто искать просроченные задачи")
		workers := fs.Int("workers", 4, "сколько напоминаний отправлять одновременно")
		if err := fs.Parse(rest); err != nil {
			return fmt.Errorf("%w: %v", errUsage, err)
		}
		return serve(ctx, store, *addr, *interval, *workers, out)

	default:
		return fmt.Errorf("%w: неизвестная команда %q", errUsage, cmd)
	}
}

// serve запускает HTTP-сервер и воркер напоминаний и работает, пока не отменён ctx.
func serve(ctx context.Context, store *task.FileStore, addr string, interval time.Duration, workers int, out io.Writer) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	ctx, stop := context.WithCancel(ctx)
	defer stop()

	var bg sync.WaitGroup
	bg.Go(func() {
		reminder.New(store, reminder.LogNotifier{}, interval, workers).Run(ctx)
	})

	srv := &http.Server{
		Handler:           httpapi.New(store),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}
	failed := make(chan error, 1)
	go func() { failed <- srv.Serve(ln) }()

	if _, err := fmt.Fprintf(out, "Сервер слушает %s\n", ln.Addr()); err != nil {
		return err
	}

	select {
	case err := <-failed:
		stop()
		bg.Wait()
		return err
	case <-ctx.Done():
	}

	// Даём текущим запросам завершиться, потом ждём воркер.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	bg.Wait()
	if _, werr := fmt.Fprintln(out, "Сервер остановлен"); err == nil {
		err = werr
	}
	return err
}
