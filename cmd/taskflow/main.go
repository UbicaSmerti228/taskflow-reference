// Команда taskflow — трекер задач: CLI и HTTP-сервер поверх PostgreSQL.
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
	"github.com/UbicaSmerti228/taskflow-reference/internal/postgres"
	"github.com/UbicaSmerti228/taskflow-reference/internal/reminder"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

const usage = `Использование:
  taskflow add <заголовок>    добавить задачу
  taskflow list               показать задачи
  taskflow done <id>          отметить задачу выполненной
  taskflow migrate            накатить миграции базы
  taskflow serve [флаги]      накатить миграции, запустить HTTP-сервер и воркер напоминаний
    -addr :8080               адрес сервера
    -remind-interval 30s      как часто искать просроченные задачи
    -workers 4                сколько напоминаний отправлять одновременно

Адрес базы задаётся переменной DATABASE_URL, например:
  postgres://taskflow:taskflow@localhost:5432/taskflow?sslmode=disable`

var errUsage = errors.New("неверные аргументы")

// store — всё, что командам нужно от хранилища.
type store interface {
	httpapi.Store
	reminder.Store
}

// app собирает зависимости команд. В тестах база подменяется хранилищем в памяти.
type app struct {
	out     io.Writer
	migrate func(ctx context.Context) error
	open    func(ctx context.Context) (s store, closeFn func(), err error)
}

func main() {
	// SIGINT и SIGTERM отменяют контекст: сервер успевает завершить текущие запросы.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := newApp(os.Getenv("DATABASE_URL"), os.Stdout).run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ошибка:", err)
		if errors.Is(err, errUsage) {
			fmt.Fprintln(os.Stderr, usage)
		}
		os.Exit(1)
	}
}

// newApp связывает команды с PostgreSQL по адресу dsn.
func newApp(dsn string, out io.Writer) *app {
	// Без адреса базы не стартуем: лучше упасть сразу с понятной ошибкой, чем на первом запросе.
	check := func() error {
		if dsn == "" {
			return fmt.Errorf("%w: не задана переменная DATABASE_URL", errUsage)
		}
		return nil
	}
	return &app{
		out: out,
		migrate: func(ctx context.Context) error {
			if err := check(); err != nil {
				return err
			}
			return postgres.Migrate(ctx, dsn)
		},
		open: func(ctx context.Context) (store, func(), error) {
			if err := check(); err != nil {
				return nil, nil, err
			}
			pool, err := postgres.Connect(ctx, dsn)
			if err != nil {
				return nil, nil, err
			}
			s, err := postgres.NewStore(ctx, pool)
			if err != nil {
				pool.Close()
				return nil, nil, err
			}
			return s, pool.Close, nil
		},
	}
}

// run выполняет одну команду.
func (a *app) run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errUsage
	}
	switch cmd, rest := args[0], args[1:]; cmd {
	case "add":
		return a.withStore(ctx, func(s store) error {
			t, err := s.Create(ctx, task.NewTask{Title: strings.Join(rest, " ")})
			if errors.Is(err, task.ErrEmptyTitle) {
				return fmt.Errorf("%w: у задачи должен быть заголовок", errUsage)
			}
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(a.out, "Добавлена задача %d: %s\n", t.ID, t.Title)
			return err
		})

	case "list":
		return a.withStore(ctx, func(s store) error {
			tasks, _, err := s.List(ctx, task.Filter{})
			if err != nil {
				return err
			}
			if len(tasks) == 0 {
				_, err = fmt.Fprintln(a.out, "Задач нет.")
				return err
			}
			for _, t := range tasks {
				mark := " "
				if t.Done {
					mark = "x"
				}
				if _, err := fmt.Fprintf(a.out, "[%s] %d  %s\n", mark, t.ID, t.Title); err != nil {
					return err
				}
			}
			return nil
		})

	case "done":
		if len(rest) != 1 {
			return fmt.Errorf("%w: укажи id задачи", errUsage)
		}
		id, err := strconv.ParseInt(rest[0], 10, 64)
		if err != nil {
			return fmt.Errorf("%w: id должен быть числом, а не %q", errUsage, rest[0])
		}
		return a.withStore(ctx, func(s store) error {
			done := true
			t, err := s.Update(ctx, id, task.Patch{Done: &done})
			if errors.Is(err, task.ErrNotFound) {
				return fmt.Errorf("задачи с id %d нет", id)
			}
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(a.out, "Выполнена задача %d: %s\n", t.ID, t.Title)
			return err
		})

	case "migrate":
		if err := a.migrate(ctx); err != nil {
			return err
		}
		_, err := fmt.Fprintln(a.out, "Миграции применены")
		return err

	case "serve":
		fs := flag.NewFlagSet("serve", flag.ContinueOnError)
		addr := fs.String("addr", ":8080", "адрес, на котором слушает сервер")
		interval := fs.Duration("remind-interval", 30*time.Second, "как часто искать просроченные задачи")
		workers := fs.Int("workers", 4, "сколько напоминаний отправлять одновременно")
		if err := fs.Parse(rest); err != nil {
			return fmt.Errorf("%w: %v", errUsage, err)
		}
		// Схема обновляется при каждом старте: отдельный шаг деплоя не нужен.
		if err := a.migrate(ctx); err != nil {
			return err
		}
		return a.withStore(ctx, func(s store) error {
			return serve(ctx, s, *addr, *interval, *workers, a.out)
		})

	default:
		return fmt.Errorf("%w: неизвестная команда %q", errUsage, cmd)
	}
}

// withStore открывает хранилище, выполняет fn и закрывает соединения.
func (a *app) withStore(ctx context.Context, fn func(s store) error) error {
	s, closeFn, err := a.open(ctx)
	if err != nil {
		return err
	}
	defer closeFn()
	return fn(s)
}

// serve запускает HTTP-сервер и воркер напоминаний и работает, пока не отменён ctx.
func serve(ctx context.Context, s store, addr string, interval time.Duration, workers int, out io.Writer) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	ctx, stop := context.WithCancel(ctx)
	defer stop()

	var bg sync.WaitGroup
	bg.Go(func() {
		reminder.New(s, reminder.LogNotifier{}, interval, workers).Run(ctx)
	})

	srv := &http.Server{
		Handler:           httpapi.New(s),
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

	// Даём текущим запросам завершиться, потом ждём воркер. Пул базы закроется после возврата.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	bg.Wait()
	if _, werr := fmt.Fprintln(out, "Сервер остановлен"); err == nil {
		err = werr
	}
	return err
}
