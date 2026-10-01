// Команда taskflow — трекер задач: CLI и HTTP-сервер поверх одного JSON-файла.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/httpapi"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

const usage = `Использование:
  taskflow add <заголовок>    добавить задачу
  taskflow list               показать задачи
  taskflow done <id>          отметить задачу выполненной
  taskflow serve [-addr :8080]  запустить HTTP-сервер

Файл с задачами задаётся переменной TASKFLOW_FILE, по умолчанию tasks.json.`

var errUsage = errors.New("неверные аргументы")

func main() {
	path := os.Getenv("TASKFLOW_FILE")
	if path == "" {
		path = "tasks.json"
	}
	if err := run(os.Args[1:], task.NewFileStore(path), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "ошибка:", err)
		if errors.Is(err, errUsage) {
			fmt.Fprintln(os.Stderr, usage)
		}
		os.Exit(1)
	}
}

// run выполняет одну команду. Вынесена из main, чтобы её можно было тестировать.
func run(args []string, store *task.FileStore, out io.Writer) error {
	if len(args) == 0 {
		return errUsage
	}
	switch cmd, rest := args[0], args[1:]; cmd {
	case "add":
		t, err := store.Add(strings.Join(rest, " "))
		if errors.Is(err, task.ErrEmptyTitle) {
			return fmt.Errorf("%w: у задачи должен быть заголовок", errUsage)
		}
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "Добавлена задача %d: %s\n", t.ID, t.Title)
		return err

	case "list":
		tasks, err := store.List()
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
		t, err := store.Done(id)
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
		if err := fs.Parse(rest); err != nil {
			return fmt.Errorf("%w: %v", errUsage, err)
		}
		srv := &http.Server{
			Addr:              *addr,
			Handler:           httpapi.New(store),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
		}
		if _, err := fmt.Fprintf(out, "Сервер слушает %s\n", *addr); err != nil {
			return err
		}
		return srv.ListenAndServe()

	default:
		return fmt.Errorf("%w: неизвестная команда %q", errUsage, cmd)
	}
}
