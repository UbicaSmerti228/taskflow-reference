package main

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

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
			err := run(st.args, store, &out)

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
