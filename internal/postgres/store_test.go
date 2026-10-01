package postgres

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/UbicaSmerti228/taskflow-reference/internal/storetest"
	"github.com/UbicaSmerti228/taskflow-reference/internal/task"
)

// Один PostgreSQL на весь пакет: старт контейнера занимает секунды, тестов много.
var testPool *pgxpool.Pool

// TestMain поднимает PostgreSQL в контейнере и накатывает миграции.
// Если задан TASKFLOW_TEST_DSN, тесты идут в уже запущенную базу — так их можно гонять без Docker.
// С флагом -short интеграционные тесты пропускаются.
func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	flag.Parse() // в TestMain флаги ещё не разобраны, а нам нужен -short
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	dsn := os.Getenv("TASKFLOW_TEST_DSN")
	if dsn == "" {
		if !testing.Short() {
			ctr, err := tcpostgres.Run(ctx, "postgres:18-alpine",
				tcpostgres.WithDatabase("taskflow"),
				tcpostgres.WithUsername("taskflow"),
				tcpostgres.WithPassword("taskflow"),
				tcpostgres.BasicWaitStrategies(),
			)
			if err != nil {
				fmt.Fprintln(os.Stderr, "start postgres container:", err)
				return 1
			}
			defer testcontainers.TerminateContainer(ctr) //nolint:errcheck // тесты уже завершились

			dsn, err = ctr.ConnectionString(ctx, "sslmode=disable")
			if err != nil {
				fmt.Fprintln(os.Stderr, "container address:", err)
				return 1
			}
		}
	}

	if dsn != "" {
		if err := Migrate(ctx, dsn); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		pool, err := Connect(ctx, dsn)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer pool.Close()
		testPool = pool
	}
	return m.Run()
}

// reset очищает таблицы. RESTART IDENTITY возвращает счётчики id к единице: тесты не зависят друг от друга.
func reset(t *testing.T) {
	t.Helper()
	if testPool == nil {
		t.Skip("интеграционный тест: запусти без -short (нужен Docker) или задай TASKFLOW_TEST_DSN")
	}
	if _, err := testPool.Exec(context.Background(), `TRUNCATE users, tasks RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
}

func TestTasks(t *testing.T) {
	storetest.RunTaskSuite(t, func(t *testing.T) (storetest.TaskStore, int64, int64) {
		reset(t)
		ctx, users := context.Background(), NewUsers(testPool)
		alice, err := users.Create(ctx, "alice@example.com", "hash")
		if err != nil {
			t.Fatal(err)
		}
		bob, err := users.Create(ctx, "bob@example.com", "hash")
		if err != nil {
			t.Fatal(err)
		}
		return NewTasks(testPool), alice.ID, bob.ID
	})
}

func TestUsers(t *testing.T) {
	storetest.RunUserSuite(t, func(t *testing.T) storetest.UserStore {
		reset(t)
		return NewUsers(testPool)
	})
}

func TestMigrateTwice(t *testing.T) {
	reset(t)
	// Повторный запуск ничего не меняет: так сервис стартует каждый раз.
	if err := Migrate(context.Background(), testPool.Config().ConnString()); err != nil {
		t.Errorf("повторный Migrate() error = %v, want nil", err)
	}
}

// Ограничения схемы — последняя линия защиты: они срабатывают, даже если проверку в коде забыли.
func TestSchemaConstraints(t *testing.T) {
	reset(t)
	ctx := context.Background()
	u, err := NewUsers(testPool).Create(ctx, "ann@example.com", "hash")
	if err != nil {
		t.Fatal(err)
	}

	bad := map[string]string{
		"задача без пользователя":            `INSERT INTO tasks (user_id, title) VALUES (999999, 'a')`,
		"задача с пустым заголовком":         `INSERT INTO tasks (user_id, title) VALUES ($1, '   ')`,
		"второй пользователь с тем же email": `INSERT INTO users (email) VALUES ('ann@example.com')`,
		"пользователь с пустым email":        `INSERT INTO users (email) VALUES ('')`,
		"email с заглавными буквами":         `INSERT INTO users (email) VALUES ('Bob@Example.com')`,
	}
	for name, query := range bad {
		args := []any{}
		if name == "задача с пустым заголовком" {
			args = append(args, u.ID)
		}
		if _, err := testPool.Exec(ctx, query, args...); err == nil {
			t.Errorf("%s: запрос прошёл, want ошибку ограничения", name)
		}
	}

	// Удаление пользователя забирает с собой его задачи.
	if _, err := NewTasks(testPool).Create(ctx, u.ID, task.NewTask{Title: "задача"}); err != nil {
		t.Fatal(err)
	}
	if _, err := testPool.Exec(ctx, `DELETE FROM users WHERE id = $1`, u.ID); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM tasks`).Scan(&left); err != nil || left != 0 {
		t.Errorf("после удаления пользователя осталось %d задач (%v), want 0", left, err)
	}
}

func TestConnectErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := Connect(ctx, "это не DSN"); err == nil {
		t.Error("Connect() с мусором вместо DSN вернул nil, want ошибку")
	}
	if _, err := Connect(ctx, "postgres://nobody:nothing@127.0.0.1:1/none?sslmode=disable"); err == nil {
		t.Error("Connect() к закрытому порту вернул nil, want ошибку")
	}
}
