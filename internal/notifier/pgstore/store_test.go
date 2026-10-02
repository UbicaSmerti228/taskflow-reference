package pgstore

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/UbicaSmerti228/taskflow-reference/internal/notification"
	"github.com/UbicaSmerti228/taskflow-reference/internal/storetest"
)

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	dsn, stop, err := storetest.StartPostgres(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer stop()

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

func reset(t *testing.T) {
	t.Helper()
	if testPool == nil {
		t.Skip("интеграционный тест: запусти без -short (нужен Docker) или задай TASKFLOW_TEST_DSN")
	}
	if _, err := testPool.Exec(context.Background(), `TRUNCATE notifications, processed_events RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
}

func TestStore(t *testing.T) {
	storetest.RunNotificationSuite(t, func(t *testing.T) storetest.NotificationStore {
		reset(t)
		return New(testPool)
	})
}

// Таблицы лежат в схеме notifier, а не в public: сервис не пересекается с таблицами TaskFlow.
func TestTablesLiveInOwnSchema(t *testing.T) {
	reset(t)
	ctx := context.Background()
	for _, table := range []string{"notifications", "processed_events", "goose_db_version"} {
		var schema string
		err := testPool.QueryRow(ctx,
			`SELECT schemaname FROM pg_tables WHERE tablename = $1 AND schemaname = $2`, table, Schema).Scan(&schema)
		if err != nil {
			t.Errorf("таблица %s не найдена в схеме %s: %v", table, Schema, err)
		}
	}
	// Повторный запуск миграций ничего не меняет.
	if err := Migrate(ctx, testPool.Config().ConnString()); err != nil {
		t.Errorf("повторный Migrate() error = %v, want nil", err)
	}
}

// Одно и то же событие пришло двум экземплярам одновременно: уведомление одно.
func TestSaveConcurrentDuplicates(t *testing.T) {
	reset(t)
	ctx, s := context.Background(), New(testPool)
	n := notification.Notification{UserID: 1, Kind: notification.KindTaskDue, TaskID: 7, Title: "отчёт", CreatedAt: time.Now()}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		saved int
	)
	for range 20 {
		wg.Go(func() {
			_, isNew, err := s.Save(ctx, "task.due:7:1", n)
			if err != nil {
				t.Errorf("Save() error = %v", err)
			}
			if isNew {
				mu.Lock()
				saved++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if saved != 1 {
		t.Errorf("сохранений %d, want 1", saved)
	}
	if got, _ := s.List(ctx, 1, 100, 0); len(got) != 1 {
		t.Errorf("уведомлений в базе %d, want 1", len(got))
	}
}

func TestConnectErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := Migrate(ctx, "это не DSN"); err == nil {
		t.Error("Migrate() с мусором вместо DSN вернул nil")
	}
	if _, err := Connect(ctx, "postgres://nobody:nothing@127.0.0.1:1/none?sslmode=disable"); err == nil {
		t.Error("Connect() к закрытому порту вернул nil")
	}
}
