package storetest

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// StartPostgres возвращает адрес PostgreSQL для интеграционных тестов и функцию, которая его останавливает.
//
// Если задан TASKFLOW_TEST_DSN, тесты идут в уже запущенную базу — так их можно гонять без Docker.
// Иначе PostgreSQL поднимается в контейнере. С флагом -short возвращается пустой адрес:
// интеграционные тесты пропускаются. Вызывать из TestMain после flag.Parse.
func StartPostgres(ctx context.Context) (dsn string, stop func(), err error) {
	stop = func() {}
	if dsn = os.Getenv("TASKFLOW_TEST_DSN"); dsn != "" || testing.Short() {
		return dsn, stop, nil
	}
	ctr, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("taskflow"),
		tcpostgres.WithUsername("taskflow"),
		tcpostgres.WithPassword("taskflow"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return "", stop, fmt.Errorf("start postgres container: %w", err)
	}
	stop = func() { testcontainers.TerminateContainer(ctr) } //nolint:errcheck // тесты уже завершились
	dsn, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		stop()
		return "", func() {}, fmt.Errorf("container address: %w", err)
	}
	return dsn, stop, nil
}
