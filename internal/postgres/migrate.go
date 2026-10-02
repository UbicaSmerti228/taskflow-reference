package postgres

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib" // драйвер database/sql: он нужен только goose
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrate накатывает все новые миграции. Файлы вшиты в бинарник, отдельная утилита не нужна.
// Блокировка в базе не даёт двум экземплярам сервиса мигрировать одновременно.
func Migrate(ctx context.Context, dsn string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close() //nolint:errcheck // соединение больше не нужно

	files, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	return migrate(ctx, db, files)
}

// MigrateSchema накатывает миграции из files в отдельную схему. Таблицы и журнал миграций лежат в ней,
// поэтому в одной базе могут жить несколько сервисов, каждый со своей историей миграций.
func MigrateSchema(ctx context.Context, dsn, schema string, files fs.FS) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	// search_path задаётся для каждого соединения: и goose, и миграции работают внутри схемы.
	cfg.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*cfg)
	defer db.Close() //nolint:errcheck // соединение больше не нужно

	// Имя схемы подставляется в текст запроса, поэтому экранируется как идентификатор.
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS `+pgx.Identifier{schema}.Sanitize()); err != nil {
		return fmt.Errorf("create schema %s: %w", schema, err)
	}
	return migrate(ctx, db, files)
}

func migrate(ctx context.Context, db *sql.DB, files fs.FS) error {
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("create migration lock: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, files, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("prepare migrations: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
