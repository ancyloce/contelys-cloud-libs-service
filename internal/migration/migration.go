// Package migration applies a library package's goose migrations to a service's database, under
// a version table and an advisory lock of the package's own, so that the library's tables and the
// service's own migrations never share a numbering, and instances that start together migrate one
// at a time.
package migration

import (
	"context"
	"io/fs"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"
)

// Up applies the migrations in the directory migrations of embedded to the database of pool that
// versionTable does not record as applied, under the advisory lock lockID.
func Up(ctx context.Context, pool *pgxpool.Pool, embedded fs.FS, versionTable string, lockID int64) error {
	files, err := fs.Sub(embedded, "migrations")
	if err != nil {
		return err
	}
	return up(ctx, pool, files, versionTable, lockID)
}

// UpGo applies migrations written in Go, which goose.NewGoMigration makes, to the database of pool
// that versionTable does not record as applied, under the advisory lock lockID.
func UpGo(ctx context.Context, pool *pgxpool.Pool, migrations []*goose.Migration, versionTable string, lockID int64) error {
	return up(ctx, pool, nil, versionTable, lockID, goose.WithGoMigrations(migrations...))
}

func up(ctx context.Context, pool *pgxpool.Pool, files fs.FS, versionTable string, lockID int64, opts ...goose.ProviderOption) error {
	store, err := database.NewStore(database.DialectPostgres, versionTable)
	if err != nil {
		return err
	}
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(lockID))
	if err != nil {
		return err
	}
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	// The Go migrations that the service registers with goose are the service's own, never the
	// package's.
	opts = append(opts, goose.WithStore(store), goose.WithSessionLocker(locker), goose.WithDisableGlobalRegistry(true))
	provider, err := goose.NewProvider("", db, files, opts...)
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}
