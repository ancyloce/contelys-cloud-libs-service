// Package migration applies a library package's embedded goose migrations to a service's
// database, under a version table and an advisory lock of the package's own, so that the
// library's tables and the service's own migrations never share a numbering, and instances that
// start together migrate one at a time.
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
	provider, err := goose.NewProvider("", db, files, goose.WithStore(store), goose.WithSessionLocker(locker))
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}
