package barrier_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ancyloce/contelys-cloud-libs-service/barrier"
	"github.com/ancyloce/contelys-cloud-libs-service/internal/pgtest"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

const (
	principal  = "01JDZ8Q3V7XK2M4N6P8R0T2W4Y"
	membership = "m-7b2c9e41"
)

// newDatabase creates a database of the test's own, as a registered domain service's database,
// and applies the barrier's migrations to it.
func newDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := pgtest.Database(t)
	if err := barrier.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return pool
}

// inTx runs f in a transaction of its own and commits it when f succeeds, as a write does. It
// returns its errors rather than failing the test, so that a goroutine may call it.
func inTx(t *testing.T, pool *pgxpool.Pool, f func(pgx.Tx) error) error {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// install installs minimum for the kind and id in a transaction of its own.
func install(t *testing.T, pool *pgxpool.Pool, kind barrier.Kind, id string, minimum int64) {
	t.Helper()
	err := inTx(t, pool, func(tx pgx.Tx) error { return barrier.Install(context.Background(), tx, kind, id, minimum) })
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
}

// check checks the authority versions of one write in a transaction of its own.
func check(t *testing.T, pool *pgxpool.Pool, versions ...barrier.AuthorityVersion) error {
	t.Helper()
	return inTx(t, pool, func(tx pgx.Tx) error { return barrier.Check(context.Background(), tx, versions...) })
}
