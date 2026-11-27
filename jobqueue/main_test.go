package jobqueue_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/pgtest"
	"github.com/ancyloce/contelys-cloud-libs-service/jobqueue"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

// newDatabase creates a database of the test's own, as a domain service's database, and applies
// River's migrations to it.
func newDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := pgtest.Database(t)
	if err := jobqueue.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return pool
}

// start returns a client of the database with opts, as one instance of a service runs it, and
// stops it when the test ends.
func start(t *testing.T, pool *pgxpool.Pool, opts jobqueue.Options) *river.Client[pgx.Tx] {
	t.Helper()
	client, err := jobqueue.New(pool, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := client.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return client
}

// peak counts the jobs that run at once and keeps the most it has seen.
type peak struct{ running, most atomic.Int64 }

// enter counts a job that starts and returns the function that counts its end.
func (p *peak) enter() (leave func()) {
	now := p.running.Add(1)
	for {
		seen := p.most.Load()
		if now <= seen || p.most.CompareAndSwap(seen, now) {
			break
		}
	}
	return func() { p.running.Add(-1) }
}

// eventually waits until done holds, for at most timeout, and fails the test with what otherwise.
func eventually(t *testing.T, timeout time.Duration, done func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
