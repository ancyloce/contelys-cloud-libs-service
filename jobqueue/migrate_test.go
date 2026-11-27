package jobqueue_test

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/pgtest"
	"github.com/ancyloce/contelys-cloud-libs-service/jobqueue"
)

// greeting is a job of the tests, as a service defines its own.
type greeting struct {
	Name string `json:"name"`
}

func (greeting) Kind() string { return "greeting" }

// riverIsCurrent fails the test unless River's own migrator finds every one of its migrations
// applied to the database of pool.
func riverIsCurrent(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := migrator.Validate(context.Background(), nil)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !result.OK {
		t.Fatalf("River's migrations are not all applied: %v", result.Messages)
	}
}

func TestMigrateLetsAClientWorkAJob(t *testing.T) {
	pool := newDatabase(t)
	riverIsCurrent(t, pool)

	worked := make(chan string, 1)
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(_ context.Context, job *river.Job[greeting]) error {
		worked <- job.Args.Name
		return nil
	}))
	client := start(t, pool, jobqueue.Options{
		Workers: workers,
		Queues:  map[string]jobqueue.Queue{river.QueueDefault: {Concurrency: 1}},
	})
	if _, err := client.Insert(context.Background(), greeting{Name: "Ada"}, nil); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	select {
	case name := <-worked:
		if name != "Ada" {
			t.Fatalf("worked the job of %q, want Ada", name)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the job was not worked")
	}
}

func TestMigrateLeavesTheServicesOwnGoMigrations(t *testing.T) {
	// A service that writes a migration in Go registers it with goose, under a version of its own.
	var ran atomic.Bool
	goose.AddNamedMigrationNoTxContext("20261126120000_service.go", func(context.Context, *sql.DB) error {
		ran.Store(true)
		return nil
	}, nil)
	t.Cleanup(goose.ResetGlobalMigrations)

	newDatabase(t)
	if ran.Load() {
		t.Fatal("Migrate applied the service's own migration")
	}
}

func TestMigrateRunsAgainAndOnInstancesThatStartTogether(t *testing.T) {
	pool := pgtest.Database(t)
	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range errs {
		wg.Go(func() { errs[i] = jobqueue.Migrate(context.Background(), pool) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("instance %d: Migrate: %v", i, err)
		}
	}
	if err := jobqueue.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("Migrate again: %v", err)
	}
	riverIsCurrent(t, pool)
}
