// Package jobqueue runs a domain service's River jobs in its own database: it applies River's
// migrations through goose and returns River's client over the service's pool (ADR 0016,
// ADR 0021, ADR 0034).
package jobqueue

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/migration"
)

const (
	// versionTable is goose's table of River's migrations, apart from the service's own, so that
	// the two numberings never meet.
	versionTable = "jobqueue_goose_version"
	// migrationLock is the advisory lock under which an instance migrates River's tables, so that
	// instances that start together migrate them one at a time. Its value spells "jobqueue" in
	// ASCII.
	migrationLock int64 = 0x6a6f627175657565
)

// Migrate creates or updates River's tables in the service's database. Each of River's
// migrations is one goose migration of the same version, which River's own migrator applies, so
// that River's record of its versions stays true.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{Logger: slog.Default()})
	if err != nil {
		return fmt.Errorf("jobqueue: %w", err)
	}
	var migrations []*goose.Migration
	for _, version := range migrator.AllVersions() {
		apply := func(ctx context.Context, _ *sql.DB) error {
			_, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{TargetVersion: version.Version})
			return err
		}
		migrations = append(migrations, goose.NewGoMigration(int64(version.Version), &goose.GoFunc{RunDB: apply}, nil))
	}
	if err := migration.UpGo(ctx, pool, migrations, versionTable, migrationLock); err != nil {
		return fmt.Errorf("jobqueue: %w", err)
	}
	return nil
}

// Queue holds the settings of one queue on one instance of the service. A queue's job timeout is
// set by the worker of its jobs, in River's Timeout method.
type Queue struct {
	// Concurrency is the most jobs of the queue that one instance works at once.
	Concurrency int
}

// Periodic is a periodic job of the service. Only the instance that River elects leader inserts
// its runs, one when it becomes leader and one each time Every passes, so that a restart that
// comes sooner than Every does not keep the job from running. A run is not inserted while an
// earlier one waits or runs, so that two runs never overlap. A run that fails is not retried: the
// next run, Every later, is its retry, so that River's backoff, which reaches days, never holds up
// the schedule. A run on an instance that stopped holds it up until River rescues the run, after
// Options.RescueAfter.
type Periodic struct {
	// Every is the time between two runs.
	Every time.Duration
	// Args is the job that each run inserts.
	Args river.JobArgs
}

// oneAtATime inserts a run of a periodic job only if no run of the same kind is in a state before
// its end, and gives it one attempt.
func oneAtATime() *river.InsertOpts {
	return &river.InsertOpts{MaxAttempts: 1, UniqueOpts: river.UniqueOpts{ByState: []rivertype.JobState{
		rivertype.JobStateAvailable,
		rivertype.JobStatePending,
		rivertype.JobStateRetryable,
		rivertype.JobStateRunning,
		rivertype.JobStateScheduled,
	}}}
}

// Options configure the client of a service.
type Options struct {
	// Workers holds the service's workers, which it adds with river.AddWorker.
	Workers *river.Workers
	// Queues are the queues that the instance works, by name.
	Queues map[string]Queue
	// Periodic are the service's periodic jobs.
	Periodic []Periodic
	// RescueAfter is how long a job may run before River counts it as stuck, as after its
	// instance stopped, and rescues it: River schedules it for a retry, or discards it once its
	// attempts are spent. It holds for every queue, but River never rescues a job that has run
	// for less than its own timeout, River's default of one minute or the timeout that the job's
	// worker returns from its Timeout method, nor a job whose worker returns -1, no timeout. A
	// queue whose jobs need a longer timeout, as Notifications' send queue, therefore sets the
	// timeout in its worker and a longer rescue time here. River refuses a rescue time shorter
	// than its default timeout. Zero leaves River's default of one hour.
	RescueAfter time.Duration
	// Logger receives River's own log lines, which a service gets from telemetry.Setup. Nil sends
	// them to slog.Default.
	Logger *slog.Logger
}

// New returns River's client of the service's database, which inserts jobs, in the caller's
// transaction too with InsertTx, and works them once it is started.
func New(pool *pgxpool.Pool, opts Options) (*river.Client[pgx.Tx], error) {
	queues := make(map[string]river.QueueConfig, len(opts.Queues))
	for name, queue := range opts.Queues {
		queues[name] = river.QueueConfig{MaxWorkers: queue.Concurrency}
	}
	periodic := make([]*river.PeriodicJob, 0, len(opts.Periodic))
	for _, job := range opts.Periodic {
		periodic = append(periodic, river.NewPeriodicJob(river.PeriodicInterval(job.Every),
			func() (river.JobArgs, *river.InsertOpts) { return job.Args, oneAtATime() },
			&river.PeriodicJobOpts{RunOnStart: true}))
	}
	telemetry, err := newTelemetry()
	if err != nil {
		return nil, fmt.Errorf("jobqueue: %w", err)
	}
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Logger:               cmp.Or(opts.Logger, slog.Default()),
		Middleware:           []rivertype.Middleware{telemetry},
		PeriodicJobs:         periodic,
		Queues:               queues,
		RescueStuckJobsAfter: opts.RescueAfter,
		Workers:              opts.Workers,
	})
	if err != nil {
		return nil, fmt.Errorf("jobqueue: %w", err)
	}
	return client, nil
}
