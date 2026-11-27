package idempotency

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ancyloce/contelys-cloud-libs-service/jobqueue"
)

const (
	// Retention is how long the store keeps a key: 24 hours (ADR 0062). The job of Purging
	// removes a key within purgeEvery after.
	Retention = 24 * time.Hour
	// purgeEvery is the time between two runs of the job of Purging.
	purgeEvery = time.Hour
)

// Purge removes every key stored more than age ago, by the database's clock, which also dates
// each key. After it, a request with a removed key is a first request again.
func Purge(ctx context.Context, pool *pgxpool.Pool, age time.Duration) error {
	_, err := pool.Exec(ctx, "DELETE FROM idempotency_keys WHERE created_at < now() - make_interval(secs => $1)", age.Seconds())
	if err != nil {
		return fmt.Errorf("idempotency: %w", err)
	}
	return nil
}

// purgeArgs is the job that removes the keys older than Retention.
type purgeArgs struct{}

func (purgeArgs) Kind() string { return "idempotency_purge" }

// Purging adds the worker of the job that removes the keys older than Retention to workers, and
// returns the job as a periodic job of every purgeEvery, which the service lists in
// jobqueue.Options.Periodic. It runs in River's default queue, which the service lists in
// jobqueue.Options.Queues.
func Purging(workers *river.Workers, pool *pgxpool.Pool) jobqueue.Periodic {
	return purging(workers, pool, Retention)
}

// purging is Purging for the keys older than age.
func purging(workers *river.Workers, pool *pgxpool.Pool, age time.Duration) jobqueue.Periodic {
	river.AddWorker(workers, &purger{pool: pool, age: age})
	return jobqueue.Periodic{Every: purgeEvery, Args: purgeArgs{}}
}

// purger is the worker of purgeArgs.
type purger struct {
	river.WorkerDefaults[purgeArgs]
	pool *pgxpool.Pool
	age  time.Duration
}

func (p *purger) Work(ctx context.Context, _ *river.Job[purgeArgs]) error {
	return Purge(ctx, p.pool, p.age)
}
