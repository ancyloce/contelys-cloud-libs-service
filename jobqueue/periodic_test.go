package jobqueue_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/riverqueue/river"

	"github.com/ancyloce/contelys-cloud-libs-service/jobqueue"
)

// cleanup is a periodic job of the tests, as Identity's removal of unverified accounts.
type cleanup struct{}

func (cleanup) Kind() string { return "cleanup" }

func TestAPeriodicJobRunsOnOneInstanceAtATime(t *testing.T) {
	pool := newDatabase(t)

	// A run outlasts the interval, so the schedule falls due while a run is still going.
	const interval, run = 100 * time.Millisecond, 400 * time.Millisecond
	var runs atomic.Int64
	var at peak
	work := river.WorkFunc(func(context.Context, *river.Job[cleanup]) error {
		defer at.enter()()
		time.Sleep(run)
		runs.Add(1)
		return nil
	})

	// Two instances of the service, each with the same periodic job.
	for range 2 {
		workers := river.NewWorkers()
		river.AddWorker(workers, work)
		start(t, pool, jobqueue.Options{
			Workers:  workers,
			Queues:   map[string]jobqueue.Queue{river.QueueDefault: {Concurrency: 4}},
			Periodic: []jobqueue.Periodic{{Every: interval, Args: cleanup{}}},
		})
	}

	eventually(t, 20*time.Second, func() bool { return runs.Load() >= 4 }, "the periodic job did not run again after each run")
	if got := at.most.Load(); got != 1 {
		t.Fatalf("%d runs of the periodic job at once, want 1", got)
	}
}

func TestAPeriodicJobThatFailsRunsAgainAtItsNextTime(t *testing.T) {
	pool := newDatabase(t)
	var runs atomic.Int64
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(context.Context, *river.Job[cleanup]) error {
		runs.Add(1)
		return errors.New("the database is read only")
	}))
	start(t, pool, jobqueue.Options{
		Workers:  workers,
		Queues:   map[string]jobqueue.Queue{river.QueueDefault: {Concurrency: 1}},
		Periodic: []jobqueue.Periodic{{Every: 100 * time.Millisecond, Args: cleanup{}}},
	})

	// River would retry a failed run after 1, 16 and 81 seconds, and insert no new run meanwhile.
	eventually(t, 10*time.Second, func() bool { return runs.Load() >= 4 }, "a failed run kept the periodic job from running again")
}
