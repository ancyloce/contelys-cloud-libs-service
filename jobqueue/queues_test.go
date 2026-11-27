package jobqueue_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/riverqueue/river"

	"github.com/ancyloce/contelys-cloud-libs-service/jobqueue"
)

// send is a job of a queue of its own, as Notifications' send job.
type send struct {
	NotificationDelivery int `json:"notification_delivery"`
}

func (send) Kind() string { return "send" }

func (send) InsertOpts() river.InsertOpts { return river.InsertOpts{Queue: "send"} }

func TestAQueueWorksNoMoreJobsAtOnceThanItsConcurrency(t *testing.T) {
	pool := newDatabase(t)
	var done atomic.Int64
	var at peak
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(context.Context, *river.Job[send]) error {
		defer at.enter()()
		time.Sleep(300 * time.Millisecond)
		done.Add(1)
		return nil
	}))
	client := start(t, pool, jobqueue.Options{
		Workers: workers,
		Queues:  map[string]jobqueue.Queue{"send": {Concurrency: 2}},
	})

	const sends = 6
	for delivery := range sends {
		if _, err := client.Insert(context.Background(), send{NotificationDelivery: delivery}, nil); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
	eventually(t, 20*time.Second, func() bool { return done.Load() == sends }, "the queue did not work every job")
	if got := at.most.Load(); got != 2 {
		t.Fatalf("%d jobs of the queue at once, want its concurrency, 2", got)
	}
}

func TestTheRescueTimeIsTheServicesOwn(t *testing.T) {
	pool := newDatabase(t)
	queues := map[string]jobqueue.Queue{"send": {Concurrency: 1}}
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(context.Context, *river.Job[send]) error { return nil }))

	if _, err := jobqueue.New(pool, jobqueue.Options{Workers: workers, Queues: queues, RescueAfter: 5 * time.Minute}); err != nil {
		t.Fatalf("New with a rescue time of 5 minutes: %v", err)
	}
	// River refuses a rescue time shorter than its default job timeout of one minute, so a
	// rescue time below it shows that the client received the service's own.
	if _, err := jobqueue.New(pool, jobqueue.Options{Workers: workers, Queues: queues, RescueAfter: 30 * time.Second}); err == nil {
		t.Fatal("New accepted a rescue time shorter than the job timeout")
	}
}
