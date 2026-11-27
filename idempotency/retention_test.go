package idempotency_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/riverqueue/river"

	"github.com/ancyloce/contelys-cloud-libs-service/idempotency"
	"github.com/ancyloce/contelys-cloud-libs-service/jobqueue"
)

func TestAKeyIsKeptUntilTheRemovalOfTheKeysOlderThanTheRetention(t *testing.T) {
	if idempotency.Retention != 24*time.Hour {
		t.Errorf("Retention = %s, want the 24 hours of ADR 0062", idempotency.Retention)
	}
	s := newService(t)
	ctx := context.Background()
	body := `{"name":"Acme"}`
	s.create(t, member, key, body)

	if err := idempotency.Purge(ctx, s.pool, idempotency.Retention); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	s.create(t, member, key, body)
	if got := s.things(t); got != 1 {
		t.Errorf("after removing the keys older than the retention, %d things, want 1: the key is kept", got)
	}
	if err := idempotency.Purge(ctx, s.pool, 0); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if got := s.create(t, member, key, body); got.Code != http.StatusCreated || s.things(t) != 2 {
		t.Errorf("after removing the key, the request = %d with %d things, want 201 and a second thing", got.Code, s.things(t))
	}
}

func TestThePeriodicJobRemovesTheKeysOlderThanTheRetention(t *testing.T) {
	s := newService(t)
	ctx := context.Background()
	body := `{"name":"Acme"}`
	s.create(t, member, key, body)
	if err := jobqueue.Migrate(ctx, s.pool); err != nil {
		t.Fatalf("jobqueue.Migrate: %v", err)
	}
	workers := river.NewWorkers()
	// The job removes the keys of any age, so that the key need not wait a day.
	purging := idempotency.PurgingAfter(workers, s.pool, 0)
	client, err := jobqueue.New(s.pool, jobqueue.Options{
		Workers:  workers,
		Queues:   map[string]jobqueue.Queue{river.QueueDefault: {Concurrency: 1}},
		Periodic: []jobqueue.Periodic{purging},
	})
	if err != nil {
		t.Fatalf("jobqueue.New: %v", err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = client.Stop(context.Background()) })

	// Each repeat is answered from the store until the job removes the key; the next one creates.
	deadline := time.Now().Add(15 * time.Second)
	for s.things(t) == 1 && time.Now().Before(deadline) {
		s.create(t, member, key, body)
		time.Sleep(100 * time.Millisecond)
	}
	if got := s.things(t); got != 2 {
		t.Errorf("%d things after the periodic job, want 2: it never removed the key", got)
	}
}
