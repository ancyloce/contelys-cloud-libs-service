package idempotency_test

import (
	"context"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/ancyloce/contelys-cloud-libs-service/idempotency"
	"github.com/ancyloce/contelys-cloud-libs-service/jobqueue"
	"github.com/ancyloce/contelys-cloud-libs-service/tokens"
)

// A domain service whose contract marks createOrganization as an operation that takes an
// Idempotency-Key. The interface layer checks the key inside the token's check. The use case,
// the transaction boundary, claims the key first in the transaction, and saves the contract's 201
// body with the organization, its events and its audit record. The composition root runs the job
// that removes old keys. In a domain service the use case reaches the pool through the ports that
// it declares.
func ExampleRequire() {
	var (
		pool     *pgxpool.Pool
		verifier *tokens.Verifier // tokens.New(…)
		mux      = http.NewServeMux()
	)
	_ = idempotency.Migrate(context.Background(), pool) // with the service's other migrations

	createOrganization := func(ctx context.Context, name string) ([]byte, error) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		claimed, err := idempotency.Claim(ctx, tx)
		if err != nil {
			return nil, err
		}
		// … the organization, its first Owner, the audit record and outbox.Write, in tx; then the
		// contract's 201 body of the organization, rendered before the commit, which stores it.
		answer := []byte(`{"id":"…","name":"` + name + `"}`)
		if err := claimed.Save(ctx, idempotency.Response{Status: http.StatusCreated, ContentType: "application/json", Body: answer}); err != nil {
			return nil, err
		}
		return answer, tx.Commit(ctx)
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		answer, err := createOrganization(r.Context(), "Acme")
		switch {
		case errors.Is(err, idempotency.ErrInProgress):
			idempotency.InProgress(w, r)
		case err != nil:
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write(answer)
		}
	})
	mux.Handle("POST /api/organization-access/v1/organizations",
		verifier.RequireInternal(idempotency.Require(pool, "createOrganization", handler)))

	workers := river.NewWorkers()
	_, _ = jobqueue.New(pool, jobqueue.Options{
		Workers:  workers,
		Queues:   map[string]jobqueue.Queue{river.QueueDefault: {Concurrency: 1}},
		Periodic: []jobqueue.Periodic{idempotency.Purging(workers, pool)},
	})
}
