package idempotency_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ancyloce/contelys-cloud-libs-service/idempotency"
	"github.com/ancyloce/contelys-cloud-libs-service/internal/identitytest"
	"github.com/ancyloce/contelys-cloud-libs-service/internal/pgtest"
	"github.com/ancyloce/contelys-cloud-libs-service/tokens"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

const (
	member = "01JDZ8Q3V7XK2M4N6P8R0T2W4Y"
	key    = "5f0c6a2e-8d1b-4c3a-9e7f-2b4d6c8a0e1f"
)

// service is a domain service with one creating operation, createThing. Its use case claims the
// key first in its transaction, creates a thing in the service's own table and saves the answer
// in the transaction that commits it. A test can hold the use case at each of these steps.
type service struct {
	pool     *pgxpool.Pool
	id       *identitytest.Identity
	key      identitytest.Key
	verifier *tokens.Verifier
	handler  http.Handler
	// pause, when set, is called with "claim" before the claim, "save" before the save and
	// "commit" after it.
	pause func(stage string)
}

func newService(t *testing.T) *service {
	t.Helper()
	ctx := context.Background()
	s := &service{pool: pgtest.Database(t), id: identitytest.New(t), key: identitytest.NewKey(t, "2026-11")}
	s.id.Publish(s.key)
	if err := idempotency.Migrate(ctx, s.pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := s.pool.Exec(ctx, "CREATE TABLE things (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	s.verifier = tokens.New(tokens.Options{KeySet: s.id.KeySet(), Client: http.DefaultClient, Deployable: "projects"})
	s.handler = s.operation("createThing")
	return s
}

// operation is the handler of the use case under the operationId operation.
func (s *service) operation(operation string) http.Handler {
	return s.verifier.RequireInternal(idempotency.Require(s.pool, operation, http.HandlerFunc(s.createThing)))
}

func (s *service) createThing(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var thing struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&thing); err != nil || thing.Name == "" {
		w.WriteHeader(http.StatusBadRequest) // refused by the use case, which commits nothing
		return
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if s.pause != nil {
		s.pause("claim")
	}
	claimed, err := idempotency.Claim(ctx, tx)
	if errors.Is(err, idempotency.ErrInProgress) {
		idempotency.InProgress(w, r)
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	var id int64
	if err := tx.QueryRow(ctx, "INSERT INTO things (name) VALUES ($1) RETURNING id", thing.Name).Scan(&id); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	answer := []byte(fmt.Sprintf(`{"id":%d,"name":%q}`, id, thing.Name))
	if s.pause != nil {
		s.pause("save")
	}
	if err := claimed.Save(ctx, idempotency.Response{Status: http.StatusCreated, ContentType: "application/json", Body: answer}); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if s.pause != nil {
		s.pause("commit")
	}
	if err := tx.Commit(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(answer)
}

// create sends POST /things as principal with the body and the key, if any, and returns the
// answer.
func (s *service) create(t *testing.T, principal, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	return s.post(t, s.handler, principal, key, body)
}

// post sends the request of create to handler.
func (s *service) post(t *testing.T, handler http.Handler, principal, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	return s.postTo(t, handler, "/things", principal, key, body)
}

// postTo sends the request of create to handler, at path.
func (s *service) postTo(t *testing.T, handler http.Handler, path, principal, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	request.Header.Set("Authorization", "Bearer "+identitytest.Sign(t, s.key, "internal+jwt", identitytest.Internal(principal)))
	request.Header.Set("X-Request-Id", "req-7f3a")
	request.Header.Set("Content-Type", "application/json")
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// things is how many things the service has created.
func (s *service) things(t *testing.T) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(), "SELECT count(*) FROM things").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// problem is the shared contracts' problem body, openapi/shared/problem.yaml.
type problem struct {
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Detail        string `json:"detail"`
	Code          string `json:"code"`
	RequestID     string `json:"requestId"`
	CorrelationID string `json:"correlationId"`
}

// refusal checks that response is the one problem body with status and code, and that it
// carries nothing of the request's body.
func refusal(t *testing.T, response *httptest.ResponseRecorder, status int, code, body string) {
	t.Helper()
	if response.Code != status {
		t.Errorf("status = %d, want %d", response.Code, status)
	}
	if got := response.Header().Get("Content-Type"); got != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", got)
	}
	var answer problem
	if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil {
		t.Fatalf("body %q: %v", response.Body.String(), err)
	}
	if answer.Status != status || answer.Code != code || answer.Type != "about:blank" || answer.Title == "" || answer.RequestID != "req-7f3a" {
		t.Errorf("problem = %+v, want status %d, code %s and the request ID", answer, status, code)
	}
	if body != "" && bytes.Contains(response.Body.Bytes(), []byte(body)) {
		t.Errorf("the problem body %q carries the request's body", response.Body.String())
	}
}
