package idempotency_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ancyloce/contelys-cloud-libs-service/idempotency"
)

func TestARepeatWithTheSameKeyAndBodyGetsTheStoredResponseAndCreatesNothing(t *testing.T) {
	s := newService(t)
	body := `{"name":"Acme"}`

	first := s.create(t, member, key, body)
	if first.Code != http.StatusCreated {
		t.Fatalf("the first request = %d %s, want 201", first.Code, first.Body.String())
	}
	repeat := s.create(t, member, key, body)
	if repeat.Code != http.StatusCreated || repeat.Body.String() != first.Body.String() {
		t.Errorf("the repeat = %d %s, want the stored 201 %s", repeat.Code, repeat.Body.String(), first.Body.String())
	}
	if got := repeat.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("the repeat's Content-Type = %q, want the stored application/json", got)
	}
	if got := s.things(t); got != 1 {
		t.Errorf("%d things, want 1", got)
	}
}

func TestTheSameKeyWithAnotherBodyIsRefused(t *testing.T) {
	s := newService(t)
	s.create(t, member, key, `{"name":"Acme"}`)

	refusal(t, s.create(t, member, key, `{"name":"Globex"}`), http.StatusUnprocessableEntity, "idempotency_key_reused", "Globex")
	if got := s.things(t); got != 1 {
		t.Errorf("%d things, want 1", got)
	}
}

func TestARequestWithoutAKeyOrWithAKeyThatIsNoUUIDIsRefused(t *testing.T) {
	s := newService(t)

	refusal(t, s.create(t, member, "", `{"name":"Acme"}`), http.StatusBadRequest, "idempotency_key_missing", "Acme")
	refusal(t, s.create(t, member, "not-a-uuid", `{"name":"Acme"}`), http.StatusBadRequest, "invalid_request", "Acme")
	if got := s.things(t); got != 0 {
		t.Errorf("%d things, want none", got)
	}
}

// held starts the request of create in the background, holds it at stage until the returned
// release is called, and returns the channel of its answer.
func (s *service) held(t *testing.T, stage, body string) (release func(), answer <-chan *httptest.ResponseRecorder) {
	t.Helper()
	reached, resume := make(chan struct{}), make(chan struct{})
	s.pause = func(at string) {
		if at == stage {
			close(reached)
			<-resume
		}
	}
	answered := make(chan *httptest.ResponseRecorder, 1)
	go func() { answered <- s.create(t, member, key, body) }()
	<-reached
	return func() { close(resume) }, answered
}

func TestTheSameKeyWhileTheFirstRequestStoresItsAnswerIsRefused(t *testing.T) {
	s := newService(t)
	release, first := s.held(t, "commit", `{"name":"Acme"}`)

	s.pause = nil
	refusal(t, s.create(t, member, key, `{"name":"Acme"}`), http.StatusConflict, "idempotency_key_in_progress", "Acme")
	release()
	stored := <-first
	if stored.Code != http.StatusCreated {
		t.Fatalf("the first request = %d %s, want 201", stored.Code, stored.Body.String())
	}
	if repeat := s.create(t, member, key, `{"name":"Acme"}`); repeat.Code != http.StatusCreated || repeat.Body.String() != stored.Body.String() {
		t.Errorf("the repeat once the first has finished = %d %s, want its 201", repeat.Code, repeat.Body.String())
	}
	if got := s.things(t); got != 1 {
		t.Errorf("%d things, want 1", got)
	}
}

func TestARequestLetThroughBeforeTheFirstFinishedIsRefusedWhenItClaimsTheKey(t *testing.T) {
	s := newService(t)
	release, late := s.held(t, "claim", `{"name":"Acme"}`)

	s.pause = nil
	early := s.create(t, member, key, `{"name":"Acme"}`)
	if early.Code != http.StatusCreated {
		t.Fatalf("the request that claimed first = %d %s, want 201", early.Code, early.Body.String())
	}
	release()
	refusal(t, <-late, http.StatusConflict, "idempotency_key_in_progress", "Acme")
	if got := s.things(t); got != 1 {
		t.Errorf("%d things, want 1", got)
	}
	if repeat := s.create(t, member, key, `{"name":"Acme"}`); repeat.Body.String() != early.Body.String() {
		t.Errorf("the late request's repeat = %d %s, want the stored %s", repeat.Code, repeat.Body.String(), early.Body.String())
	}
}

func TestAKeyIsScopedToTheCallerAndTheOperation(t *testing.T) {
	s := newService(t)
	body := `{"name":"Acme"}`
	s.create(t, member, key, body)

	if got := s.create(t, "01JDZ9A1B2C3D4E5F6G7H8J9KA", key, body); got.Code != http.StatusCreated {
		t.Errorf("another caller's request with the key = %d %s, want 201", got.Code, got.Body.String())
	}
	if got := s.post(t, s.operation("createWidget"), member, key, body); got.Code != http.StatusCreated {
		t.Errorf("the caller's request with the key on another operation = %d %s, want 201", got.Code, got.Body.String())
	}
	if got := s.things(t); got != 3 {
		t.Errorf("%d things, want 3", got)
	}
}

func TestARequestThatFailsStoresNothing(t *testing.T) {
	s := newService(t)

	if got := s.create(t, member, key, `{"name":""}`); got.Code != http.StatusBadRequest {
		t.Fatalf("a request that the use case refuses = %d, want 400", got.Code)
	}
	if got := s.create(t, member, key, `{"name":"Acme"}`); got.Code != http.StatusCreated {
		t.Errorf("the key with another body after a failure = %d %s, want 201: nothing was stored", got.Code, got.Body.String())
	}
}

func TestClaimNeedsARequestThatRequireLetThrough(t *testing.T) {
	s := newService(t)
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := idempotency.Claim(ctx, tx); err == nil {
		t.Error("Claim outside a request that Require let through succeeded, want an error")
	}
}

func TestRequireNeedsACallerThatTheTokenAdmitted(t *testing.T) {
	s := newService(t)
	unverified := idempotency.Require(s.pool, "createThing", http.HandlerFunc(s.createThing))

	refusal(t, s.post(t, unverified, member, key, `{"name":"Acme"}`), http.StatusUnauthorized, "token_invalid", "Acme")
}

func TestARepeatDuringTheFirstRequestIsRefusedBeforeItsOperationRuns(t *testing.T) {
	s := newService(t)
	// A rule of the service's own on what it creates, as one pending invitation for an address.
	if _, err := s.pool.Exec(context.Background(), "CREATE UNIQUE INDEX things_name ON things (name)"); err != nil {
		t.Fatal(err)
	}
	release, first := s.held(t, "save", `{"name":"Acme"}`)

	s.pause = nil
	repeat := make(chan *httptest.ResponseRecorder, 1)
	go func() { repeat <- s.create(t, member, key, `{"name":"Acme"}`) }()
	select {
	case got := <-repeat:
		refusal(t, got, http.StatusConflict, "idempotency_key_in_progress", "Acme")
	case <-time.After(3 * time.Second):
		t.Error("the repeat waited on the first request's thing, want 409 idempotency_key_in_progress at once")
		release()
		<-repeat
		<-first
		return
	}
	release()
	if got := <-first; got.Code != http.StatusCreated {
		t.Errorf("the first request = %d %s, want 201", got.Code, got.Body.String())
	}
}

func TestTheSameKeyAndBodyForAnotherPathIsRefused(t *testing.T) {
	s := newService(t)
	body := `{"name":"Acme"}`
	s.create(t, member, key, body)

	refusal(t, s.postTo(t, s.handler, "/organizations/org-2/things", member, key, body), http.StatusUnprocessableEntity, "idempotency_key_reused", "Acme")
	if got := s.things(t); got != 1 {
		t.Errorf("%d things, want 1", got)
	}
}

func TestABodyOverTheLimitIsRefused(t *testing.T) {
	s := newService(t)
	body := `{"name":"` + strings.Repeat("a", 1<<20) + `"}`

	refusal(t, s.create(t, member, key, body), http.StatusBadRequest, "invalid_request", strings.Repeat("a", 64))
	if got := s.things(t); got != 0 {
		t.Errorf("%d things, want none", got)
	}
}
