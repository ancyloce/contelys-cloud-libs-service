package roles_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/identitytest"
	"github.com/ancyloce/contelys-cloud-libs-service/internal/problem"
	"github.com/ancyloce/contelys-cloud-libs-service/tokens"
)

// clock is the time of a test, shared by the fake Organization Access, which dates its answers by
// it, and by the client, which judges their validity by it.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock { return &clock{now: time.Date(2026, 11, 26, 9, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// held is what a principal holds on a scope, as Organization Access computes it.
type held struct {
	role             string
	membership       string
	authorityVersion int64
}

// question is one question that the fake received: who it was about, and how the caller showed it
// the principal, "internal" or "workload".
type question struct {
	token, principal, kind, scope string
}

// organizationAccess is a fake of Organization Access's role question, GET /internal/v1/role of
// its contract, and its scopes question, GET /internal/v1/scopes. It verifies the token of each
// question as Organization Access does, answers with what it was told each principal holds, and
// can answer 503, answer slowly or answer anything else, as Organization Access does when it is
// down, slow or broken.
type organizationAccess struct {
	server   *httptest.Server
	verifier *tokens.Verifier
	clock    *clock

	mu        sync.Mutex
	held      map[string]held
	scopes    map[string][]map[string]any
	validity  time.Duration
	down      bool
	delay     time.Duration
	raw       string
	questions []question
}

func newOrganizationAccess(t *testing.T, id *identitytest.Identity, clock *clock) *organizationAccess {
	t.Helper()
	oa := &organizationAccess{
		verifier: tokens.New(tokens.Options{KeySet: id.KeySet(), Client: http.DefaultClient, Deployable: "organization-access"}),
		clock:    clock,
		held:     map[string]held{},
		scopes:   map[string][]map[string]any{},
		validity: 60 * time.Second,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /organization-access/internal/v1/role", oa.getRole)
	mux.HandleFunc("GET /organization-access/internal/v1/scopes", oa.listScopes)
	oa.server = httptest.NewServer(mux)
	t.Cleanup(oa.server.Close)
	return oa
}

// address is the base address that a client takes: Edge's internal address followed by
// /organization-access.
func (oa *organizationAccess) address() string { return oa.server.URL + "/organization-access" }

func (oa *organizationAccess) give(principal, kind, scope string, h held) {
	oa.mu.Lock()
	defer oa.mu.Unlock()
	oa.held[principal+" "+kind+" "+scope] = h
}

func (oa *organizationAccess) list(principal string, scopes ...map[string]any) {
	oa.mu.Lock()
	defer oa.mu.Unlock()
	oa.scopes[principal] = scopes
}

func (oa *organizationAccess) set(change func(oa *organizationAccess)) {
	oa.mu.Lock()
	defer oa.mu.Unlock()
	change(oa)
}

func (oa *organizationAccess) asked() []question {
	oa.mu.Lock()
	defer oa.mu.Unlock()
	return append([]question(nil), oa.questions...)
}

// answer waits, records the question and tells whether to answer it, as a down Organization
// Access answers 503 and a broken one answers raw.
func (oa *organizationAccess) answer(w http.ResponseWriter, r *http.Request, q question) bool {
	oa.mu.Lock()
	oa.questions = append(oa.questions, q)
	delay, down, raw := oa.delay, oa.down, oa.raw
	oa.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return false
		}
	}
	if down {
		problem.Write(w, r, http.StatusServiceUnavailable, "unavailable", problem.UnavailableTitle, "")
		return false
	}
	if raw != "" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(raw))
		return false
	}
	return true
}

// kinds and identifier are what the contract allows as a scope's kind, and as a scope's or a
// principal's identifier.
var (
	kinds      = map[string]bool{"organization": true, "project": true, "site": true}
	identifier = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)
)

func bearer(r *http.Request) string {
	return strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
}

func (oa *organizationAccess) getRole(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	q := question{principal: query.Get("principal"), kind: query.Get("kind"), scope: query.Get("scope")}
	raw := bearer(r)
	caller, internal := oa.verifier.Internal(r.Context(), raw)
	_, workload := oa.verifier.Workload(r.Context(), raw, "getRole")
	switch {
	case internal == nil:
		q.token = "internal"
	case workload == nil:
		q.token = "workload"
	}
	if !oa.answer(w, r, q) {
		return
	}
	switch {
	case !kinds[q.kind] || !identifier.MatchString(q.scope) || q.principal != "" && !identifier.MatchString(q.principal):
		problem.Write(w, r, http.StatusBadRequest, "invalid_request", "The request does not match the contract.", "")
		return
	case q.token == "internal" && q.principal == "", q.token == "workload" && q.principal != "":
	case q.token == "":
		problem.Write(w, r, http.StatusUnauthorized, "token_invalid", "The token is invalid.", "")
		return
	default:
		problem.Write(w, r, http.StatusBadRequest, "invalid_request", "The request does not match the contract.", "")
		return
	}
	principal := q.principal
	if q.token == "internal" {
		principal = caller.Principal
	}
	oa.mu.Lock()
	h, ok := oa.held[principal+" "+q.kind+" "+q.scope]
	validUntil := oa.clock.Now().Add(oa.validity)
	oa.mu.Unlock()
	body := map[string]any{"role": "none", "validUntil": validUntil}
	if ok {
		body["role"] = h.role
		if h.membership != "" {
			body["membership"] = h.membership
			body["authorityVersion"] = h.authorityVersion
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func (oa *organizationAccess) listScopes(w http.ResponseWriter, r *http.Request) {
	caller, err := oa.verifier.Internal(r.Context(), bearer(r))
	q := question{kind: "scopes"}
	if err == nil {
		q.token, q.principal = "internal", caller.Principal
	}
	if !oa.answer(w, r, q) {
		return
	}
	if err != nil {
		problem.Write(w, r, http.StatusUnauthorized, "token_invalid", "The token is invalid.", "")
		return
	}
	oa.mu.Lock()
	scopes := oa.scopes[caller.Principal]
	validUntil := oa.clock.Now().Add(oa.validity)
	oa.mu.Unlock()
	if scopes == nil {
		scopes = []map[string]any{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"scopes": scopes, "validUntil": validUntil})
}
