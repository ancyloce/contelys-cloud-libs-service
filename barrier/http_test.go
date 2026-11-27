package barrier_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ancyloce/contelys-cloud-libs-service/barrier"
	"github.com/ancyloce/contelys-cloud-libs-service/internal/identitytest"
	"github.com/ancyloce/contelys-cloud-libs-service/tokens"
)

// service is a registered domain service, Projects, that serves the shared operation.
type service struct {
	pool    *pgxpool.Pool
	handler http.Handler
	id      *identitytest.Identity
	key     identitytest.Key
}

func newService(t *testing.T) *service {
	t.Helper()
	pool := newDatabase(t)
	id := identitytest.New(t)
	key := identitytest.NewKey(t, "workload-2026-11")
	id.Publish(key)
	verifier := tokens.New(tokens.Options{KeySet: id.KeySet(), Client: http.DefaultClient, Deployable: "projects"})
	return &service{pool: pool, handler: barrier.Handler(pool, verifier), id: id, key: key}
}

// below checks a write of the principal at version 3, as a test of what is installed.
func (s *service) below(t *testing.T) error {
	return check(t, s.pool, barrier.AuthorityVersion{Kind: barrier.Principal, ID: principal, Version: 3})
}

// token is a workload token of caller for Projects that permits the shared operation.
func (s *service) token(t *testing.T, caller string) string {
	return identitytest.Sign(t, s.key, "workload+jwt",
		identitytest.Workload(caller, "projects", "installMinimumAuthorityVersion"))
}

// put sends the shared operation for kind and id with body and token.
func (s *service) put(t *testing.T, token, kind, id, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPut, "/internal/v1/minimum-authority-versions/"+kind+"/"+id, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	s.handler.ServeHTTP(response, request)
	return response
}

func TestIdentityInstallsTheMinimumOfAPrincipalAndARepeatOrALowerOneChangesNothing(t *testing.T) {
	s := newService(t)
	identity := s.token(t, "identity")

	if got := s.put(t, identity, "principal", principal, `{"minimumAuthorityVersion":4}`); got.Code != http.StatusNoContent {
		t.Fatalf("PUT of minimum 4 = %d %s, want 204", got.Code, got.Body)
	}
	if err := s.below(t); !errors.Is(err, barrier.ErrRevoked) {
		t.Errorf("a write at version 3 after minimum 4 = %v, want ErrRevoked", err)
	}
	for _, body := range []string{`{"minimumAuthorityVersion":4}`, `{"minimumAuthorityVersion":2}`} {
		if got := s.put(t, identity, "principal", principal, body); got.Code != http.StatusNoContent {
			t.Errorf("PUT of %s = %d %s, want the same 204", body, got.Code, got.Body)
		}
	}
	if err := s.below(t); !errors.Is(err, barrier.ErrRevoked) {
		t.Errorf("a write at version 3 after a repeat and a lower minimum = %v, want ErrRevoked", err)
	}
}

// refused checks that response is the problem body of the shared operation with status and code.
func refused(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var body struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
		Code   string `json:"code"`
	}
	_ = json.Unmarshal(response.Body.Bytes(), &body)
	if response.Code != status || body.Status != status || body.Code != code || body.Type != "about:blank" ||
		body.Title == "" || response.Header().Get("Content-Type") != "application/problem+json" {
		t.Errorf("answer = %d %s, want %d with the code %s", response.Code, response.Body, status, code)
	}
}

func TestAMinimumIsAcceptedOnlyFromItsAuthority(t *testing.T) {
	s := newService(t)
	identity, organizationAccess := s.token(t, "identity"), s.token(t, "organization-access")

	if got := s.put(t, organizationAccess, "membership", membership, `{"minimumAuthorityVersion":4}`); got.Code != http.StatusNoContent {
		t.Errorf("Organization Access's PUT of a membership's minimum = %d %s, want 204", got.Code, got.Body)
	}
	refused(t, s.put(t, organizationAccess, "principal", principal, `{"minimumAuthorityVersion":4}`),
		http.StatusForbidden, "kind_not_permitted")
	refused(t, s.put(t, identity, "membership", membership, `{"minimumAuthorityVersion":9}`),
		http.StatusForbidden, "kind_not_permitted")
	if err := s.below(t); err != nil {
		t.Errorf("a write of the principal after a refused installation = %v, want nil", err)
	}
}

func TestARequestThatBreaksTheContractIsRefusedAndInstallsNothing(t *testing.T) {
	s := newService(t)
	identity := s.token(t, "identity")
	other := identitytest.Sign(t, s.key, "workload+jwt", identitytest.Workload("identity", "projects", "lookUpPrincipals"))
	valid := `{"minimumAuthorityVersion":4}`

	cases := []struct {
		name, token, kind, id, body, code string
		status                            int
	}{
		{"without a token", "", "principal", principal, valid, "token_invalid", http.StatusUnauthorized},
		{"with a token for another operation", other, "principal", principal, valid, "operation_not_permitted", http.StatusForbidden},
		{"of an unknown kind", identity, "session", principal, valid, "invalid_request", http.StatusBadRequest},
		{"for an id the contract does not allow", identity, "principal", "bad_id", valid, "invalid_request", http.StatusBadRequest},
		{"for an id that is too long", identity, "principal", strings.Repeat("a", 65), valid, "invalid_request", http.StatusBadRequest},
		{"without a minimum", identity, "principal", principal, `{}`, "invalid_request", http.StatusBadRequest},
		{"with a minimum of 0", identity, "principal", principal, `{"minimumAuthorityVersion":0}`, "invalid_request", http.StatusBadRequest},
		{"with another member", identity, "principal", principal, `{"minimumAuthorityVersion":4,"reason":"x"}`, "invalid_request", http.StatusBadRequest},
		{"that is not JSON", identity, "principal", principal, `minimum 4`, "invalid_request", http.StatusBadRequest},
		{"with the member's name in another case", identity, "principal", principal, `{"MINIMUMAUTHORITYVERSION":4}`, "invalid_request", http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			refused(t, s.put(t, c.token, c.kind, c.id, c.body), c.status, c.code)
		})
	}
	t.Run("in another media type", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodPut, "/internal/v1/minimum-authority-versions/principal/"+principal, strings.NewReader(valid))
		request.Header.Set("Content-Type", "text/plain")
		request.Header.Set("Authorization", "Bearer "+identity)
		response := httptest.NewRecorder()
		s.handler.ServeHTTP(response, request)
		refused(t, response, http.StatusBadRequest, "invalid_request")
	})
	if err := s.below(t); err != nil {
		t.Errorf("a write of the principal after the refused requests = %v, want nil", err)
	}
}

func TestAnInstallationThatCannotCommitIsAnsweredUnavailable(t *testing.T) {
	s := newService(t)
	identity := s.token(t, "identity")
	// The key set is cached before the database goes away.
	if got := s.put(t, identity, "principal", principal, `{"minimumAuthorityVersion":1}`); got.Code != http.StatusNoContent {
		t.Fatalf("first PUT = %d %s, want 204", got.Code, got.Body)
	}
	s.pool.Close()

	refused(t, s.put(t, identity, "principal", principal, `{"minimumAuthorityVersion":4}`),
		http.StatusServiceUnavailable, "unavailable")
}

func TestOrganizationAccessInstallsAMembershipsMinimumWithATokenThatItsHolderRequested(t *testing.T) {
	s := newService(t)
	s.id.Register("organization-access", "s3cret")
	s.id.Grant(s.key, "organization-access", "projects", "installMinimumAuthorityVersion")
	secretFile := filepath.Join(t.TempDir(), "workload-secret")
	if err := os.WriteFile(secretFile, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	holder, err := tokens.NewHolder(tokens.HolderOptions{
		WorkloadTokens: s.id.WorkloadTokens(), Client: http.DefaultClient,
		Deployable: "organization-access", SecretFile: secretFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	projects := httptest.NewServer(s.handler)
	t.Cleanup(projects.Close)

	request, err := http.NewRequest(http.MethodPut, projects.URL+"/internal/v1/minimum-authority-versions/membership/"+membership,
		strings.NewReader(`{"minimumAuthorityVersion":4}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Transport: holder.Transport("projects", nil)}).Do(request)
	if err != nil {
		t.Fatalf("the installation was not sent: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("the installation answered %d, want 204", response.StatusCode)
	}
	write := barrier.AuthorityVersion{Kind: barrier.Membership, ID: membership, Version: 3}
	if err := check(t, s.pool, write); !errors.Is(err, barrier.ErrRevoked) {
		t.Errorf("a write of the membership at version 3 after the installation = %v, want ErrRevoked", err)
	}
}
