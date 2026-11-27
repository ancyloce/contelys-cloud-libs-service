// Package identitytest is a fake of Identity for the library's tests: its key set, GET
// /internal/v1/keys of its contract, its issueWorkloadToken, POST /internal/v1/workload-tokens,
// and its signing of tokens. Only tests import it.
package identitytest

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/problem"
)

// Identity serves the key set and issues workload tokens. It counts the fetches of the key set
// and the requests for workload tokens, refused ones included, and it can stop answering or
// answer slowly, as Identity does when it is down or slow.
type Identity struct {
	server *httptest.Server

	mu       sync.Mutex
	keys     []jose.JSONWebKey
	down     bool
	delay    time.Duration
	fetches  int
	secrets  map[string]string
	grants   map[pair]grant
	lifetime time.Duration
	requests int
}

// pair is a calling deployable and the target of its tokens.
type pair struct{ caller, target string }

// grant is what a pair's tokens permit, and the key that signs them.
type grant struct {
	key        Key
	operations []string
}

// New starts a fake Identity that stops when the test ends. Its workload tokens live five
// minutes, as Identity's do.
func New(t testing.TB) *Identity {
	t.Helper()
	id := &Identity{secrets: map[string]string{}, grants: map[pair]grant{}, lifetime: 5 * time.Minute}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/v1/keys", id.serveKeys)
	mux.HandleFunc("POST /internal/v1/workload-tokens", id.issueWorkloadToken)
	id.server = httptest.NewServer(mux)
	t.Cleanup(id.server.Close)
	return id
}

// KeySet is the address of the key set.
func (id *Identity) KeySet() string { return id.server.URL + "/internal/v1/keys" }

// WorkloadTokens is the address of issueWorkloadToken.
func (id *Identity) WorkloadTokens() string { return id.server.URL + "/internal/v1/workload-tokens" }

// Publish adds k's public key to the key set.
func (id *Identity) Publish(k Key) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.keys = append(id.keys, jose.JSONWebKey{
		Key: k.Private.Public(), KeyID: k.KID, Algorithm: string(jose.EdDSA), Use: "sig",
	})
}

// Register makes Identity know the deployable caller by its secret.
func (id *Identity) Register(caller, secret string) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.secrets[caller] = secret
}

// Grant makes issueWorkloadToken answer the registered deployable caller with tokens for target
// that permit operations and are signed with k, as the target's access file would.
func (id *Identity) Grant(k Key, caller, target string, operations ...string) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.grants[pair{caller, target}] = grant{key: k, operations: operations}
}

// SetLifetime makes each workload token issued from now on live for lifetime.
func (id *Identity) SetLifetime(lifetime time.Duration) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.lifetime = lifetime
}

// SetDown makes the key set and issueWorkloadToken answer 503, or answer again.
func (id *Identity) SetDown(down bool) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.down = down
}

// SetDelay makes each answer come only after delay.
func (id *Identity) SetDelay(delay time.Duration) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.delay = delay
}

// Fetches is how many times the key set was fetched.
func (id *Identity) Fetches() int {
	id.mu.Lock()
	defer id.mu.Unlock()
	return id.fetches
}

// TokenRequests is how many requests for a workload token issueWorkloadToken received.
func (id *Identity) TokenRequests() int {
	id.mu.Lock()
	defer id.mu.Unlock()
	return id.requests
}

// wait waits for the delay, and reports whether the request is still there and Identity is up.
func (id *Identity) wait(w http.ResponseWriter, r *http.Request) bool {
	id.mu.Lock()
	delay := id.delay
	id.mu.Unlock()
	select {
	case <-time.After(delay):
	case <-r.Context().Done():
		return false
	}
	id.mu.Lock()
	down := id.down
	id.mu.Unlock()
	if down {
		problem.Write(w, r, http.StatusServiceUnavailable, "unavailable", problem.UnavailableTitle, "")
		return false
	}
	return true
}

func (id *Identity) serveKeys(w http.ResponseWriter, r *http.Request) {
	id.mu.Lock()
	id.fetches++
	id.mu.Unlock()
	if !id.wait(w, r) {
		return
	}
	id.mu.Lock()
	defer id.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: id.keys})
}

// issueWorkloadToken answers as Identity's contract says: HTTP Basic with the caller's slug and
// secret, a JSON body that names one target, and a token with its expiry.
func (id *Identity) issueWorkloadToken(w http.ResponseWriter, r *http.Request) {
	id.mu.Lock()
	id.requests++
	serial := id.requests
	id.mu.Unlock()
	if !id.wait(w, r) {
		return
	}
	var request struct {
		Target string `json:"target"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if r.Header.Get("Content-Type") != "application/json" || decoder.Decode(&request) != nil || request.Target == "" {
		problem.Write(w, r, http.StatusBadRequest, "invalid_request", "The request is not valid.", "")
		return
	}
	caller, secret, ok := r.BasicAuth()

	id.mu.Lock()
	known, isKnown := id.secrets[caller]
	granted, isGranted := id.grants[pair{caller, request.Target}]
	lifetime := id.lifetime
	id.mu.Unlock()
	switch {
	case !ok || !isKnown || secret != known:
		problem.Write(w, r, http.StatusUnauthorized, "credentials_invalid", "The slug or the secret is wrong.", "")
		return
	case !isGranted:
		problem.Write(w, r, http.StatusForbidden, "target_not_permitted", "No access file lists the target for the caller.", "")
		return
	}

	claims := Workload(caller, request.Target, granted.operations...)
	expiry := time.Now().Add(lifetime)
	claims["exp"] = expiry.Unix()
	claims["jti"] = fmt.Sprintf("workload-%d", serial)
	token, err := sign(granted.key, "workload+jwt", claims)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"token":     token,
		"expiresAt": expiry.UTC().Format(time.RFC3339Nano),
	})
}

// Key is one of Identity's Ed25519 signing keys.
type Key struct {
	KID     string
	Private ed25519.PrivateKey
}

// NewKey returns a new signing key named kid.
func NewKey(t testing.TB, kid string) Key {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return Key{KID: kid, Private: private}
}

// Sign signs claims as Identity does: EdDSA, with typ and the key's kid in the header.
func Sign(t testing.TB, k Key, typ string, claims map[string]any) string {
	t.Helper()
	raw, err := sign(k, typ, claims)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func sign(k Key, typ string, claims map[string]any) (string, error) {
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.EdDSA, Key: jose.JSONWebKey{Key: k.Private, KeyID: k.KID}},
		(&jose.SignerOptions{}).WithType(jose.ContentType(typ)),
	)
	if err != nil {
		return "", err
	}
	return jwt.Signed(signer).Claims(claims).Serialize()
}

// Workload returns the claims of a workload token that Identity issued now to the deployable
// caller for calls to target, permitting operations.
func Workload(caller, target string, operations ...string) map[string]any {
	now := time.Now()
	return map[string]any{
		"iss":        "urn:contelys:identity",
		"sub":        "urn:contelys:" + caller,
		"aud":        "urn:contelys:" + target,
		"operations": operations,
		"iat":        now.Unix(),
		"exp":        now.Add(5 * time.Minute).Unix(),
	}
}
