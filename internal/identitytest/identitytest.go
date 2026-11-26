// Package identitytest is a fake of Identity for the library's tests: its key set, GET
// /internal/v1/keys of its contract, and its signing of tokens. Only tests import it.
package identitytest

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// Identity serves the key set. It counts the fetches, and it can stop answering or answer
// slowly, as Identity does when it is down or slow.
type Identity struct {
	server *httptest.Server

	mu      sync.Mutex
	keys    []jose.JSONWebKey
	down    bool
	delay   time.Duration
	fetches int
}

// New starts a fake Identity that stops when the test ends.
func New(t testing.TB) *Identity {
	t.Helper()
	id := &Identity{}
	id.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id.mu.Lock()
		id.fetches++
		delay := id.delay
		id.mu.Unlock()
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
		id.mu.Lock()
		defer id.mu.Unlock()
		if id.down || r.URL.Path != "/internal/v1/keys" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: id.keys})
	}))
	t.Cleanup(id.server.Close)
	return id
}

// KeySet is the address of the key set.
func (id *Identity) KeySet() string { return id.server.URL + "/internal/v1/keys" }

// Publish adds k's public key to the key set.
func (id *Identity) Publish(k Key) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.keys = append(id.keys, jose.JSONWebKey{
		Key: k.Private.Public(), KeyID: k.KID, Algorithm: string(jose.EdDSA), Use: "sig",
	})
}

// SetDown makes the key set answer 503, or answer again.
func (id *Identity) SetDown(down bool) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.down = down
}

// SetDelay makes each fetch answer only after delay.
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
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.EdDSA, Key: jose.JSONWebKey{Key: k.Private, KeyID: k.KID}},
		(&jose.SignerOptions{}).WithType(jose.ContentType(typ)),
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return raw
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
