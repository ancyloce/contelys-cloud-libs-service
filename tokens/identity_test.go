package tokens_test

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

// identity is a fake of Identity's key set, GET /internal/v1/keys of its contract. It counts the
// fetches, and it can stop answering, as Identity does when it is down.
type identity struct {
	t      *testing.T
	server *httptest.Server

	mu      sync.Mutex
	keys    []jose.JSONWebKey
	down    bool
	delay   time.Duration
	fetches int
}

func newIdentity(t *testing.T) *identity {
	t.Helper()
	id := &identity{t: t}
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

// keySet is the address of the fake's key set.
func (id *identity) keySet() string { return id.server.URL + "/internal/v1/keys" }

func (id *identity) publish(k signingKey) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.keys = append(id.keys, jose.JSONWebKey{
		Key: k.private.Public(), KeyID: k.kid, Algorithm: string(jose.EdDSA), Use: "sig",
	})
}

func (id *identity) setDown(down bool) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.down = down
}

// setDelay makes the fake answer each fetch only after delay, as a slow or hanging Identity does.
func (id *identity) setDelay(delay time.Duration) {
	id.mu.Lock()
	defer id.mu.Unlock()
	id.delay = delay
}

func (id *identity) fetchCount() int {
	id.mu.Lock()
	defer id.mu.Unlock()
	return id.fetches
}

// signingKey is one of Identity's Ed25519 signing keys.
type signingKey struct {
	kid     string
	private ed25519.PrivateKey
}

func newKey(t *testing.T, kid string) signingKey {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return signingKey{kid: kid, private: private}
}

// sign signs claims as Identity does: EdDSA, with typ and the key's kid in the header.
func sign(t *testing.T, k signingKey, typ string, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.EdDSA, Key: jose.JSONWebKey{Key: k.private, KeyID: k.kid}},
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

// internalClaims are the claims of a valid internal token issued now, as Identity's design
// section 05 gives them.
func internalClaims() map[string]any {
	now := time.Now()
	return map[string]any{
		"iss":               "urn:contelys:identity",
		"aud":               "urn:contelys:domain-services",
		"sub":               "01JDZ8Q3V7XK2M4N6P8R0T2W4Y",
		"sid":               "01JDZ8R5A1B3C5D7E9F1G3H5J7",
		"authority_version": 3,
		"iat":               now.Unix(),
		"exp":               now.Add(60 * time.Second).Unix(),
		"jti":               "req-7f3a",
	}
}
