package tokens_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/ancyloce/contelys-cloud/libs/service/tokens"
)

func newVerifier(id *identity) *tokens.Verifier {
	return tokens.New(tokens.Options{KeySet: id.keySet(), Client: http.DefaultClient, Deployable: "projects"})
}

func TestAnInternalTokenIsVerifiedAgainstIdentitysKeySet(t *testing.T) {
	id := newIdentity(t)
	key := newKey(t, "internal-2026-11")
	id.publish(key)

	got, err := newVerifier(id).Internal(context.Background(), sign(t, key, "internal+jwt", internalClaims()))
	if err != nil {
		t.Fatalf("Internal: %v", err)
	}
	want := tokens.Internal{
		Principal:        "01JDZ8Q3V7XK2M4N6P8R0T2W4Y",
		Session:          "01JDZ8R5A1B3C5D7E9F1G3H5J7",
		AuthorityVersion: 3,
		RequestID:        "req-7f3a",
	}
	if got != want {
		t.Errorf("claims = %+v\nwant %+v", got, want)
	}
}

// with returns the valid internal claims with one change: a value of nil removes the claim.
func with(name string, value any) map[string]any {
	claims := internalClaims()
	if value == nil {
		delete(claims, name)
	} else {
		claims[name] = value
	}
	return claims
}

// altered changes the payload of a signed token and keeps its signature.
func altered(t *testing.T, raw string) string {
	t.Helper()
	parts := strings.Split(raw, ".")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	payload = bytes.Replace(payload, []byte(`"authority_version":3`), []byte(`"authority_version":9`), 1)
	parts[1] = base64.RawURLEncoding.EncodeToString(payload)
	return strings.Join(parts, ".")
}

func TestAnInternalTokenThatBreaksARuleIsRefused(t *testing.T) {
	id := newIdentity(t)
	key := newKey(t, "internal-2026-11")
	id.publish(key)
	impostor := newKey(t, "internal-2026-11")
	now := time.Now()

	hmac, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.HS256, Key: jose.JSONWebKey{Key: []byte("a shared secret of thirty-two by"), KeyID: key.kid}},
		(&jose.SignerOptions{}).WithType("internal+jwt"),
	)
	if err != nil {
		t.Fatal(err)
	}
	symmetric, err := jwt.Signed(hmac).Claims(internalClaims()).Serialize()
	if err != nil {
		t.Fatal(err)
	}

	refused := map[string]string{
		"of the workload kind":         sign(t, key, "workload+jwt", internalClaims()),
		"without a typ":                sign(t, key, "", internalClaims()),
		"from another issuer":          sign(t, key, "internal+jwt", with("iss", "urn:contelys:keycloak")),
		"for another audience":         sign(t, key, "internal+jwt", with("aud", "urn:contelys:projects")),
		"expired":                      sign(t, key, "internal+jwt", with("exp", now.Add(-2*time.Minute).Unix())),
		"without an expiry":            sign(t, key, "internal+jwt", with("exp", nil)),
		"issued in the future":         sign(t, key, "internal+jwt", with("iat", now.Add(5*time.Minute).Unix())),
		"altered":                      altered(t, sign(t, key, "internal+jwt", internalClaims())),
		"signed by another key":        sign(t, impostor, "internal+jwt", internalClaims()),
		"signed with HS256":            symmetric,
		"not a token":                  "not.a.token",
		"without a sub":                sign(t, key, "internal+jwt", with("sub", nil)),
		"without a sid":                sign(t, key, "internal+jwt", with("sid", nil)),
		"without an authority version": sign(t, key, "internal+jwt", with("authority_version", nil)),
	}
	verifier := newVerifier(id)
	for name, raw := range refused {
		t.Run(name, func(t *testing.T) {
			if _, err := verifier.Internal(context.Background(), raw); !errors.Is(err, tokens.ErrInvalid) {
				t.Errorf("Internal = %v, want ErrInvalid", err)
			}
		})
	}
}

// pastTheBound waits until the verifier may fetch the key set again: it fetches at most once a
// second.
func pastTheBound() { time.Sleep(1100 * time.Millisecond) }

// verifyAll verifies each token at once, as concurrent requests do, and returns their errors.
func verifyAll(verifier *tokens.Verifier, raws []string) []error {
	errs := make([]error, len(raws))
	var wg sync.WaitGroup
	for i, raw := range raws {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = verifier.Internal(context.Background(), raw)
		}()
	}
	wg.Wait()
	return errs
}

func TestARotatedKeyIsFetchedOnceAndAnUnknownKeyIsRefused(t *testing.T) {
	id := newIdentity(t)
	old := newKey(t, "internal-2026-10")
	id.publish(old)
	verifier := newVerifier(id)
	if _, err := verifier.Internal(context.Background(), sign(t, old, "internal+jwt", internalClaims())); err != nil {
		t.Fatalf("Internal with the published key: %v", err)
	}

	// Identity publishes a new key and then signs with it, as a rotation does.
	pastTheBound()
	fresh := newKey(t, "internal-2026-11")
	id.publish(fresh)
	signed := make([]string, 20)
	for i := range signed {
		signed[i] = sign(t, fresh, "internal+jwt", internalClaims())
	}
	before := id.fetchCount()
	for _, err := range verifyAll(verifier, signed) {
		if err != nil {
			t.Errorf("Internal with the new key: %v", err)
		}
	}
	if got := id.fetchCount() - before; got != 1 {
		t.Errorf("fetches for the new key = %d, want 1", got)
	}

	// Tokens that name keys Identity never published are refused, and a burst of them fetches
	// the key set at most once.
	pastTheBound()
	forged := make([]string, 20)
	for i := range forged {
		forged[i] = sign(t, newKey(t, fmt.Sprintf("forged-%d", i)), "internal+jwt", internalClaims())
	}
	before = id.fetchCount()
	for _, err := range verifyAll(verifier, forged) {
		if !errors.Is(err, tokens.ErrInvalid) {
			t.Errorf("Internal with an unknown key = %v, want ErrInvalid", err)
		}
	}
	if got := id.fetchCount() - before; got > 1 {
		t.Errorf("fetches for a burst of unknown keys = %d, want at most 1", got)
	}
}

func TestWithoutAKeySetVerificationIsUnavailableAndALaterOutageDoesNotStopIt(t *testing.T) {
	id := newIdentity(t)
	key := newKey(t, "internal-2026-11")
	id.publish(key)
	id.setDown(true)
	verifier := newVerifier(id)
	ctx := context.Background()
	raw := sign(t, key, "internal+jwt", internalClaims())

	if _, err := verifier.Internal(ctx, raw); !errors.Is(err, tokens.ErrUnavailable) {
		t.Errorf("Internal without a key set = %v, want ErrUnavailable", err)
	}
	fetches := id.fetchCount()
	if _, err := verifier.Internal(ctx, raw); !errors.Is(err, tokens.ErrUnavailable) {
		t.Errorf("Internal again without a key set = %v, want ErrUnavailable", err)
	}
	if got := id.fetchCount(); got != fetches {
		t.Errorf("fetches within the bound = %d, want %d", got, fetches)
	}

	id.setDown(false)
	pastTheBound()
	if _, err := verifier.Internal(ctx, raw); err != nil {
		t.Fatalf("Internal once Identity answers: %v", err)
	}

	// A later outage of Identity does not stop verification with the cached key set.
	id.setDown(true)
	if _, err := verifier.Internal(ctx, sign(t, key, "internal+jwt", internalClaims())); err != nil {
		t.Errorf("Internal during an outage: %v", err)
	}
	// A token that names a key the verifier does not hold cannot be judged during the outage.
	pastTheBound()
	unknown := sign(t, newKey(t, "internal-2026-12"), "internal+jwt", internalClaims())
	if _, err := verifier.Internal(ctx, unknown); !errors.Is(err, tokens.ErrUnavailable) {
		t.Errorf("Internal with an unknown key during an outage = %v, want ErrUnavailable", err)
	}
}

// workloadClaims are the claims of a workload token that Identity issued now to Notifications
// for calls to Projects, as Identity's design section 06 gives them.
func workloadClaims(change func(map[string]any)) map[string]any {
	now := time.Now()
	claims := map[string]any{
		"iss":        "urn:contelys:identity",
		"sub":        "urn:contelys:notifications",
		"aud":        "urn:contelys:projects",
		"operations": []string{"listProjects", "getProject"},
		"iat":        now.Unix(),
		"exp":        now.Add(5 * time.Minute).Unix(),
	}
	if change != nil {
		change(claims)
	}
	return claims
}

func TestAWorkloadTokenIsAcceptedOnlyForAnOperationThatItLists(t *testing.T) {
	id := newIdentity(t)
	key := newKey(t, "workload-2026-11")
	id.publish(key)
	verifier := newVerifier(id)
	ctx := context.Background()
	raw := sign(t, key, "workload+jwt", workloadClaims(nil))

	got, err := verifier.Workload(ctx, raw, "getProject")
	if err != nil {
		t.Fatalf("Workload for a listed operation: %v", err)
	}
	if got.Caller != "notifications" {
		t.Errorf("caller = %q, want notifications", got.Caller)
	}
	if _, err := verifier.Workload(ctx, raw, "deleteProject"); !errors.Is(err, tokens.ErrNotPermitted) {
		t.Errorf("Workload for an operation it does not list = %v, want ErrNotPermitted", err)
	}

	refused := map[string]string{
		"for another deployable": sign(t, key, "workload+jwt", workloadClaims(func(c map[string]any) {
			c["aud"] = "urn:contelys:organization-access"
		})),
		"an internal token":    sign(t, key, "internal+jwt", internalClaims()),
		"of the internal kind": sign(t, key, "internal+jwt", workloadClaims(nil)),
		"expired": sign(t, key, "workload+jwt", workloadClaims(func(c map[string]any) {
			c["exp"] = time.Now().Add(-time.Minute).Unix()
		})),
		"from a caller that is not a deployable": sign(t, key, "workload+jwt", workloadClaims(func(c map[string]any) {
			c["sub"] = "notifications"
		})),
	}
	for name, raw := range refused {
		t.Run(name, func(t *testing.T) {
			if _, err := verifier.Workload(ctx, raw, "getProject"); !errors.Is(err, tokens.ErrInvalid) {
				t.Errorf("Workload = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestACallerThatGivesUpDoesNotSpoilTheFetchForTheOthers(t *testing.T) {
	id := newIdentity(t)
	key := newKey(t, "internal-2026-11")
	id.publish(key)
	id.setDelay(100 * time.Millisecond)
	verifier := newVerifier(id)
	raw := sign(t, key, "internal+jwt", internalClaims())

	hurried, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, _ = verifier.Internal(hurried, raw)

	if _, err := verifier.Internal(context.Background(), raw); err != nil {
		t.Errorf("Internal after a caller gave up = %v, want the token accepted", err)
	}
}

func TestCallersWaitingOnAHangingKeySetShareOneFetchAndKeepTheirDeadline(t *testing.T) {
	id := newIdentity(t)
	key := newKey(t, "internal-2026-11")
	id.publish(key)
	id.setDelay(5 * time.Second)
	verifier := newVerifier(id)
	raw := sign(t, key, "internal+jwt", internalClaims())

	start := time.Now()
	for _, err := range verifyAll(verifier, []string{raw, raw, raw}) {
		if !errors.Is(err, tokens.ErrUnavailable) {
			t.Errorf("Internal while Identity hangs = %v, want ErrUnavailable", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("three callers waited %s, want them answered within one bounded fetch", elapsed)
	}
	if got := id.fetchCount(); got != 1 {
		t.Errorf("fetches = %d, want the callers to share 1", got)
	}

	pastTheBound()
	short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	if _, err := verifier.Internal(short, raw); !errors.Is(err, tokens.ErrUnavailable) {
		t.Errorf("Internal with a short deadline = %v, want ErrUnavailable", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("a caller with a deadline of 100ms waited %s", elapsed)
	}
}
