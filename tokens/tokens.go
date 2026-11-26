// Package tokens verifies the internal tokens and the workload tokens that Identity issues,
// against Identity's key set, which it fetches and caches (ADR 0052).
package tokens

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// ErrInvalid is a token that must be refused: missing, altered, expired, of the wrong kind, from
// another issuer or for another audience.
var ErrInvalid = errors.New("invalid token")

// ErrNotPermitted is a valid workload token whose operations claim does not list the operation
// called.
var ErrNotPermitted = errors.New("the workload token does not permit the operation")

// ErrUnavailable is a token that cannot be judged, because the key set that would verify it
// cannot be fetched from Identity. A service answers it with 503 and never accepts the token.
var ErrUnavailable = errors.New("the key set cannot be established")

const (
	issuer = "urn:contelys:identity"
	// internalAudience is the audience of every internal token.
	internalAudience = "urn:contelys:domain-services"
	// deployableURN prefixes a deployable's slug in a workload token's sub and aud.
	deployableURN = "urn:contelys:"
	// leeway is the clock skew allowed between Identity and the verifier.
	leeway = 5 * time.Second
	// refreshInterval bounds how often a token with an unknown kid makes the verifier fetch the
	// key set again, so tokens that name invented keys cannot flood Identity.
	refreshInterval = time.Second
	// fetchTimeout bounds one fetch of the key set.
	fetchTimeout = 2 * time.Second
)

// Options say where the key set is and who is verifying.
type Options struct {
	// KeySet is the address of Identity's key set, GET /internal/v1/keys of its contract.
	KeySet string
	// Client fetches the key set, with the transport that the internal listener needs.
	Client *http.Client
	// Deployable is this deployable's slug, which a workload token must name as its audience.
	Deployable string
}

// Internal is what an internal token says about the request's caller.
type Internal struct {
	Principal        string
	Session          string
	AuthorityVersion int64
	RequestID        string
}

// kind is what tells one kind of token from the other: its typ and its audience.
type kind struct {
	typ, audience string
}

// internalKind is every internal token's kind.
var internalKind = kind{typ: "internal+jwt", audience: internalAudience}

// Workload is what a workload token says about the calling deployable.
type Workload struct {
	// Caller is the slug of the calling deployable.
	Caller string
}

// Verifier verifies tokens against the key set, which it fetches on first use and again only
// when a token names a key that it does not hold.
type Verifier struct {
	opts Options
	keys atomic.Pointer[jose.JSONWebKeySet]

	// mu guards the fetch in flight, and the time and the outcome of the last one.
	mu          sync.Mutex
	inFlight    *fetching
	lastAttempt time.Time
	lastFailed  bool
}

// fetching is one fetch of the key set, whose outcome every caller that waits for it shares.
type fetching struct {
	done chan struct{}
	keys *jose.JSONWebKeySet
	err  error
}

// New returns a verifier. It fetches nothing until it verifies its first token.
func New(opts Options) *Verifier {
	return &Verifier{opts: opts}
}

// Internal verifies an internal token and returns its claims.
func (v *Verifier) Internal(ctx context.Context, raw string) (Internal, error) {
	var claims struct {
		Session          string `json:"sid"`
		AuthorityVersion *int64 `json:"authority_version"`
	}
	standard, err := v.verify(ctx, raw, internalKind, &claims)
	if err != nil {
		return Internal{}, err
	}
	if standard.Subject == "" || claims.Session == "" || claims.AuthorityVersion == nil {
		return Internal{}, fmt.Errorf("tokens: %w: sub, sid and authority_version are required", ErrInvalid)
	}
	return Internal{
		Principal:        standard.Subject,
		Session:          claims.Session,
		AuthorityVersion: *claims.AuthorityVersion,
		RequestID:        standard.ID,
	}, nil
}

// Workload verifies a workload token for the operation called, by its operationId, and returns
// its caller. A valid token whose operations claim does not list the operation is ErrNotPermitted.
func (v *Verifier) Workload(ctx context.Context, raw, operation string) (Workload, error) {
	var claims struct {
		Operations []string `json:"operations"`
	}
	workload := kind{typ: "workload+jwt", audience: deployableURN + v.opts.Deployable}
	standard, err := v.verify(ctx, raw, workload, &claims)
	if err != nil {
		return Workload{}, err
	}
	caller, ok := strings.CutPrefix(standard.Subject, deployableURN)
	if !ok || caller == "" {
		return Workload{}, fmt.Errorf("tokens: %w: sub %q names no deployable", ErrInvalid, standard.Subject)
	}
	if !slices.Contains(claims.Operations, operation) {
		return Workload{}, fmt.Errorf("tokens: %w: %s", ErrNotPermitted, operation)
	}
	return Workload{Caller: caller}, nil
}

// verify checks raw as a token of kind, decodes its private claims into claims, and returns its
// registered claims.
func (v *Verifier) verify(ctx context.Context, raw string, kind kind, claims any) (jwt.Claims, error) {
	var standard jwt.Claims
	token, err := jwt.ParseSigned(raw, []jose.SignatureAlgorithm{jose.EdDSA})
	if err != nil {
		return standard, fmt.Errorf("tokens: %w: %v", ErrInvalid, err)
	}
	header := token.Headers[0]
	if header.ExtraHeaders[jose.HeaderType] != kind.typ {
		return standard, fmt.Errorf("tokens: %w: typ is not %s", ErrInvalid, kind.typ)
	}
	if header.KeyID == "" {
		return standard, fmt.Errorf("tokens: %w: no kid", ErrInvalid)
	}
	key, err := v.key(ctx, header.KeyID)
	if err != nil {
		return standard, err
	}
	if err := token.Claims(key, &standard, claims); err != nil {
		return standard, fmt.Errorf("tokens: %w: %v", ErrInvalid, err)
	}
	if standard.Expiry == nil {
		return standard, fmt.Errorf("tokens: %w: no exp", ErrInvalid)
	}
	expected := jwt.Expected{Issuer: issuer, AnyAudience: jwt.Audience{kind.audience}, Time: time.Now()}
	if err := standard.ValidateWithLeeway(expected, leeway); err != nil {
		return standard, fmt.Errorf("tokens: %w: %v", ErrInvalid, err)
	}
	return standard, nil
}

// key returns the public key that kid names. A kid that the cached key set does not hold makes
// it fetch the key set again, at most once per refreshInterval after the last fetch ended. One
// fetch at a time runs, detached from the caller that started it, and every caller waits for it
// no longer than its own context allows.
func (v *Verifier) key(ctx context.Context, kid string) (any, error) {
	if key := find(v.keys.Load(), kid); key != nil {
		return key, nil
	}
	v.mu.Lock()
	if key := find(v.keys.Load(), kid); key != nil {
		v.mu.Unlock()
		return key, nil
	}
	call := v.inFlight
	if call == nil {
		if time.Since(v.lastAttempt) < refreshInterval {
			failed := v.lastFailed
			v.mu.Unlock()
			if failed {
				return nil, fmt.Errorf("tokens: %w: the last fetch failed", ErrUnavailable)
			}
			return nil, fmt.Errorf("tokens: %w: unknown key %q", ErrInvalid, kid)
		}
		call = &fetching{done: make(chan struct{})}
		v.inFlight = call
		go v.refresh(context.WithoutCancel(ctx), call)
	}
	v.mu.Unlock()

	select {
	case <-call.done:
	case <-ctx.Done():
		return nil, fmt.Errorf("tokens: %w: %v", ErrUnavailable, ctx.Err())
	}
	if call.err != nil {
		return nil, fmt.Errorf("tokens: %w: %v", ErrUnavailable, call.err)
	}
	if key := find(call.keys, kid); key != nil {
		return key, nil
	}
	return nil, fmt.Errorf("tokens: %w: unknown key %q", ErrInvalid, kid)
}

// refresh fetches the key set for call, within fetchTimeout, and records its outcome.
func (v *Verifier) refresh(ctx context.Context, call *fetching) {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	keys, err := v.fetch(ctx)

	v.mu.Lock()
	if err == nil {
		v.keys.Store(keys)
	}
	v.lastAttempt = time.Now()
	v.lastFailed = err != nil
	v.inFlight = nil
	call.keys, call.err = keys, err
	v.mu.Unlock()
	close(call.done)
}

// find returns the public key that kid names in keys, or nil.
func find(keys *jose.JSONWebKeySet, kid string) any {
	if keys == nil {
		return nil
	}
	if found := keys.Key(kid); len(found) > 0 {
		return found[0].Key
	}
	return nil
}

// fetch reads the key set from Identity.
func (v *Verifier) fetch(ctx context.Context) (*jose.JSONWebKeySet, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, v.opts.KeySet, nil)
	if err != nil {
		return nil, fmt.Errorf("key set: %w", err)
	}
	response, err := v.opts.Client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("key set: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("key set: %s", response.Status)
	}
	var keys jose.JSONWebKeySet
	if err := json.NewDecoder(response.Body).Decode(&keys); err != nil {
		return nil, fmt.Errorf("key set: %w", err)
	}
	return &keys, nil
}
