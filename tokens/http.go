package tokens

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/problem"
)

// ErrNoInternalToken is a call for a caller that was not sent, because its context holds no
// internal token that RequireInternal admitted.
var ErrNoInternalToken = errors.New("no internal token of a caller")

type contextKey int

const (
	internalKey contextKey = iota
	workloadKey
)

// admitted is an internal token that RequireInternal verified: its claims, and the token itself,
// which only PassOn reads.
type admitted struct {
	claims Internal
	raw    string
}

// RequireInternal serves next only for a request whose Authorization header carries a valid
// internal token, and puts the token's claims in the request's context for InternalFrom, and the
// token for PassOn. It refuses any other request with the problem body of the shared contracts
// (ADR 0054).
func (v *Verifier) RequireInternal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := bearer(r)
		caller, err := v.Internal(r.Context(), raw)
		if err != nil {
			refuse(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), internalKey, admitted{claims: caller, raw: raw})))
	})
}

// PassOn makes every call through base, or http.DefaultTransport when base is nil, carry the
// internal token of the caller that RequireInternal admitted, read from the call's context, as
// its bearer token: a call made for a caller carries the caller's token (ADR 0030). A call whose
// context holds no such token is not sent, and fails with ErrNoInternalToken, which http.Client
// wraps in a *url.Error.
func PassOn(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return passing{base: base}
}

// passing is the transport of calls made for a caller.
type passing struct {
	base http.RoundTripper
}

func (p passing) RoundTrip(r *http.Request) (*http.Response, error) {
	caller, ok := r.Context().Value(internalKey).(admitted)
	if !ok {
		if r.Body != nil {
			_ = r.Body.Close()
		}
		return nil, fmt.Errorf("tokens: %w", ErrNoInternalToken)
	}
	call := r.Clone(r.Context())
	call.Header.Set("Authorization", "Bearer "+caller.raw)
	return p.base.RoundTrip(call)
}

// RequireWorkload serves next only for a request whose Authorization header carries a valid
// workload token that permits operation, the operationId of the route, and puts the token's
// claims in the request's context for WorkloadFrom.
func (v *Verifier) RequireWorkload(operation string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller, err := v.Workload(r.Context(), bearer(r), operation)
		if err != nil {
			refuse(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), workloadKey, caller)))
	})
}

// InternalFrom returns the internal token's claims that RequireInternal verified.
func InternalFrom(ctx context.Context) (Internal, bool) {
	caller, ok := ctx.Value(internalKey).(admitted)
	return caller.claims, ok
}

// WorkloadFrom returns the workload token's claims that RequireWorkload verified.
func WorkloadFrom(ctx context.Context) (Workload, bool) {
	caller, ok := ctx.Value(workloadKey).(Workload)
	return caller, ok
}

// bearer returns the token of the request's Authorization header, or "".
func bearer(r *http.Request) string {
	header := r.Header.Get("Authorization")
	if len(header) > len("Bearer ") && strings.EqualFold(header[:len("Bearer ")], "Bearer ") {
		return header[len("Bearer "):]
	}
	return ""
}

// refuse answers err with its status and code. The body names no token and no claim.
func refuse(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrNotPermitted):
		problem.Write(w, r, http.StatusForbidden, "operation_not_permitted",
			"The workload token does not permit this operation.", "")
	case errors.Is(err, ErrUnavailable):
		problem.Write(w, r, http.StatusServiceUnavailable, "unavailable",
			problem.UnavailableTitle, "The key set that verifies tokens cannot be established.")
	default:
		problem.Write(w, r, http.StatusUnauthorized, "token_invalid", problem.TokenInvalidTitle, "")
	}
}
