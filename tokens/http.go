package tokens

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/problem"
)

type contextKey int

const (
	internalKey contextKey = iota
	workloadKey
)

// RequireInternal serves next only for a request whose Authorization header carries a valid
// internal token, and puts the token's claims in the request's context for InternalFrom. It
// refuses any other request with the problem body of the shared contracts (ADR 0054).
func (v *Verifier) RequireInternal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller, err := v.Internal(r.Context(), bearer(r))
		if err != nil {
			refuse(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), internalKey, caller)))
	})
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
	caller, ok := ctx.Value(internalKey).(Internal)
	return caller, ok
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
		problem.Write(w, r, http.StatusUnauthorized, "token_invalid",
			"The token is missing, expired, altered, of the wrong kind, or for another audience.", "")
	}
}
