// Package idempotency gives a domain service's POSTs that create something the four answers of
// ADR 0062. Require checks a request's Idempotency-Key at the HTTP edge, and Claim holds the key
// in the transaction that commits the effect, where Claimed.Save stores the answer (architecture
// document, section 05), so that a client that repeats a request whose answer it lost never
// creates a second thing.
package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/migration"
	"github.com/ancyloce/contelys-cloud-libs-service/internal/problem"
	"github.com/ancyloce/contelys-cloud-libs-service/tokens"
)

// ErrInProgress is a Claim of a key that another request holds, or has stored its answer under
// since Require let this one through. The caller rolls its transaction back and answers with
// InProgress.
var ErrInProgress = errors.New("another request with the same idempotency key has not finished")

const (
	// header is the request header of the key, of the IETF draft "The Idempotency-Key HTTP
	// Header Field".
	header = "Idempotency-Key"
	// bodyLimit is the most bytes that the body of a request with a key may hold.
	bodyLimit = 1 << 20
)

// keyPattern is the pattern of a key in every contract: a UUID.
var keyPattern = regexp.MustCompile(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`)

//go:embed migrations/*.sql
var migrations embed.FS

const (
	// versionTable is goose's table of the store's migrations, apart from the service's own.
	versionTable = "idempotency_goose_version"
	// migrationLock is the advisory lock under which an instance migrates the store. Its value
	// spells "idempot" in ASCII.
	migrationLock int64 = 0x6964656d706f74
	// lockClass is the first key of the advisory lock of each key, which Claim holds until the
	// transaction ends. The two-key locks are apart from the one-key locks of the write barrier.
	// Its value spells "idmp" in ASCII.
	lockClass int32 = 0x69646d70
)

// Migrate creates or updates the store's table in the service's database.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if err := migration.Up(ctx, pool, migrations, versionTable, migrationLock); err != nil {
		return fmt.Errorf("idempotency: %w", err)
	}
	return nil
}

// Response is the answer of a request's first success, which a repeat gets again.
type Response struct {
	Status      int
	ContentType string
	Body        []byte
}

// scoped is a request's key, scoped to its caller and its operation, with the SHA-256 digest of
// the request: its path and query, and its body's bytes as they came, so that the same key on
// another resource, or with another body, is refused even when the two bodies mean the same.
type scoped struct {
	caller, operation, key string
	digest                 []byte
}

// lockName is the name of the key's advisory lock.
func (s scoped) lockName() string { return s.caller + "/" + s.operation + "/" + s.key }

type contextKey struct{}

// Require serves next, the handler of operation, by its operationId, only for a request of a
// caller that tokens.RequireInternal admitted, with an Idempotency-Key that no earlier request of
// the caller and the operation used. It answers every other request itself, with the problem body
// of the shared contracts (ADR 0054):
//
//   - no key: 400 idempotency_key_missing, and a key that is not a UUID 400 invalid_request;
//   - the key of an earlier request for the same path with the same body: that request's stored
//     response;
//   - the key of an earlier request for another path or with another body: 422
//     idempotency_key_reused.
//
// next's request carries the key, which next holds with Claim, first in the transaction of the
// effect. Claim refuses a key that another request holds, so that the effect happens once.
func Require(pool *pgxpool.Pool, operation string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller, ok := tokens.InternalFrom(r.Context())
		if !ok {
			problem.Write(w, r, http.StatusUnauthorized, "token_invalid", problem.TokenInvalidTitle, "")
			return
		}
		key := r.Header.Get(header)
		switch {
		case key == "":
			problem.Write(w, r, http.StatusBadRequest, "idempotency_key_missing", "The request has no Idempotency-Key.", "")
			return
		case !keyPattern.MatchString(key):
			problem.Write(w, r, http.StatusBadRequest, "invalid_request", problem.InvalidRequestTitle,
				"The Idempotency-Key is not a UUID.")
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, bodyLimit))
		if err != nil {
			problem.Write(w, r, http.StatusBadRequest, "invalid_request", problem.InvalidRequestTitle,
				"The request's body cannot be read, or is too large.")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		digest := sha256.New()
		digest.Write([]byte(r.URL.RequestURI() + "\n"))
		digest.Write(body)
		request := scoped{caller: caller.Principal, operation: operation, key: key, digest: digest.Sum(nil)}

		stored, err := lookUp(r.Context(), pool, request)
		switch {
		case errors.Is(err, errReused):
			problem.Write(w, r, http.StatusUnprocessableEntity, "idempotency_key_reused", "The key was used with another body.", "")
		case err != nil:
			problem.Write(w, r, http.StatusServiceUnavailable, "unavailable", problem.UnavailableTitle, "")
		case stored != nil:
			w.Header().Set("Content-Type", stored.ContentType)
			w.WriteHeader(stored.Status)
			_, _ = w.Write(stored.Body)
		default:
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), contextKey{}, request)))
		}
	})
}

// InProgress answers a request whose Claim returned ErrInProgress with 409
// idempotency_key_in_progress.
func InProgress(w http.ResponseWriter, r *http.Request) {
	problem.Write(w, r, http.StatusConflict, "idempotency_key_in_progress", "A request with the same key has not finished.", "")
}

// errReused is a key that an earlier request used for another path or with another body.
var errReused = errors.New("the idempotency key was used with another request")

// lookUp returns the response stored under the request's key, errReused for a key stored with
// another digest, and nil for a key that no committed request has stored.
func lookUp(ctx context.Context, pool *pgxpool.Pool, request scoped) (*Response, error) {
	var (
		digest   []byte
		response Response
	)
	err := pool.QueryRow(ctx,
		"SELECT digest, status, content_type, body FROM idempotency_keys WHERE caller = $1 AND operation = $2 AND key = $3",
		request.caller, request.operation, request.key).Scan(&digest, &response.Status, &response.ContentType, &response.Body)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("idempotency: %w", err)
	case !bytes.Equal(digest, request.digest):
		return nil, errReused
	}
	return &response, nil
}

// Claimed is a key that a transaction holds until it ends.
type Claimed struct {
	tx      pgx.Tx
	request scoped
}

// Claim holds the key of the request in ctx, which Require let through, in tx, the transaction
// that commits the operation's effect, until tx ends. It comes first in tx, before the effect, so
// that a repeat during the first request is refused before its operation runs. When another
// request holds the key, or has stored its answer under it since Require let this one through, it
// returns ErrInProgress, and the caller rolls tx back.
func Claim(ctx context.Context, tx pgx.Tx) (*Claimed, error) {
	request, ok := ctx.Value(contextKey{}).(scoped)
	if !ok {
		return nil, errors.New("idempotency: the request has no key that Require let through")
	}
	var free, stored bool
	err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1, hashtext($2))", lockClass, request.lockName()).Scan(&free)
	if err != nil {
		return nil, fmt.Errorf("idempotency: %w", err)
	}
	if !free {
		return nil, fmt.Errorf("idempotency: %w", ErrInProgress)
	}
	err = tx.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM idempotency_keys WHERE caller = $1 AND operation = $2 AND key = $3)",
		request.caller, request.operation, request.key).Scan(&stored)
	if err != nil {
		return nil, fmt.Errorf("idempotency: %w", err)
	}
	if stored {
		return nil, fmt.Errorf("idempotency: %w", ErrInProgress)
	}
	return &Claimed{tx: tx, request: request}, nil
}

// Save stores response under the claimed key, in the claim's transaction, so that a repeat gets it
// once the transaction commits.
func (c *Claimed) Save(ctx context.Context, response Response) error {
	_, err := c.tx.Exec(ctx,
		`INSERT INTO idempotency_keys (caller, operation, key, digest, status, content_type, body)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		c.request.caller, c.request.operation, c.request.key, c.request.digest, response.Status, response.ContentType, response.Body)
	if err != nil {
		return fmt.Errorf("idempotency: %w", err)
	}
	return nil
}
