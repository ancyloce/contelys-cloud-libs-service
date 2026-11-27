package barrier

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/problem"
	"github.com/ancyloce/contelys-cloud-libs-service/tokens"
)

// operation is the operationId of the shared operation, which a workload token must permit.
const operation = "installMinimumAuthorityVersion"

// bodyLimit is the most bytes that the body of an installation may hold.
const bodyLimit = 1024

// idPattern is PrincipalOrMembershipId of the shared operation's contract.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)

// installers names the one deployable that may install the minimum of each kind (ADR 0068).
var installers = map[Kind]string{Principal: "identity", Membership: "organization-access"}

// Handler serves the shared operation of ADR 0068, PUT
// /internal/v1/minimum-authority-versions/{kind}/{id} as contracts/openapi/shared/
// minimum-authority-version.yaml gives it, for the registered domain service whose database is
// pool and whose workload tokens verifier verifies. It answers 204 once the stored minimum is at
// least the one sent and committed, and every refusal with the problem body and a code of that
// contract.
func Handler(pool *pgxpool.Pool, verifier *tokens.Verifier) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("PUT /internal/v1/minimum-authority-versions/{kind}/{id}",
		verifier.RequireWorkload(operation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			kind, id := Kind(r.PathValue("kind")), r.PathValue("id")
			installer, known := installers[kind]
			if !known || !idPattern.MatchString(id) {
				invalid(w, r)
				return
			}
			if caller, _ := tokens.WorkloadFrom(r.Context()); caller.Caller != installer {
				problem.Write(w, r, http.StatusForbidden, "kind_not_permitted",
					"The caller may not install a minimum of this kind.", "")
				return
			}
			minimum, ok := readMinimum(w, r)
			if !ok {
				invalid(w, r)
				return
			}
			if err := installAlone(r.Context(), pool, kind, id, minimum); err != nil {
				problem.Write(w, r, http.StatusServiceUnavailable, "unavailable",
					problem.UnavailableTitle, "The installation could not be committed.")
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})))
	return mux
}

func invalid(w http.ResponseWriter, r *http.Request) {
	problem.Write(w, r, http.StatusBadRequest, "invalid_request", problem.InvalidRequestTitle, "")
}

// readMinimum reads the body MinimumAuthorityVersionInstallation of the contract: one JSON object
// of the media type application/json with the member minimumAuthorityVersion, spelled exactly, an
// integer of at least 1, and no other member.
func readMinimum(w http.ResponseWriter, r *http.Request) (int64, bool) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return 0, false
	}
	// Members are read by their exact names: decoding into a struct would match any case.
	var members map[string]json.RawMessage
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, bodyLimit))
	if err := decoder.Decode(&members); err != nil {
		return 0, false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return 0, false
	}
	value, ok := members["minimumAuthorityVersion"]
	if !ok || len(members) != 1 {
		return 0, false
	}
	var minimum int64
	if err := json.Unmarshal(value, &minimum); err != nil || minimum < 1 {
		return 0, false
	}
	return minimum, true
}

// installAlone installs minimum in a transaction of its own, which it commits.
func installAlone(ctx context.Context, pool *pgxpool.Pool, kind Kind, id string, minimum int64) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := install(ctx, tx, kind, id, minimum); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
