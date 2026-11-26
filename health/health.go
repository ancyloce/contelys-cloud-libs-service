// Package health serves a domain service's liveness, its readiness and the revision of the
// configuration it applied. Liveness says the process runs. Readiness says the instance can do
// its work, and only checks that the service marks as required take it out of routing.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// checkDeadline bounds each check. A check that has not answered within it has failed, so a
// hanging check that is not required never delays readiness past a prober's timeout.
const checkDeadline = time.Second

// Check is one named signal of readiness.
type Check struct {
	Name string
	// Required checks take the instance out of routing when they fail. Others are reported
	// beside them and leave the instance ready.
	Required bool
	// Run reports whether the check passes. Its context ends at the check's deadline.
	Run func(context.Context) error
}

// Handler serves GET /livez, GET /readyz and GET /revision. applied reports the revision and
// digest of the configuration in force.
func Handler(applied func() (revision, digest string), checks ...Check) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		answer := readiness{Ready: true, Checks: run(r.Context(), checks)}
		for _, check := range answer.Checks {
			if check.Required && !check.OK {
				answer.Ready = false
			}
		}
		status := http.StatusOK
		if !answer.Ready {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, answer)
	})
	mux.HandleFunc("GET /revision", func(w http.ResponseWriter, _ *http.Request) {
		var answer revision
		answer.Revision, answer.Digest = applied()
		writeJSON(w, http.StatusOK, answer)
	})
	return mux
}

// run runs every check at once, each until its deadline, and returns their results in the
// order of checks.
func run(ctx context.Context, checks []Check) []result {
	ctx, cancel := context.WithTimeout(ctx, checkDeadline)
	defer cancel()

	passed := make([]chan bool, len(checks))
	for i, check := range checks {
		passed[i] = make(chan bool, 1)
		go func() { passed[i] <- check.Run(ctx) == nil }()
	}
	results := make([]result, len(checks))
	for i, check := range checks {
		results[i] = result{Name: check.Name, Required: check.Required}
		select {
		case ok := <-passed[i]:
			results[i].OK = ok
		case <-ctx.Done():
		}
	}
	return results
}

// revision is the body of GET /revision, in the form of Edge's own answer: the Git commit of
// the configuration in force and the digest of its file.
type revision struct {
	Revision string `json:"revision"`
	Digest   string `json:"digest"`
}

// readiness is the body of GET /readyz. It names each check and whether it passed, and leaves
// out the reason of a failure, which may hold an address or a secret.
type readiness struct {
	Ready  bool     `json:"ready"`
	Checks []result `json:"checks"`
}

type result struct {
	Name     string `json:"name"`
	Required bool   `json:"required"`
	OK       bool   `json:"ok"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
