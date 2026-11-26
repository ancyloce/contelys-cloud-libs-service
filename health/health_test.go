package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ancyloce/contelys-cloud-libs-service/health"
)

// applied stands for the configuration store: it reports the revision and digest in force.
func applied() (revision, digest string) {
	return "3f2a9c1", "sha256:57da87c8a3a02ed8c35d1f2be456bee4c2b21ff4ee4ffef3fca6548a83d8388b"
}

func passing(context.Context) error { return nil }

func failing(context.Context) error { return errors.New("unreachable") }

// slow answers only after five seconds, as a check of an unreachable host might.
func slow(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return nil
	}
}

func get(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	return recorder
}

func TestLivenessAnswersWhileTheProcessRunsEvenWhenItIsNotReady(t *testing.T) {
	handler := health.Handler(applied, health.Check{Name: "database", Required: true, Run: failing})

	if got := get(t, handler, "/livez").Code; got != http.StatusOK {
		t.Errorf("GET /livez = %d, want 200", got)
	}
}

func TestReadinessFailsOnlyOnARequiredCheck(t *testing.T) {
	cases := []struct {
		name   string
		checks []health.Check
		status int
		body   string
	}{
		{
			name: "every check passes",
			checks: []health.Check{
				{Name: "database", Required: true, Run: passing},
				{Name: "keycloak", Run: passing},
			},
			status: http.StatusOK,
			body:   `{"ready":true,"checks":[{"name":"database","required":true,"ok":true},{"name":"keycloak","required":false,"ok":true}]}`,
		},
		{
			name: "a required check fails",
			checks: []health.Check{
				{Name: "database", Required: true, Run: failing},
				{Name: "keycloak", Run: passing},
			},
			status: http.StatusServiceUnavailable,
			body:   `{"ready":false,"checks":[{"name":"database","required":true,"ok":false},{"name":"keycloak","required":false,"ok":true}]}`,
		},
		{
			name: "only a check that is not required fails",
			checks: []health.Check{
				{Name: "database", Required: true, Run: passing},
				{Name: "keycloak", Run: failing},
			},
			status: http.StatusOK,
			body:   `{"ready":true,"checks":[{"name":"database","required":true,"ok":true},{"name":"keycloak","required":false,"ok":false}]}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			response := get(t, health.Handler(applied, c.checks...), "/readyz")
			if response.Code != c.status {
				t.Errorf("GET /readyz = %d, want %d", response.Code, c.status)
			}
			if got := response.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}
			if got := strings.TrimSpace(response.Body.String()); got != c.body {
				t.Errorf("body = %s\nwant %s", got, c.body)
			}
		})
	}
}

func TestRevisionAnswersTheAppliedRevisionAndDigest(t *testing.T) {
	response := get(t, health.Handler(applied), "/revision")

	if response.Code != http.StatusOK {
		t.Errorf("GET /revision = %d, want 200", response.Code)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	const body = `{"revision":"3f2a9c1","digest":"sha256:57da87c8a3a02ed8c35d1f2be456bee4c2b21ff4ee4ffef3fca6548a83d8388b"}`
	if got := strings.TrimSpace(response.Body.String()); got != body {
		t.Errorf("body = %s\nwant %s", got, body)
	}
}

func TestACheckThatDoesNotAnswerInTimeFailsWithoutDelayingReadiness(t *testing.T) {
	handler := health.Handler(applied,
		health.Check{Name: "database", Required: true, Run: passing},
		health.Check{Name: "keycloak", Run: slow},
	)

	start := time.Now()
	response := get(t, handler, "/readyz")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("GET /readyz took %s, want at most 2s", elapsed)
	}
	if response.Code != http.StatusOK {
		t.Errorf("GET /readyz = %d, want 200", response.Code)
	}
	const body = `{"ready":true,"checks":[{"name":"database","required":true,"ok":true},{"name":"keycloak","required":false,"ok":false}]}`
	if got := strings.TrimSpace(response.Body.String()); got != body {
		t.Errorf("body = %s\nwant %s", got, body)
	}
}

func TestAChecksAnswerInTimeCountsWhenAnEarlierCheckRunsOutOfTime(t *testing.T) {
	checks := []health.Check{{Name: "keycloak", Run: slow}}
	for i := range 16 {
		checks = append(checks, health.Check{Name: fmt.Sprintf("store-%d", i), Required: true, Run: passing})
	}

	response := get(t, health.Handler(applied, checks...), "/readyz")
	if response.Code != http.StatusOK {
		t.Errorf("GET /readyz = %d, want 200", response.Code)
	}
	var body struct {
		Checks []struct {
			Name string `json:"name"`
			OK   bool   `json:"ok"`
		} `json:"checks"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", response.Body.String(), err)
	}
	for _, check := range body.Checks[1:] {
		if !check.OK {
			t.Errorf("%s reported failed, though it passed at once", check.Name)
		}
	}
}
