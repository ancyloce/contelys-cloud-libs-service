package tokens_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/identitytest"
	"github.com/ancyloce/contelys-cloud-libs-service/tokens"
)

// problem is the shared contracts' problem body, openapi/shared/problem.yaml.
type problem struct {
	Type          string `json:"type"`
	Title         string `json:"title"`
	Status        int    `json:"status"`
	Code          string `json:"code"`
	RequestID     string `json:"requestId"`
	CorrelationID string `json:"correlationId"`
}

// call sends GET / with the token, if any, and the request ID that Edge sets, to handler.
func call(t *testing.T, handler http.Handler, ctx context.Context, token string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	request.Header.Set("X-Request-Id", "req-7f3a")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

// refusal checks that response is the problem body with status and code.
func refusal(t *testing.T, response *httptest.ResponseRecorder, status int, code string) problem {
	t.Helper()
	if response.Code != status {
		t.Errorf("status = %d, want %d", response.Code, status)
	}
	if got := response.Header().Get("Content-Type"); got != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", got)
	}
	var body problem
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", response.Body.String(), err)
	}
	if body.Status != status || body.Code != code || body.Type != "about:blank" || body.Title == "" {
		t.Errorf("problem = %+v, want status %d and code %s", body, status, code)
	}
	if body.RequestID != "req-7f3a" {
		t.Errorf("requestId = %q, want the X-Request-Id req-7f3a", body.RequestID)
	}
	return body
}

// served answers 200 with what the token said, read from the request's context.
var served = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	if caller, ok := tokens.InternalFrom(r.Context()); ok {
		_, _ = w.Write([]byte(caller.Principal))
	}
	if caller, ok := tokens.WorkloadFrom(r.Context()); ok {
		_, _ = w.Write([]byte(caller.Caller))
	}
})

func TestAnInternalRouteServesOnlyARequestWithAValidToken(t *testing.T) {
	id := identitytest.New(t)
	key := identitytest.NewKey(t, "internal-2026-11")
	id.Publish(key)
	handler := newVerifier(id).RequireInternal(served)
	ctx := context.Background()

	response := call(t, handler, ctx, identitytest.Sign(t, key, "internal+jwt", internalClaims()))
	if response.Code != http.StatusOK || response.Body.String() != "01JDZ8Q3V7XK2M4N6P8R0T2W4Y" {
		t.Errorf("with a valid token = %d %q, want 200 and the principal", response.Code, response.Body.String())
	}

	body := refusal(t, call(t, handler, ctx, ""), http.StatusUnauthorized, "token_invalid")
	if body.CorrelationID != "req-7f3a" {
		t.Errorf("correlationId without a trace = %q, want the request ID", body.CorrelationID)
	}
	refusal(t, call(t, handler, ctx, "not.a.token"), http.StatusUnauthorized, "token_invalid")

	// Within a trace, the problem body correlates by the trace's identifier.
	traced := trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36},
		SpanID:  trace.SpanID{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7},
	}))
	body = refusal(t, call(t, handler, traced, ""), http.StatusUnauthorized, "token_invalid")
	if body.CorrelationID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("correlationId in a trace = %q, want the trace ID", body.CorrelationID)
	}
}

func TestARouteAnswers503WhenTheKeySetCannotBeEstablished(t *testing.T) {
	id := identitytest.New(t)
	key := identitytest.NewKey(t, "internal-2026-11")
	id.Publish(key)
	id.SetDown(true)
	handler := newVerifier(id).RequireInternal(served)

	refusal(t, call(t, handler, context.Background(), identitytest.Sign(t, key, "internal+jwt", internalClaims())),
		http.StatusServiceUnavailable, "unavailable")
}

func TestAWorkloadRouteServesOnlyAnOperationThatTheTokenLists(t *testing.T) {
	id := identitytest.New(t)
	key := identitytest.NewKey(t, "workload-2026-11")
	id.Publish(key)
	verifier := newVerifier(id)
	ctx := context.Background()
	raw := identitytest.Sign(t, key, "workload+jwt", workloadClaims(nil))

	response := call(t, verifier.RequireWorkload("getProject", served), ctx, raw)
	if response.Code != http.StatusOK || response.Body.String() != "notifications" {
		t.Errorf("for a listed operation = %d %q, want 200 and the caller", response.Code, response.Body.String())
	}
	refusal(t, call(t, verifier.RequireWorkload("deleteProject", served), ctx, raw),
		http.StatusForbidden, "operation_not_permitted")
	refusal(t, call(t, verifier.RequireWorkload("getProject", served), ctx, ""),
		http.StatusUnauthorized, "token_invalid")
}
