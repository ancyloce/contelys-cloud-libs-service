package httpapi_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ancyloce/contelys-cloud-libs-service/telemetry"
	"github.com/ancyloce/contelys-cloud-libs-service/template/internal/interfaces/httpapi"
)

// keptSpans keeps the in-memory exporter's spans across the provider's shutdown, which flushes.
type keptSpans struct{ *tracetest.InMemoryExporter }

func (keptSpans) Shutdown(context.Context) error { return nil }

func TestARouteOfTheContractIsTracedUnderItsOwnRoute(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	_, shutdown := telemetry.Setup("template", keptSpans{spans}, io.Discard)
	router := httpapi.New(func() (string, string) { return "3f2a9c1", "sha256:0" })
	router.Handle("GET /projects/{id}", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "/projects/42", nil)
	// The request of a caller that Edge traced.
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	router.ServeHTTP(httptest.NewRecorder(), request)
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	got := spans.GetSpans()
	if len(got) != 1 {
		t.Fatalf("spans = %d, want the route's one", len(got))
	}
	if id := got[0].SpanContext.TraceID().String(); id != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("trace = %s, want the caller's", id)
	}
	var route string
	for _, a := range got[0].Attributes {
		if a.Key == attribute.Key("http.route") {
			route = a.Value.AsString()
		}
	}
	if route != "/projects/{id}" {
		t.Errorf("http.route = %q, want /projects/{id}", route)
	}
}
