package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ancyloce/contelys-cloud-libs-service/telemetry"
)

// keptSpans is the test's exporter. The in-memory exporter forgets its spans when it is shut
// down, so keptSpans ignores the shutdown, and the spans can be read after the provider has
// flushed them.
type keptSpans struct{ *tracetest.InMemoryExporter }

func (keptSpans) Shutdown(context.Context) error { return nil }

// service is a domain service under test, wrapped by the library: its finished spans, its log
// lines, the traceparent header of each request it served, and its address.
type service struct {
	spans    *tracetest.InMemoryExporter
	logs     *bytes.Buffer
	received chan string
	url      string
	shutdown func(context.Context) error
}

func start(t *testing.T) *service {
	t.Helper()
	s := &service{
		spans:    tracetest.NewInMemoryExporter(),
		logs:     &bytes.Buffer{},
		received: make(chan string, 1),
	}
	logger, shutdown := telemetry.Setup("notifications", keptSpans{s.spans}, s.logs)
	s.shutdown = shutdown
	t.Cleanup(func() { _ = shutdown(context.Background()) })
	server := httptest.NewServer(telemetry.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.InfoContext(r.Context(), "delivery accepted")
		s.received <- r.Header.Get("traceparent")
	}), "deliveries"))
	t.Cleanup(server.Close)
	s.url = server.URL
	return s
}

// flush makes every finished span visible.
func (s *service) flush(t *testing.T) {
	t.Helper()
	if err := s.shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestAnIncomingTraceContinuesAndItsLogLinesCarryTheTraceIdentifier(t *testing.T) {
	s := start(t)

	request, err := http.NewRequest(http.MethodGet, s.url, nil)
	if err != nil {
		t.Fatal(err)
	}
	// A caller outside the library, such as Edge, sent this W3C Trace Context header.
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	<-s.received
	s.flush(t)

	got := s.spans.GetSpans()
	if len(got) != 1 {
		t.Fatalf("spans = %d, want the server's one", len(got))
	}
	server := got[0]
	if id := server.SpanContext.TraceID().String(); id != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("trace = %s, want the caller's 4bf92f3577b34da6a3ce929d0e0e4736", id)
	}
	if id := server.Parent.SpanID().String(); id != "00f067aa0ba902b7" {
		t.Errorf("parent span = %s, want the caller's 00f067aa0ba902b7", id)
	}

	var line struct {
		Msg     string `json:"msg"`
		TraceID string `json:"trace_id"`
		SpanID  string `json:"span_id"`
	}
	if err := json.Unmarshal(s.logs.Bytes(), &line); err != nil {
		t.Fatalf("log line %q: %v", s.logs.String(), err)
	}
	if line.Msg != "delivery accepted" || line.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("log line = %+v, want the message with trace 4bf92f3577b34da6a3ce929d0e0e4736", line)
	}
	if line.SpanID != server.SpanContext.SpanID().String() {
		t.Errorf("log span = %s, want the server's %s", line.SpanID, server.SpanContext.SpanID())
	}
}

func TestAnOutgoingCallCarriesTheTraceOfItsContext(t *testing.T) {
	s := start(t)

	ctx, sending := otel.Tracer("test").Start(context.Background(), "send a notification")
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: telemetry.Transport(http.DefaultTransport)}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	received := <-s.received
	sending.End()
	s.flush(t)

	traceID := sending.SpanContext().TraceID().String()
	if !strings.HasPrefix(received, "00-"+traceID+"-") {
		t.Errorf("traceparent received = %q, want version 00 with trace %s", received, traceID)
	}
	var served bool
	for _, span := range s.spans.GetSpans() {
		if span.SpanKind == trace.SpanKindServer {
			served = true
			if got := span.SpanContext.TraceID().String(); got != traceID {
				t.Errorf("server's trace = %s, want the caller's %s", got, traceID)
			}
		}
	}
	if !served {
		t.Error("no server span was recorded")
	}
}
