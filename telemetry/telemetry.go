// Package telemetry makes a domain service trace with W3C Trace Context and write log lines that
// carry the trace identifier, so that a log line can be found from a trace (issue 11 of the
// phase 1 platform).
package telemetry

import (
	"context"
	"io"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Setup makes the process trace with W3C Trace Context. It installs the propagator and a tracer
// provider whose resource names the process as service, its service.name, and which hands
// finished spans to exporter in batches, or drops them when exporter is nil.
// It returns a logger that writes one JSON line to out for each record, with the trace_id and
// span_id of the context that the record is logged with, and the function that flushes the
// spans and stops the provider.
func Setup(service string, exporter sdktrace.SpanExporter, out io.Writer) (*slog.Logger, func(context.Context) error) {
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return slog.New(traced{slog.NewJSONHandler(out, nil)}), provider.Shutdown
}

// Handler continues the trace that an incoming request's traceparent header names, or starts
// one, for every request that next serves. operation names the server's spans.
func Handler(next http.Handler, operation string) http.Handler {
	return otelhttp.NewHandler(next, operation)
}

// Transport makes every call through base carry the trace of the request's context in a
// traceparent header, under a client span of its own.
func Transport(base http.RoundTripper) http.RoundTripper {
	return otelhttp.NewTransport(base)
}

// traced adds the trace and span identifiers of a record's context to the record.
type traced struct{ slog.Handler }

func (h traced) Handle(ctx context.Context, record slog.Record) error {
	if span := trace.SpanContextFromContext(ctx); span.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", span.TraceID().String()),
			slog.String("span_id", span.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, record)
}

func (h traced) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traced{h.Handler.WithAttrs(attrs)}
}

func (h traced) WithGroup(name string) slog.Handler {
	return traced{h.Handler.WithGroup(name)}
}
