package inbox_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/ancyloce/contelys-cloud-libs-service/inbox"
)

// traced installs a tracer provider that keeps every ended span, as telemetry.Setup installs the
// service's, and returns the kept spans.
func traced(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	spans := tracetest.NewInMemoryExporter()
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans)))
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	return spans
}

// consumerSpan waits for the span in which the event id was consumed.
func consumerSpan(t *testing.T, spans *tracetest.InMemoryExporter, id string) tracetest.SpanStub {
	t.Helper()
	var found tracetest.SpanStub
	eventually(t, 10*time.Second, func() bool {
		for _, span := range spans.GetSpans() {
			for _, kv := range span.Attributes {
				if kv.Key == "messaging.message.id" && kv.Value.AsString() == id {
					found = span
					return true
				}
			}
		}
		return false
	}, "no span of the event's consumption")
	return found
}

func TestAnEventIsConsumedInATraceOfItsOwnThatLinksToTheProducersSpan(t *testing.T) {
	spans := traced(t)
	pool := newDatabase(t)
	k := newKafka(t, 1)
	issued, untraced := newEvent(invitation, "issued"), newEvent(invitation, "untraced")
	issued.traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var handlers [2]atomic.Value
	run(t, pool, k.options(), func(ctx context.Context, e inbox.Event) (inbox.Effect, error) {
		i := 0
		if e.ID == untraced.id {
			i = 1
		}
		handlers[i].Store(trace.SpanContextFromContext(ctx))
		return record(e), nil
	})
	k.publish(t, issued.record(), untraced.record())

	span := consumerSpan(t, spans, issued.id)
	if span.Name != "process "+topic || span.SpanKind != trace.SpanKindConsumer {
		t.Errorf("span = %q of kind %v, want the consumer span process %s", span.Name, span.SpanKind, topic)
	}
	if span.Parent.IsValid() || span.SpanContext.TraceID().String() == "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("span is in trace %s under %s, want the root of a trace of its own", span.SpanContext.TraceID(), span.Parent.SpanID())
	}
	if len(span.Links) != 1 || span.Links[0].SpanContext.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" ||
		span.Links[0].SpanContext.SpanID().String() != "00f067aa0ba902b7" {
		t.Errorf("links = %v, want one to the producer's span from traceparent", span.Links)
	}
	want := map[attribute.Key]string{
		"messaging.system":              "kafka",
		"messaging.operation.name":      "process",
		"messaging.operation.type":      "process",
		"messaging.destination.name":    topic,
		"messaging.consumer.group.name": "notifications.invitations",
	}
	for _, kv := range span.Attributes {
		if value, ok := want[kv.Key]; ok && kv.Value.AsString() == value {
			delete(want, kv.Key)
		}
	}
	if len(want) != 0 {
		t.Errorf("the span lacks %v", want)
	}
	if handler, _ := handlers[0].Load().(trace.SpanContext); handler.SpanID() != span.SpanContext.SpanID() {
		t.Errorf("the handler ran under span %s, want the consumer span %s", handler.SpanID(), span.SpanContext.SpanID())
	}

	if untracedSpan := consumerSpan(t, spans, untraced.id); untracedSpan.Parent.IsValid() || len(untracedSpan.Links) != 0 {
		t.Errorf("an event without traceparent: parent %v and links %v, want a root and no link", untracedSpan.Parent, untracedSpan.Links)
	}
}
