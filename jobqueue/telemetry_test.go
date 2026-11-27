package jobqueue_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"maps"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/riverqueue/river"

	"github.com/ancyloce/contelys-cloud-libs-service/jobqueue"
)

// traced installs a tracer provider that keeps every ended span, as telemetry.Setup installs the
// service's, and returns it with the kept spans.
func traced(t *testing.T) (trace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()
	spans := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	return provider, spans
}

// workSpan waits for the span in which the job id was worked.
func workSpan(t *testing.T, spans *tracetest.InMemoryExporter, id int64) tracetest.SpanStub {
	t.Helper()
	var found tracetest.SpanStub
	eventually(t, 10*time.Second, func() bool {
		for _, span := range spans.GetSpans() {
			for _, kv := range span.Attributes {
				if kv.Key == "messaging.message.id" && kv.Value.AsString() == strconv.FormatInt(id, 10) {
					found = span
					return true
				}
			}
		}
		return false
	}, "no span of the job's work")
	return found
}

// hasAttributes fails the test unless span carries each of want.
func hasAttributes(t *testing.T, span tracetest.SpanStub, want ...attribute.KeyValue) {
	t.Helper()
	got := attribute.NewSet(span.Attributes...)
	for _, kv := range want {
		if value, ok := got.Value(kv.Key); !ok || value != kv.Value {
			t.Errorf("span %q: %s = %v, want %v", span.Name, kv.Key, value.Emit(), kv.Value.Emit())
		}
	}
}

// measured installs a meter provider whose metrics the test reads, as a service installs its own,
// and returns its reader.
func measured(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	return reader
}

// durations returns the counts of the histogram messaging.process.duration, by the error.type of
// each data point, "" for jobs that succeeded, and fails the test unless each point carries want.
func durations(t *testing.T, reader *sdkmetric.ManualReader, want ...attribute.KeyValue) map[string]uint64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]uint64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			histogram, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || m.Name != "messaging.process.duration" {
				continue
			}
			if m.Unit != "s" {
				t.Errorf("unit %q, want s", m.Unit)
			}
			for _, point := range histogram.DataPoints {
				for _, kv := range want {
					if value, ok := point.Attributes.Value(kv.Key); !ok || value != kv.Value {
						t.Errorf("data point %v: %s = %v, want %v", point.Attributes.ToSlice(), kv.Key, value.Emit(), kv.Value.Emit())
					}
				}
				errorType, _ := point.Attributes.Value("error.type")
				counts[errorType.AsString()] += point.Count
			}
		}
	}
	return counts
}

func TestAFailedJobIsReportedAndEveryJobIsTimed(t *testing.T) {
	_, spans := traced(t)
	reader := measured(t)
	pool := newDatabase(t)
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(_ context.Context, job *river.Job[greeting]) error {
		if job.Args.Name == "" {
			return errors.New("a greeting needs a name")
		}
		return nil
	}))
	client := start(t, pool, jobqueue.Options{
		Workers: workers,
		Queues:  map[string]jobqueue.Queue{river.QueueDefault: {Concurrency: 1}},
	})

	succeeded, err := client.Insert(context.Background(), greeting{Name: "Ada"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	failed, err := client.Insert(context.Background(), greeting{}, &river.InsertOpts{MaxAttempts: 1})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	if span := workSpan(t, spans, succeeded.Job.ID); span.Status.Code != codes.Unset {
		t.Errorf("the span of a job that succeeded has status %v, want none", span.Status)
	}
	span := workSpan(t, spans, failed.Job.ID)
	if span.Status.Code != codes.Error || span.Status.Description != "a greeting needs a name" {
		t.Errorf("the span of a job that failed has status %v, want an error with the worker's error", span.Status)
	}
	hasAttributes(t, span, attribute.String("error.type", "*errors.errorString"))

	counts := durations(t, reader,
		attribute.String("messaging.system", "river"),
		attribute.String("messaging.operation.name", "process"),
		attribute.String("messaging.destination.name", river.QueueDefault),
		attribute.String("messaging.river.job.kind", "greeting"),
	)
	if want := map[string]uint64{"": 1, "*errors.errorString": 1}; !maps.Equal(counts, want) {
		t.Errorf("durations by error.type = %v, want %v", counts, want)
	}
}

func TestAJobThatIsSnoozedIsNotReportedAsFailed(t *testing.T) {
	_, spans := traced(t)
	reader := measured(t)
	pool := newDatabase(t)
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(context.Context, *river.Job[greeting]) error {
		return river.JobSnooze(time.Hour)
	}))
	client := start(t, pool, jobqueue.Options{
		Workers: workers,
		Queues:  map[string]jobqueue.Queue{river.QueueDefault: {Concurrency: 1}},
	})

	inserted, err := client.Insert(context.Background(), greeting{Name: "Ada"}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	span := workSpan(t, spans, inserted.Job.ID)
	if span.Status.Code != codes.Unset {
		t.Errorf("the span of a job that was snoozed has status %v, want none", span.Status)
	}
	if counts := durations(t, reader); !maps.Equal(counts, map[string]uint64{"": 1}) {
		t.Errorf("durations by error.type = %v, want one without an error", counts)
	}
}

func TestAJobThatPanicsIsReportedAsFailed(t *testing.T) {
	_, spans := traced(t)
	reader := measured(t)
	pool := newDatabase(t)
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(context.Context, *river.Job[greeting]) error {
		panic("the worker has a bug")
	}))
	client := start(t, pool, jobqueue.Options{
		Workers: workers,
		Queues:  map[string]jobqueue.Queue{river.QueueDefault: {Concurrency: 1}},
	})

	inserted, err := client.Insert(context.Background(), greeting{Name: "Ada"}, &river.InsertOpts{MaxAttempts: 1})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	span := workSpan(t, spans, inserted.Job.ID)
	if span.Status.Code != codes.Error || span.Status.Description != "the worker has a bug" {
		t.Errorf("the span of a job that panicked has status %v, want an error with the panic's value", span.Status)
	}
	hasAttributes(t, span, attribute.String("error.type", "panic"))
	if counts := durations(t, reader); !maps.Equal(counts, map[string]uint64{"panic": 1}) {
		t.Errorf("durations by error.type = %v, want one panic", counts)
	}
}

// lines is a writer of log lines that several goroutines share.
type lines struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lines) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func TestRiversLinesGoToTheDefaultLoggerWhenNoneIsGiven(t *testing.T) {
	// A service makes the logger of telemetry.Setup the default.
	var written lines
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&written, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	pool := newDatabase(t)
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(context.Context, *river.Job[greeting]) error {
		panic("the worker has a bug")
	}))
	client := start(t, pool, jobqueue.Options{
		Workers: workers,
		Queues:  map[string]jobqueue.Queue{river.QueueDefault: {Concurrency: 1}},
	})
	if _, err := client.Insert(context.Background(), greeting{Name: "Ada"}, &river.InsertOpts{MaxAttempts: 1}); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// River logs a recovered panic as an error.
	eventually(t, 10*time.Second, func() bool { return strings.Contains(written.String(), "the worker has a bug") },
		"River's line about the panic did not reach the default logger")
}

func TestAJobIsTracedFromTheRequestThatInsertedIt(t *testing.T) {
	tracers, spans := traced(t)
	pool := newDatabase(t)
	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(context.Context, *river.Job[greeting]) error { return nil }))
	client := start(t, pool, jobqueue.Options{
		Workers: workers,
		Queues:  map[string]jobqueue.Queue{river.QueueDefault: {Concurrency: 1}},
	})

	ctx, request := tracers.Tracer("test").Start(context.Background(), "POST /invitations")
	inserted, err := client.Insert(ctx, greeting{Name: "Ada"}, nil)
	request.End()
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	span := workSpan(t, spans, inserted.Job.ID)
	if span.Name != "process default" || span.SpanKind != trace.SpanKindConsumer {
		t.Errorf("span %q of kind %v, want the consumer span \"process default\"", span.Name, span.SpanKind)
	}
	if span.SpanContext.TraceID() == request.SpanContext().TraceID() {
		t.Error("the job's work continues the request's trace, want a trace of its own")
	}
	if len(span.Links) != 1 || span.Links[0].SpanContext.TraceID() != request.SpanContext().TraceID() ||
		span.Links[0].SpanContext.SpanID() != request.SpanContext().SpanID() {
		t.Errorf("links = %v, want one to the span of the request that inserted the job", span.Links)
	}
	hasAttributes(t, span,
		attribute.String("messaging.system", "river"),
		attribute.String("messaging.operation.name", "process"),
		attribute.String("messaging.operation.type", "process"),
		attribute.String("messaging.destination.name", river.QueueDefault),
		attribute.String("messaging.river.job.kind", "greeting"),
		attribute.Int("messaging.river.job.attempt", 1),
	)
}
