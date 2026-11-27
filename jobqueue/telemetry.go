package jobqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// instrumentation names the tracer and the meter of jobs.
const instrumentation = "github.com/ancyloce/contelys-cloud-libs-service/jobqueue"

// telemetry carries the trace of whoever inserts a job in the job's metadata, and works the job
// in a trace of its own whose consumer span links to the inserter's span, as issue 11 of the
// phase 1 platform asks for an asynchronous step. It times each job in the histogram
// messaging.process.duration. Names follow the OpenTelemetry conventions for messaging
// (ADR 0026), with the queue as the destination.
type telemetry struct {
	river.MiddlewareDefaults
	tracer   trace.Tracer
	duration metric.Float64Histogram
}

func newTelemetry() (*telemetry, error) {
	duration, err := otel.Meter(instrumentation).Float64Histogram("messaging.process.duration",
		metric.WithUnit("s"),
		metric.WithDescription("The time that a job took to be worked."),
		// The boundaries that the conventions advise for this histogram.
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10))
	if err != nil {
		return nil, err
	}
	return &telemetry{tracer: otel.Tracer(instrumentation), duration: duration}, nil
}

// traceFields are the keys of a job's metadata that carry the inserter's trace.
type traceFields struct {
	Traceparent string `json:"traceparent,omitempty"`
	Tracestate  string `json:"tracestate,omitempty"`
}

func (m *telemetry) InsertMany(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	if len(carrier) == 0 {
		return doInner(ctx)
	}
	for _, params := range manyParams {
		metadata, err := withTrace(params.Metadata, carrier)
		if err != nil {
			return nil, fmt.Errorf("jobqueue: the metadata of a job of kind %s: %w", params.Kind, err)
		}
		params.Metadata = metadata
	}
	return doInner(ctx)
}

// withTrace returns metadata, a JSON object, with the trace of carrier added.
func withTrace(metadata []byte, carrier propagation.MapCarrier) ([]byte, error) {
	fields := map[string]any{}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &fields); err != nil {
			return nil, err
		}
	}
	for key, value := range carrier {
		fields[key] = value
	}
	return json.Marshal(fields)
}

func (m *telemetry) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) (err error) {
	// The attributes of the histogram, which the span carries too.
	measured := []attribute.KeyValue{
		attribute.String("messaging.system", "river"),
		attribute.String("messaging.operation.name", "process"),
		attribute.String("messaging.destination.name", job.Queue),
		attribute.String("messaging.river.job.kind", job.Kind),
	}
	opts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(measured...),
		trace.WithAttributes(
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.message.id", strconv.FormatInt(job.ID, 10)),
			attribute.Int("messaging.river.job.attempt", job.Attempt),
		),
	}
	var fields traceFields
	if json.Unmarshal(job.Metadata, &fields) == nil && fields.Traceparent != "" {
		carrier := propagation.MapCarrier{"traceparent": fields.Traceparent, "tracestate": fields.Tracestate}
		inserter := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), carrier))
		if inserter.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: inserter}))
		}
	}
	ctx, span := m.tracer.Start(ctx, "process "+job.Queue, opts...)
	start := time.Now()
	defer func() {
		// River recovers a worker's panic only after it has left the middleware, so the panic is
		// recorded here and goes on to River.
		recovered := recover()
		var errorType, description string
		var snoozed *rivertype.JobSnoozeError
		switch {
		case recovered != nil:
			errorType, description = "panic", fmt.Sprint(recovered)
		case errors.As(err, &snoozed):
			// A worker that snoozes its job puts it off on purpose: the job has not failed.
		case err != nil:
			errorType, description = fmt.Sprintf("%T", err), err.Error()
		}
		if errorType != "" {
			span.SetAttributes(attribute.String("error.type", errorType))
			span.SetStatus(codes.Error, description)
			measured = append(measured, attribute.String("error.type", errorType))
		}
		m.duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(measured...))
		span.End()
		if recovered != nil {
			panic(recovered)
		}
	}()
	return doInner(ctx)
}
