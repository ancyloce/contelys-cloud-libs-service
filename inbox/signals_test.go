package inbox_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/ancyloce/contelys-cloud-libs-service/inbox"
)

func TestTheLagOfASubscriptionIsASignal(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	lag := func() int64 {
		t.Helper()
		var collected metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &collected); err != nil {
			t.Fatal(err)
		}
		for _, scope := range collected.ScopeMetrics {
			for _, m := range scope.Metrics {
				if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok && m.Name == "inbox.lag" {
					for _, point := range gauge.DataPoints {
						if subscription, _ := point.Attributes.Value("subscription"); subscription == attribute.StringValue("invitations") {
							if point.Attributes.Len() != 1 {
								t.Errorf("attributes = %v, want the subscription alone", point.Attributes.ToSlice())
							}
							return point.Value
						}
					}
				}
			}
		}
		return -1
	}

	pool := newDatabase(t)
	k := newKafka(t, 1)
	var unreachable atomic.Bool
	unreachable.Store(true)
	run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		if unreachable.Load() {
			return nil, errors.New("the neighbour cannot be reached")
		}
		return record(e), nil
	})

	k.publish(t, newEvent(invitation, "1").record(), newEvent(invitation, "2").record(), newEvent(invitation, "3").record())
	eventually(t, 10*time.Second, func() bool { return lag() == 3 }, "the lag of three waiting records never showed as 3")
	unreachable.Store(false)
	eventually(t, 10*time.Second, func() bool { return lag() == 0 }, "the lag never fell to 0 once the records committed")
}

// buffer is a log output that tests read while a consumer writes to it.
type buffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestNoLogLineCarriesARecordsKeyOrValue(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t, 1)
	var logs buffer
	opts := k.options()
	opts.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	const key, value = "key-0123456789", "value-0123456789"
	headless := newEvent(key, value)
	failing, valid := newEvent(key, value), newEvent(key, value)
	var failures atomic.Int32
	run(t, pool, opts, func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		if e.ID == failing.id && failures.Add(1) == 1 {
			return nil, errors.New("the neighbour cannot be reached")
		}
		return record(e), nil
	})

	k.publish(t, without(headless, "ce_id"), failing.record(), valid.record())

	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 2 }, "the effects never committed")
	out := logs.String()
	if !strings.Contains(out, "dead") || !strings.Contains(out, "the neighbour cannot be reached") {
		t.Errorf("logs = %s, want the dead letter and the failure reported", out)
	}
	if strings.Contains(out, key) || strings.Contains(out, value) {
		t.Errorf("logs = %s, want neither the record's key nor its value", out)
	}
}
