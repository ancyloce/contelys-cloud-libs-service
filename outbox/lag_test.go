package outbox_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/ancyloce/contelys-cloud/libs/service/outbox"
)

func TestTheLagOfTheOutboxIsASignal(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	lag := func() float64 {
		t.Helper()
		var collected metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &collected); err != nil {
			t.Fatal(err)
		}
		for _, scope := range collected.ScopeMetrics {
			for _, m := range scope.Metrics {
				if gauge, ok := m.Data.(metricdata.Gauge[float64]); ok && m.Name == "outbox.lag" && m.Unit == "s" {
					return gauge.DataPoints[0].Value
				}
			}
		}
		t.Fatal("no gauge outbox.lag in seconds")
		return 0
	}

	pool := newDatabase(t)
	k := newKafka(t)
	relay := newRelay(t, pool, k.addrs, "identity")
	if got := lag(); got != 0 {
		t.Errorf("lag of an empty outbox = %v, want 0", got)
	}

	commit(t, context.Background(), pool, outbox.Event{Topic: topic, Key: principal, Message: wrapperspb.String("waiting")})
	time.Sleep(1100 * time.Millisecond)
	if got := lag(); got < 1 {
		t.Errorf("lag with an event waiting for a second = %v, want at least 1", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		relay.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	k.consume(t, 1)
	deadline := time.Now().Add(5 * time.Second)
	for lag() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("lag after the relay published = %v, want 0", lag())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
