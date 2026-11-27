package inbox_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/ancyloce/contelys-cloud-libs-service/inbox"
)

// lagReader installs a meter provider for the test and returns what the gauge inbox.lag reports
// for the test's subscription, and whether it reports anything.
func lagReader(t *testing.T) func() (int64, bool) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	return func() (int64, bool) {
		t.Helper()
		var collected metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &collected); err != nil {
			t.Fatal(err)
		}
		for _, scope := range collected.ScopeMetrics {
			for _, m := range scope.Metrics {
				if gauge, ok := m.Data.(metricdata.Gauge[int64]); ok && m.Name == "inbox.lag" {
					for _, point := range gauge.DataPoints {
						if subscription, _ := point.Attributes.Value("subscription"); subscription.AsString() == "invitations" {
							return point.Value, true
						}
					}
				}
			}
		}
		return 0, false
	}
}

func skipEvery(context.Context, inbox.Event) (inbox.Effect, error) { return nil, nil }

// A consumer that holds no partition, as while Kafka cannot be reached, reports no lag rather than
// a lag of zero: Notifications reopens sending after a restore only once the lag of
// identity.principal is zero.
func TestAConsumerWithoutItsPartitionsReportsNoLag(t *testing.T) {
	lag := lagReader(t)
	pool := newDatabase(t)
	opts := inbox.Options{
		Subscriber: "notifications", Subscription: "invitations", Topic: topic,
		Brokers: []string{"127.0.0.1:1"}, Backoff: 20 * time.Millisecond,
	}
	run(t, pool, opts, skipEvery)

	time.Sleep(500 * time.Millisecond)

	if got, reported := lag(); reported {
		t.Errorf("lag = %d while Kafka cannot be reached, want no value", got)
	}
}

// A consumer of partitions that hold nothing yet reports a lag of zero once it holds them.
func TestAConsumerOfAnEmptyTopicReportsNoLagOnceItHoldsItsPartitions(t *testing.T) {
	lag := lagReader(t)
	pool := newDatabase(t)
	k := newKafka(t, 3)
	run(t, pool, k.options(), skipEvery)

	eventually(t, 10*time.Second, func() bool {
		got, reported := lag()
		return reported && got == 0
	}, "the lag of an empty topic never showed as 0")
}
