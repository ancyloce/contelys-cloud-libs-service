package outbox_test

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/ancyloce/contelys-cloud/libs/service/outbox"
)

const principal = "01JDZ8Q3V7XK2M4N6P8R0T2W4Y"

// commit writes events in one transaction of its own, which it commits.
func commit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, events ...outbox.Event) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Write(ctx, tx, events...); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func headers(record *kgo.Record) map[string]string {
	got := map[string]string{}
	for _, h := range record.Headers {
		got[h.Key] = string(h.Value)
	}
	return got
}

func TestACommittedEventIsPublishedWithTheCloudEventsHeadersOfADR0061(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t)
	startRelay(t, pool, k, "identity")
	// The request that wrote the event was traced.
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36},
		SpanID:     trace.SpanID{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7},
		TraceFlags: trace.FlagsSampled,
	}))
	before := time.Now()

	commit(t, ctx, pool, outbox.Event{Topic: topic, Key: principal, Message: wrapperspb.String("erased")})

	record := k.consume(t, 1)[0]
	if string(record.Key) != principal {
		t.Errorf("key = %q, want the principal's identifier", record.Key)
	}
	var value wrapperspb.StringValue
	if err := proto.Unmarshal(record.Value, &value); err != nil || value.GetValue() != "erased" {
		t.Errorf("value = %x (%v), want the Protobuf message alone", record.Value, err)
	}
	got := headers(record)
	want := map[string]string{
		"ce_specversion": "1.0",
		"ce_source":      "identity",
		"ce_type":        "google.protobuf.StringValue",
		"content-type":   "application/protobuf",
		"traceparent":    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("header %s = %q, want %q", name, got[name], value)
		}
	}
	if id, err := uuid.Parse(got["ce_id"]); err != nil || id.Version() != 7 {
		t.Errorf("ce_id = %q, want a UUID v7", got["ce_id"])
	}
	if at, err := time.Parse(time.RFC3339Nano, got["ce_time"]); err != nil || at.Before(before.Add(-time.Second)) || at.After(time.Now()) {
		t.Errorf("ce_time = %q, want the time of the write in RFC 3339", got["ce_time"])
	}
}

func TestAnEventOfATransactionThatRollsBackIsNeverPublished(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t)
	startRelay(t, pool, k, "identity")
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.Write(ctx, tx, outbox.Event{Topic: topic, Key: principal, Message: wrapperspb.String("rolled back")}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	commit(t, ctx, pool, outbox.Event{Topic: topic, Key: principal, Message: wrapperspb.String("committed")})

	// The relay publishes in the order of writing, so a rolled-back event would come first.
	var value wrapperspb.StringValue
	if err := proto.Unmarshal(k.consume(t, 1)[0].Value, &value); err != nil || value.GetValue() != "committed" {
		t.Errorf("first record = %q (%v), want the committed event", value.GetValue(), err)
	}
}

func TestThePositionOfEachPublishedRecordIsRecorded(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t)
	startRelay(t, pool, k, "identity")
	ctx := context.Background()

	for _, thing := range []string{"01JDZ8Q3V7XK2M4N6P8R0T2W4Y", "01JDZ9A1B2C3D4E5F6G7H8J9KA", "01JDZ9M3N4P5Q6R7S8T9V0W1X2"} {
		commit(t, ctx, pool, outbox.Event{Topic: topic, Key: thing, Message: wrapperspb.String(thing)})
	}
	want := map[int32]int64{}
	for _, record := range k.consume(t, 3) {
		want[record.Partition] = max(want[record.Partition], record.Offset)
	}

	// The relay records the positions in the transaction that deletes the events, just after
	// Kafka has them, so wait for that transaction.
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := outbox.Positions(ctx, pool, topic)
		if err != nil {
			t.Fatalf("Positions: %v", err)
		}
		if maps.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("positions = %v, want the last offset of each partition %v", got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTheEventsAboutOneThingLeaveInTheOrderTheyWereWrittenWhileTwoInstancesRelay(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t)
	startRelay(t, pool, k, "identity")
	startRelay(t, pool, k, "identity")
	ctx := context.Background()

	for i := range 30 {
		commit(t, ctx, pool, outbox.Event{Topic: topic, Key: principal, Message: wrapperspb.String(strconv.Itoa(i))})
	}
	for i, record := range k.consume(t, 30)[:30] {
		var value wrapperspb.StringValue
		if err := proto.Unmarshal(record.Value, &value); err != nil || value.GetValue() != strconv.Itoa(i) {
			t.Fatalf("record %d = %q (%v), want %d: the events left out of order", i, value.GetValue(), err, i)
		}
	}
}

func TestWhileKafkaCannotBeReachedEventsWaitAndTheRelaySaysSo(t *testing.T) {
	pool := newDatabase(t)
	ctx := context.Background()
	relay, stop := runRelay(t, pool, []string{"127.0.0.1:1"}, "identity")

	commit(t, ctx, pool, outbox.Event{Topic: topic, Key: principal, Message: wrapperspb.String("waited")})
	deadline := time.Now().Add(15 * time.Second)
	for relay.Check(ctx) == nil {
		if time.Now().After(deadline) {
			t.Fatal("the relay's check still passes while Kafka cannot be reached")
		}
		time.Sleep(50 * time.Millisecond)
	}
	var waiting int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox").Scan(&waiting); err != nil || waiting != 1 {
		t.Errorf("events in the outbox = %d (%v), want the one event waiting", waiting, err)
	}
	stop()

	k := newKafka(t)
	live := startRelay(t, pool, k, "identity")
	var value wrapperspb.StringValue
	if err := proto.Unmarshal(k.consume(t, 1)[0].Value, &value); err != nil || value.GetValue() != "waited" {
		t.Errorf("record = %q (%v), want the event that waited", value.GetValue(), err)
	}
	if err := live.Check(ctx); err != nil {
		t.Errorf("Check once Kafka answers = %v, want nil", err)
	}
}

func TestARelayNeedsItsSourceItsBrokersAndAPause(t *testing.T) {
	pool := newDatabase(t)
	valid := outbox.RelayOptions{Source: "identity", Brokers: []string{"127.0.0.1:9092"}, Every: time.Second}
	for name, change := range map[string]func(*outbox.RelayOptions){
		"without a source": func(o *outbox.RelayOptions) { o.Source = "" },
		"without brokers":  func(o *outbox.RelayOptions) { o.Brokers = nil },
		"without a pause":  func(o *outbox.RelayOptions) { o.Every = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			opts := valid
			change(&opts)
			if _, err := outbox.NewRelay(pool, opts); err == nil {
				t.Error("NewRelay accepted the options")
			}
		})
	}
}

func TestInstancesThatStartTogetherMigrateTheOutboxOnce(t *testing.T) {
	for round := range 5 {
		pool := emptyDatabase(t)
		errs := make(chan error, 4)
		for range 4 {
			go func() { errs <- outbox.Migrate(context.Background(), pool) }()
		}
		for range 4 {
			if err := <-errs; err != nil {
				t.Errorf("round %d: Migrate = %v, want nil on every instance", round, err)
			}
		}
	}
}

func TestARoundThatKafkaLeavesUnansweredFailsTheCheckAndDoesNotHoldTheStop(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t)
	var swallowed atomic.Bool
	k.cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		// The first produce request gets no answer, as from a broker that stopped answering.
		return nil, nil, swallowed.CompareAndSwap(false, true)
	})
	relay, stop := runRelay(t, pool, k.addrs, "identity")
	ctx := context.Background()

	commit(t, ctx, pool, outbox.Event{Topic: topic, Key: principal, Message: wrapperspb.String("unanswered")})
	eventually(t, 10*time.Second, swallowed.Load, "the relay sent no produce request")
	eventually(t, 7*time.Second, func() bool { return relay.Check(ctx) != nil },
		"the relay's check still passes while its round waits for Kafka past its bound")

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("stopping the relay waited on the unanswered produce request")
	}
}

func TestAnEventThatKafkaRefusesNamesItsCauseAndIsPublishedOnceKafkaAccepts(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t)
	relay := startRelay(t, pool, k, "identity")
	ctx := context.Background()
	const missing = "identity.missing"

	commit(t, ctx, pool,
		outbox.Event{Topic: topic, Key: principal, Message: wrapperspb.String("before")},
		outbox.Event{Topic: missing, Key: principal, Message: wrapperspb.String("refused")},
		outbox.Event{Topic: topic, Key: principal, Message: wrapperspb.String("after")})
	eventually(t, 10*time.Second, func() bool {
		err := relay.Check(ctx)
		return err != nil && strings.Contains(err.Error(), "UNKNOWN_TOPIC_OR_PARTITION")
	}, "the relay's check does not name the topic that Kafka does not know")

	k.createTopic(t, missing)
	eventually(t, 20*time.Second, func() bool {
		positions, err := outbox.Positions(ctx, pool, missing)
		return err == nil && len(positions) == 1
	}, "the refused event was not published once its topic existed")
	var values []string
	for _, record := range k.consume(t, 2) {
		var value wrapperspb.StringValue
		if err := proto.Unmarshal(record.Value, &value); err != nil {
			t.Fatal(err)
		}
		values = append(values, value.GetValue())
	}
	if !slices.Contains(values, "before") || !slices.Contains(values, "after") {
		t.Errorf("records of %s = %v, want before and after", topic, values)
	}
}

func TestAnInstanceThatFollowsReportsHealthyOnceAnotherPublishes(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t)
	first := startRelay(t, pool, k, "identity")
	second := startRelay(t, pool, k, "identity")
	ctx := context.Background()
	const missing = "identity.missing"

	commit(t, ctx, pool, outbox.Event{Topic: missing, Key: principal, Message: wrapperspb.String("refused")})
	// The instances trade the lock after each failed round, so each comes to fail.
	var firstFailed, secondFailed bool
	eventually(t, 30*time.Second, func() bool {
		firstFailed = firstFailed || first.Check(ctx) != nil
		secondFailed = secondFailed || second.Check(ctx) != nil
		return firstFailed && secondFailed
	}, "both instances never failed while Kafka refused the event")

	k.createTopic(t, missing)
	eventually(t, 20*time.Second, func() bool {
		positions, err := outbox.Positions(ctx, pool, missing)
		return err == nil && len(positions) == 1
	}, "the event was not published once its topic existed")
	eventually(t, 5*time.Second, func() bool { return first.Check(ctx) == nil && second.Check(ctx) == nil },
		"an instance that follows still reports the failure of a round it no longer leads")
}

func TestWhenTheLockSessionEndsTheInstancesDoNotBothGoOnPublishing(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t)
	startRelay(t, pool, k, "identity")
	startRelay(t, pool, k, "identity")
	ctx := context.Background()

	// End the session that holds the relay's lock, as a network blip or idle_session_timeout does.
	var ended int
	eventually(t, 5*time.Second, func() bool {
		err := pool.QueryRow(ctx, `SELECT count(pg_terminate_backend(pid)) FROM pg_locks
			WHERE locktype = 'advisory' AND granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&ended)
		return err == nil && ended > 0
	}, "no instance took the relay's lock")

	const events = 200
	for i := range events {
		commit(t, ctx, pool, outbox.Event{Topic: topic, Key: principal, Message: wrapperspb.String(strconv.Itoa(i))})
	}
	records := k.consumeAll(t, time.Second)
	distinct := map[string]bool{}
	for _, record := range records {
		distinct[headers(record)["ce_id"]] = true
	}
	if len(distinct) != events {
		t.Fatalf("distinct events = %d, want %d", len(distinct), events)
	}
	// The session ended before the events were written, so no round was in flight then, and at
	// least once leaves room only for a stray duplicate.
	if len(records) > events+20 {
		t.Errorf("records = %d for %d events: both instances went on publishing", len(records), events)
	}
}
