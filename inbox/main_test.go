package inbox_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ancyloce/contelys-cloud-libs-service/inbox"
	"github.com/ancyloce/contelys-cloud-libs-service/internal/pgtest"
)

// topic is the event family of the tests, Organization Access's, in three partitions.
const topic = "organization-access.organization"

func TestMain(m *testing.M) { pgtest.Main(m) }

// newDatabase creates a database of the test's own, as the subscribing service's database,
// applies the inbox's migrations to it and adds the table in which the test's effects land.
func newDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := pgtest.Database(t)
	if err := inbox.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := pool.Exec(context.Background(), "CREATE TABLE effects (event_id text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	return pool
}

// effects returns the event identifiers whose effect committed, in the order they committed.
func effects(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT event_id FROM effects ORDER BY ctid")
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

// record is an effect that writes the event's identifier into effects.
func record(e inbox.Event) inbox.Effect {
	return func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO effects (event_id) VALUES ($1)", e.ID)
		return err
	}
}

// kafka is an in-process Kafka cluster that holds the topic.
type kafka struct {
	cluster *kfake.Cluster
	addrs   []string
}

func newKafka(t *testing.T, partitions int32) *kafka {
	t.Helper()
	// Event Center creates the subscription's dead-letter topic.
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, topic), kfake.SeedTopics(1, deadLetters))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	return &kafka{cluster: cluster, addrs: cluster.ListenAddrs()}
}

// client returns a Kafka client of the cluster, with opts.
func (k *kafka) client(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(k.addrs...)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// event is a record as the outbox's relay publishes it, with the CloudEvents headers of ADR 0061.
type event struct {
	key, id, source, eventType, value string
	time                              time.Time
	traceparent                       string
}

// newEvent returns an event of Organization Access about key, with a new identifier.
func newEvent(key, value string) event {
	return event{
		key: key, id: uuid.Must(uuid.NewV7()).String(), source: "organization-access",
		eventType: "contelys.organization_access.v1.InvitationIssued", value: value,
		time: time.Date(2026, 11, 26, 9, 30, 0, 0, time.UTC),
	}
}

func (e event) record() *kgo.Record {
	headers := []kgo.RecordHeader{
		{Key: "ce_specversion", Value: []byte("1.0")},
		{Key: "ce_id", Value: []byte(e.id)},
		{Key: "ce_source", Value: []byte(e.source)},
		{Key: "ce_type", Value: []byte(e.eventType)},
		{Key: "ce_time", Value: []byte(e.time.Format(time.RFC3339Nano))},
		{Key: "content-type", Value: []byte("application/protobuf")},
	}
	if e.traceparent != "" {
		headers = append(headers, kgo.RecordHeader{Key: "traceparent", Value: []byte(e.traceparent)})
	}
	return &kgo.Record{Topic: topic, Key: []byte(e.key), Value: []byte(e.value), Headers: headers}
}

// publish produces records to the topic, in order.
func (k *kafka) publish(t *testing.T, records ...*kgo.Record) {
	t.Helper()
	producer := k.client(t, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	if err := producer.ProduceSync(context.Background(), records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// options are the options of a consumer of the test's subscription in k.
func (k *kafka) options() inbox.Options {
	return inbox.Options{
		Subscriber: "notifications", Subscription: "invitations", Topic: topic,
		Brokers: k.addrs, Backoff: 20 * time.Millisecond,
	}
}

// handled records every event that a handler was handed.
type handled struct {
	mu     sync.Mutex
	events []inbox.Event
}

func (h *handled) add(e inbox.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, e)
}

func (h *handled) all() []inbox.Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]inbox.Event(nil), h.events...)
}

func (h *handled) ids() []string {
	var ids []string
	for _, e := range h.all() {
		ids = append(ids, e.ID)
	}
	return ids
}

// run runs a consumer until stop is called or the test ends.
func run(t *testing.T, pool *pgxpool.Pool, opts inbox.Options, handle inbox.Handler) (stop func()) {
	t.Helper()
	consumer, err := inbox.New(pool, opts, handle)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		consumer.Run(ctx)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)
	return stop
}

// eventually waits until done holds, for at most timeout, and fails the test with what otherwise.
func eventually(t *testing.T, timeout time.Duration, done func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
