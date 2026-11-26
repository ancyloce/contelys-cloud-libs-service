package outbox

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const (
	// relayLock is the PostgreSQL advisory lock that the active relay of a service holds. Locks
	// belong to one database, so each service has its own. Its value spells "contelys" in ASCII.
	relayLock int64 = 0x636f6e74656c7973
	// batchSize is the most events that one round of the relay publishes.
	batchSize = 100
	// publishTimeout bounds how long one round waits for Kafka before it gives up. franz-go keeps
	// waiting for a request it has already sent, because failing it could reorder the records,
	// so Check also fails once a produce has waited longer than this.
	publishTimeout = 5 * time.Second
)

// RelayOptions configure a relay.
type RelayOptions struct {
	// Source is the slug of the publishing deployable, which ce_source carries.
	Source string
	// Brokers are the addresses of Kafka's brokers.
	Brokers []string
	// TLS, when set, is the configuration of the connections to Kafka.
	TLS *tls.Config
	// Every is how long the relay waits before it looks for events again.
	Every time.Duration
}

// Relay publishes the outbox's events to Kafka at least once.
type Relay struct {
	pool   *pgxpool.Pool
	client *kgo.Client
	opts   RelayOptions

	closeClient sync.Once

	// mu guards the outcome of the last round and the start of a produce in progress.
	mu        sync.Mutex
	lastErr   error
	producing time.Time
}

// NewRelay returns a relay of the outbox in pool. It publishes through a Kafka client of its own,
// which every record reaches only once all in-sync replicas have it, without duplicates within
// a session of the producer, in the partition that its key's hash gives, as franz-go and Java's
// clients agree. It registers the gauge outbox.lag, the age in seconds of the oldest event not
// yet published, with the process's meter provider.
func NewRelay(pool *pgxpool.Pool, opts RelayOptions) (*Relay, error) {
	if opts.Source == "" || len(opts.Brokers) == 0 || opts.Every <= 0 {
		return nil, errors.New("outbox: a relay needs a source, brokers and a positive pause")
	}
	kafkaOpts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}
	if opts.TLS != nil {
		kafkaOpts = append(kafkaOpts, kgo.DialTLSConfig(opts.TLS))
	}
	client, err := kgo.NewClient(kafkaOpts...)
	if err != nil {
		return nil, fmt.Errorf("outbox: %w", err)
	}
	r := &Relay{pool: pool, client: client, opts: opts}
	_, err = otel.Meter("github.com/ancyloce/contelys-cloud-libs-service/outbox").Float64ObservableGauge(
		"outbox.lag",
		metric.WithUnit("s"),
		metric.WithDescription("The age of the oldest event in the outbox that the relay has not published."),
		metric.WithFloat64Callback(r.observeLag),
	)
	if err != nil {
		otel.Handle(err)
	}
	return r, nil
}

// observeLag observes the age of the oldest event in the outbox, or 0 when it is empty.
func (r *Relay) observeLag(ctx context.Context, observer metric.Float64Observer) error {
	var lag float64
	err := r.pool.QueryRow(ctx,
		"SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(occurred_at)), 0)::float8 FROM outbox").Scan(&lag)
	if err != nil {
		return fmt.Errorf("outbox: lag: %w", err)
	}
	observer.Observe(lag)
	return nil
}

// Run relays until ctx ends, and then closes the relay's Kafka client, so a relay runs once. Of a
// service's instances, only the one that holds the relay's advisory lock publishes, so the
// events about one thing leave in the order they were written. The others wait to take over.
func (r *Relay) Run(ctx context.Context) {
	// Closing the client is what makes a produce that Kafka leaves unanswered return.
	stopClosing := context.AfterFunc(ctx, r.close)
	defer func() {
		stopClosing()
		r.close()
	}()
	for ctx.Err() == nil {
		if err := r.lead(ctx); err != nil && ctx.Err() == nil {
			r.setLastError(err)
		}
		r.wait(ctx)
	}
}

func (r *Relay) close() {
	r.closeClient.Do(r.client.Close)
}

// Check reports whether the relay publishes, as a signal of readiness that does not take the
// instance out of routing: it fails when the last round failed, or when Kafka has left a round
// unacknowledged for longer than publishTimeout.
func (r *Relay) Check(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.producing.IsZero() && time.Since(r.producing) > publishTimeout {
		return fmt.Errorf("outbox: Kafka has not acknowledged a round for %s", time.Since(r.producing).Round(time.Second))
	}
	return r.lastErr
}

func (r *Relay) setLastError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastErr = err
}

// setProducing marks the start of a produce, or its end with the zero time.
func (r *Relay) setProducing(at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.producing = at
}

func (r *Relay) wait(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(r.opts.Every):
	}
}

// lead takes the relay's lock, if no other instance holds it, and publishes until ctx ends or
// a round fails.
func (r *Relay) lead(ctx context.Context) error {
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	defer conn.Release()
	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", relayLock).Scan(&locked); err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	if !locked {
		// Another instance leads. Following is healthy, whatever a round of this one did before.
		r.setLastError(nil)
		return nil
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", relayLock)
	}()
	for ctx.Err() == nil {
		published, err := r.publish(ctx, conn)
		r.setLastError(err)
		if err != nil {
			return err
		}
		if published < batchSize {
			r.wait(ctx)
		}
	}
	return nil
}

// publish sends the oldest events of the outbox to Kafka, then deletes them and records the
// positions they reached, in one transaction. A failure in between publishes them again later.
// It runs on conn, the session that holds the relay's lock, so that a relay whose session has
// ended fails its round instead of publishing beside the instance that took over.
func (r *Relay) publish(ctx context.Context, conn *pgxpool.Conn) (int, error) {
	rows, err := conn.Query(ctx,
		`SELECT id, topic, key, event_id::text, type, occurred_at, payload, traceparent, tracestate
		 FROM outbox ORDER BY id LIMIT $1`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("outbox: %w", err)
	}
	var ids []int64
	var records []*kgo.Record
	for rows.Next() {
		var event row
		if err := rows.Scan(&event.id, &event.topic, &event.key, &event.eventID, &event.eventType,
			&event.occurredAt, &event.payload, &event.traceparent, &event.tracestate); err != nil {
			rows.Close()
			return 0, fmt.Errorf("outbox: %w", err)
		}
		ids = append(ids, event.id)
		records = append(records, event.record(r.opts.Source))
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("outbox: %w", err)
	}
	if len(records) == 0 {
		return 0, nil
	}
	produce, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	r.setProducing(time.Now())
	results := r.client.ProduceSync(produce, records...)
	r.setProducing(time.Time{})
	if err := results.FirstErr(); err != nil {
		return 0, fmt.Errorf("outbox: publish: %w", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, "DELETE FROM outbox WHERE id = ANY($1)", ids); err != nil {
		return 0, fmt.Errorf("outbox: %w", err)
	}
	for _, result := range results {
		_, err := tx.Exec(ctx,
			`INSERT INTO outbox_positions (topic, partition, last_offset) VALUES ($1, $2, $3)
			 ON CONFLICT (topic, partition)
			 DO UPDATE SET last_offset = GREATEST(outbox_positions.last_offset, excluded.last_offset)`,
			result.Record.Topic, result.Record.Partition, result.Record.Offset)
		if err != nil {
			return 0, fmt.Errorf("outbox: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("outbox: %w", err)
	}
	return len(records), nil
}

// row is one event of the outbox, as the relay reads it.
type row struct {
	id                             int64
	topic, key, eventID, eventType string
	occurredAt                     time.Time
	payload                        []byte
	traceparent, tracestate        string
}

// record is the Kafka record of the event, published by source: its key, the Protobuf message
// alone as its value, and the CloudEvents headers of the Kafka binding's binary mode with the W3C
// trace context (ADR 0061).
func (e row) record(source string) *kgo.Record {
	headers := []kgo.RecordHeader{
		{Key: "ce_specversion", Value: []byte("1.0")},
		{Key: "ce_id", Value: []byte(e.eventID)},
		{Key: "ce_source", Value: []byte(source)},
		{Key: "ce_type", Value: []byte(e.eventType)},
		{Key: "ce_time", Value: []byte(e.occurredAt.UTC().Format(time.RFC3339Nano))},
		{Key: "content-type", Value: []byte("application/protobuf")},
	}
	if e.traceparent != "" {
		headers = append(headers, kgo.RecordHeader{Key: "traceparent", Value: []byte(e.traceparent)})
	}
	if e.tracestate != "" {
		headers = append(headers, kgo.RecordHeader{Key: "tracestate", Value: []byte(e.tracestate)})
	}
	return &kgo.Record{Topic: e.topic, Key: []byte(e.key), Value: e.payload, Headers: headers}
}
