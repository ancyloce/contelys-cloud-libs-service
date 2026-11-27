// Package inbox consumes an event family at least once and commits the effect of each event
// together with its inbox marker in the subscriber's own database, so that a repeated or
// replayed event changes nothing (ADR 0029, ADR 0034). It reads the CloudEvents headers that the
// outbox's relay writes (ADR 0061).
package inbox

import (
	"context"
	"crypto/tls"
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/migration"
)

//go:embed migrations/*.sql
var migrations embed.FS

const (
	// versionTable is goose's table of the inbox's migrations, apart from the service's own, so
	// that the two numberings never meet.
	versionTable = "inbox_goose_version"
	// migrationLock is the advisory lock under which an instance migrates the inbox, so that
	// instances that start together migrate it one at a time. Its value spells "inbox" in ASCII.
	migrationLock int64 = 0x696e626f78
	// batchSize is the most records that one poll hands over.
	batchSize = 100
	// instrumentation names the inbox's tracer and meter.
	instrumentation = "github.com/ancyloce/contelys-cloud-libs-service/inbox"
)

const (
	// Retention is how long the inbox keeps an event identifier: 8 days, longer than a replay of
	// a topic with the default retention of 7 days (ADR 0029).
	Retention = 8 * 24 * time.Hour
	// purgeEvery is how often a running consumer removes the identifiers older than Retention.
	purgeEvery = time.Hour
)

// Purge removes from the inbox every event identifier received before before, of every
// subscription. A running consumer calls it every hour with the time Retention ago, and a
// removal that runs on several instances at once removes each identifier once.
func Purge(ctx context.Context, pool *pgxpool.Pool, before time.Time) error {
	if _, err := pool.Exec(ctx, "DELETE FROM inbox WHERE received_at < $1", before); err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	return nil
}

// Migrate creates or updates the inbox's tables in the service's database.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if err := migration.Up(ctx, pool, migrations, versionTable, migrationLock); err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	return nil
}

// Event is one consumed record: its producer, event identifier, type and time from its
// CloudEvents headers, and its key and value as Kafka holds them.
type Event struct {
	// Source is the slug of the producing deployable, from ce_source.
	Source string
	// ID is the event's stable identifier, from ce_id, which a replay keeps.
	ID string
	// Type is the full name of the event's Protobuf message, from ce_type.
	Type string
	// Time is when the producer committed the event, from ce_time.
	Time time.Time
	// Key is the record's key, the identifier of the thing the event is about.
	Key []byte
	// Value is the record's value, the Protobuf message alone.
	Value []byte
}

// Effect is what an event changes in the subscriber's database. It runs in tx, the transaction
// that also commits the event's inbox marker, so that both commit or neither does.
type Effect func(ctx context.Context, tx pgx.Tx) error

// Handler decides what an event changes. It is called outside any transaction, so it may ask a
// neighbour first, and its context carries the producer's trace. It returns the event's effect,
// or nil to skip the event, which commits its inbox marker alone. An error that wraps
// ErrUndecodable sends the record to the dead letters. Any other error leaves the record
// uncommitted, and the same record is handed over again after the consumer's backoff.
type Handler func(ctx context.Context, e Event) (Effect, error)

// ErrUndecodable is what a handler wraps when it cannot decode a record's value as the message
// that its ce_type names. The record then goes to the subscription's dead letters, with the
// error's text as the reason, and the consumer moves on.
var ErrUndecodable = errors.New("inbox: the record cannot be decoded")

// DeadLetterTopic is the topic to which a subscription's consumer moves the records it cannot
// read, with their headers and a header dead-letter-reason: "<subscriber>.<subscription>.dead-letter".
// Event Center creates and governs it (issue 53 of the phase 1 platform).
func DeadLetterTopic(subscriber, subscription string) string {
	return subscriber + "." + subscription + ".dead-letter"
}

// Options configure a consumer of one subscription.
type Options struct {
	// Subscriber is the slug of the subscribing deployable.
	Subscriber string
	// Subscription names the subscription within the subscriber, such as "invitations". The
	// consumer group is "<subscriber>.<subscription>".
	Subscription string
	// Topic is the topic of the event family.
	Topic string
	// Brokers are the addresses of Kafka's brokers.
	Brokers []string
	// TLS, when set, is the configuration of the connections to Kafka.
	TLS *tls.Config
	// Backoff is how long the consumer waits before it hands over again a record whose handler
	// or effect failed.
	Backoff time.Duration
	// KeepPosition makes the consumer write the offset of each record in the transaction of its
	// inbox marker, and start each partition after the offset that the database records, or from
	// the topic's start when it records none, never from the offset committed to Kafka. A
	// restored database then reads again every event after the point it was restored to.
	KeepPosition bool
	// Logger, when set, receives the consumer's failures and dead letters. No line carries a
	// record's key or value; a handler's error is logged as it is.
	Logger *slog.Logger
}

// Consumer consumes one subscription into the inbox of a service's database.
type Consumer struct {
	pool   *pgxpool.Pool
	opts   Options
	handle Handler
	client *kgo.Client
	tracer trace.Tracer
	logger *slog.Logger

	// mu guards the partitions assigned to this consumer, and the high watermark and the next
	// offset to consume of each, from which the lag is observed.
	mu       sync.Mutex
	assigned map[int32]bool
	high     map[int32]int64
	next     map[int32]int64
}

// New returns a consumer of the subscription that opts names, which hands each new event of the
// topic to handle and commits its effect in pool.
func New(pool *pgxpool.Pool, opts Options, handle Handler) (*Consumer, error) {
	if opts.Subscriber == "" || opts.Subscription == "" || opts.Topic == "" || len(opts.Brokers) == 0 ||
		opts.Backoff <= 0 || handle == nil {
		return nil, errors.New("inbox: a consumer needs a subscriber, a subscription, a topic, brokers, a positive backoff and a handler")
	}
	kafkaOpts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ConsumerGroup(opts.Subscriber + "." + opts.Subscription),
		kgo.ConsumeTopics(opts.Topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	}
	if opts.TLS != nil {
		kafkaOpts = append(kafkaOpts, kgo.DialTLSConfig(opts.TLS))
	}
	c := &Consumer{
		pool: pool, opts: opts, handle: handle, tracer: otel.Tracer(instrumentation), logger: opts.Logger,
		assigned: map[int32]bool{}, high: map[int32]int64{}, next: map[int32]int64{},
	}
	if c.logger == nil {
		c.logger = slog.New(slog.DiscardHandler)
	}
	c.logger = c.logger.With("subscription", opts.Subscription, "topic", opts.Topic)
	kafkaOpts = append(kafkaOpts, kgo.OnPartitionsRevoked(c.forget), kgo.OnPartitionsLost(c.forget),
		kgo.AdjustFetchOffsetsFn(c.assign))
	client, err := kgo.NewClient(kafkaOpts...)
	if err != nil {
		return nil, fmt.Errorf("inbox: %w", err)
	}
	c.client = client
	return c, nil
}

// assign runs when the group assigns partitions to this consumer, once their offsets are known.
// A subscription that keeps its position starts from the database's positions instead. It then
// reads where each partition ends, so that the lag of a partition is known before its first fetch,
// also when the partition holds nothing to fetch.
func (c *Consumer) assign(ctx context.Context, assigned map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	if c.opts.KeepPosition {
		var err error
		if assigned, err = c.recordedPositions(ctx, assigned); err != nil {
			return nil, err
		}
	}
	starts := assigned[c.opts.Topic]
	c.mu.Lock()
	for partition := range starts {
		c.assigned[partition] = true
	}
	c.mu.Unlock()
	if err := c.positionAssigned(ctx, starts); err != nil {
		// The lag of these partitions stays unknown, and is not reported, until their first fetch.
		c.logger.Warn("the end of the assigned partitions could not be read", "error", err)
	}
	return assigned, nil
}

// positionAssigned records where each assigned partition ends and where its consumption starts.
func (c *Consumer) positionAssigned(ctx context.Context, starts map[int32]kgo.Offset) error {
	var partitions, fromStart []int32
	for partition, start := range starts {
		partitions = append(partitions, partition)
		if start.EpochOffset().Offset < 0 {
			fromStart = append(fromStart, partition)
		}
	}
	if len(partitions) == 0 {
		return nil
	}
	ends, err := c.listOffsets(ctx, partitions, -1)
	if err != nil {
		return err
	}
	firsts, err := c.listOffsets(ctx, fromStart, -2)
	if err != nil {
		return err
	}
	for partition, start := range starts {
		next := start.EpochOffset().Offset
		if next < 0 {
			// No committed offset: the consumer starts from the partition's first record.
			next = firsts[partition]
		}
		c.setPosition(partition, ends[partition], next)
	}
	return nil
}

// listOffsets asks Kafka for the latest offset (timestamp -1) or the earliest (-2) of partitions of
// the topic.
func (c *Consumer) listOffsets(ctx context.Context, partitions []int32, timestamp int64) (map[int32]int64, error) {
	offsets := map[int32]int64{}
	if len(partitions) == 0 {
		return offsets, nil
	}
	request := kmsg.NewPtrListOffsetsRequest()
	request.ReplicaID = -1
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = c.opts.Topic
	for _, partition := range partitions {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition, p.Timestamp, p.CurrentLeaderEpoch = partition, timestamp, -1
		topic.Partitions = append(topic.Partitions, p)
	}
	request.Topics = append(request.Topics, topic)
	response, err := request.RequestWith(ctx, c.client)
	if err != nil {
		return nil, fmt.Errorf("inbox: %w", err)
	}
	for _, t := range response.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return nil, fmt.Errorf("inbox: partition %d: %w", p.Partition, err)
			}
			offsets[p.Partition] = p.Offset
		}
	}
	return offsets, nil
}

// recordedPositions replaces the offsets that Kafka holds for the partitions just assigned with
// the positions that the database records.
func (c *Consumer) recordedPositions(ctx context.Context, assigned map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	rows, err := c.pool.Query(ctx,
		"SELECT partition, last_offset FROM inbox_positions WHERE subscription = $1 AND topic = $2",
		c.opts.Subscription, c.opts.Topic)
	if err != nil {
		return nil, fmt.Errorf("inbox: %w", err)
	}
	recorded := map[int32]int64{}
	for rows.Next() {
		var partition int32
		var offset int64
		if err := rows.Scan(&partition, &offset); err != nil {
			rows.Close()
			return nil, fmt.Errorf("inbox: %w", err)
		}
		recorded[partition] = offset
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("inbox: %w", err)
	}
	for partition := range assigned[c.opts.Topic] {
		start := kgo.NewOffset().AtStart()
		if offset, ok := recorded[partition]; ok {
			start = kgo.NewOffset().At(offset + 1)
		}
		assigned[c.opts.Topic][partition] = start.WithEpoch(-1)
	}
	return assigned, nil
}

// forget drops the partitions that the group took from this consumer from its lag.
func (c *Consumer) forget(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, partition := range partitions[c.opts.Topic] {
		delete(c.assigned, partition)
		delete(c.high, partition)
		delete(c.next, partition)
	}
}

// observeLag observes how many records of its assigned partitions this consumer has not yet
// consumed. It observes nothing while it holds no partition, or while the lag of one of them is
// unknown, because a lag of zero would tell an operator that nothing waits.
func (c *Consumer) observeLag(observe func(int64)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.assigned) == 0 {
		return
	}
	var lag int64
	for partition := range c.assigned {
		high, known := c.high[partition]
		next, positioned := c.next[partition]
		if !known || !positioned {
			return
		}
		if high > next {
			lag += high - next
		}
	}
	observe(lag)
}

// Run consumes until ctx ends, and then leaves the consumer group, so a consumer runs once. It
// also removes the inbox's identifiers older than Retention, at its start and every hour. While it
// runs, the gauge inbox.lag of the process's meter provider reports, under the attribute
// subscription, how many records of its partitions it has not yet consumed.
func (c *Consumer) Run(ctx context.Context) {
	defer c.client.Close()
	meter := otel.Meter(instrumentation)
	gauge, err := meter.Int64ObservableGauge("inbox.lag", metric.WithUnit("{record}"),
		metric.WithDescription("The records of a subscription's partitions that its consumer has not yet consumed."))
	if err == nil {
		subscription := metric.WithAttributes(attribute.String("subscription", c.opts.Subscription))
		registration, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			c.observeLag(func(lag int64) { o.ObserveInt64(gauge, lag, subscription) })
			return nil
		}, gauge)
		if err == nil {
			defer func() { _ = registration.Unregister() }()
		}
	}
	if err != nil {
		otel.Handle(err)
	}
	purged := make(chan struct{})
	defer func() { <-purged }()
	go func() {
		defer close(purged)
		for {
			if err := Purge(ctx, c.pool, time.Now().Add(-Retention)); err != nil && ctx.Err() == nil {
				c.logger.Error("the inbox's old event identifiers were not removed", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(purgeEvery):
			}
		}
	}()
	for ctx.Err() == nil {
		fetches := c.client.PollRecords(ctx, batchSize)
		if ctx.Err() != nil {
			c.client.AllowRebalance()
			return
		}
		failed := c.consumeFetches(ctx, fetches)
		c.client.AllowRebalance()
		if failed {
			select {
			case <-ctx.Done():
			case <-time.After(c.opts.Backoff):
			}
		}
	}
}

// consumeFetches consumes the records of each partition in order, and commits the offsets of
// those that are done. A partition stops at its first record that failed, which is fetched
// again, so no offset moves past it. It reports whether a record failed.
func (c *Consumer) consumeFetches(ctx context.Context, fetches kgo.Fetches) (failed bool) {
	fetches.EachError(func(topic string, partition int32, err error) {
		c.logger.Warn("a fetch from Kafka failed", "partition", partition, "error", err)
	})
	var done []*kgo.Record
	rewind := map[int32]kgo.EpochOffset{}
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if p.Topic != c.opts.Topic || len(p.Records) == 0 {
			return
		}
		if _, stopped := rewind[p.Partition]; stopped {
			// One partition may come in several entries of a poll. After a failure in an earlier
			// one, none of its later records is consumed, so no offset passes the failure.
			return
		}
		c.setPosition(p.Partition, p.HighWatermark, p.Records[0].Offset)
		for _, record := range p.Records {
			if err := c.consume(ctx, record); err != nil {
				failed = true
				rewind[p.Partition] = kgo.EpochOffset{Epoch: record.LeaderEpoch, Offset: record.Offset}
				if ctx.Err() == nil {
					c.logger.Warn("a record waits to be handed over again", "partition", record.Partition,
						"offset", record.Offset, "error", err)
				}
				return
			}
			done = append(done, record)
			c.setPosition(p.Partition, p.HighWatermark, record.Offset+1)
		}
	})
	if len(rewind) > 0 {
		c.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{c.opts.Topic: rewind})
	}
	if len(done) > 0 {
		if err := c.client.CommitRecords(ctx, done...); err != nil && ctx.Err() == nil {
			failed = true
			c.logger.Warn("the offsets were not committed to Kafka", "error", err)
		}
	}
	return failed
}

// setPosition records the high watermark of partition and the next offset to consume there.
func (c *Consumer) setPosition(partition int32, high, next int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.high[partition], c.next[partition] = high, next
}

// consume hands one record over and commits its effect with its inbox marker, unless the inbox
// already holds it. A nil error means the record is done.
func (c *Consumer) consume(ctx context.Context, record *kgo.Record) error {
	e, err := parse(record)
	if err != nil {
		return c.deadLetter(ctx, record, err.Error())
	}
	carrier := propagation.MapCarrier{}
	for _, h := range record.Headers {
		if h.Key == "traceparent" || h.Key == "tracestate" {
			carrier[h.Key] = string(h.Value)
		}
	}
	ctx = propagation.TraceContext{}.Extract(ctx, carrier)
	ctx, span := c.tracer.Start(ctx, record.Topic+" process", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", record.Topic),
			attribute.String("messaging.consumer.group.name", c.opts.Subscriber+"."+c.opts.Subscription),
			attribute.String("messaging.message.id", e.ID),
		))
	defer span.End()

	var repeat bool
	err = c.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM inbox WHERE subscription = $1 AND source = $2 AND event_id = $3)",
		c.opts.Subscription, e.Source, e.ID).Scan(&repeat)
	if err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	if repeat {
		return nil
	}
	effect, err := c.handle(ctx, e)
	if errors.Is(err, ErrUndecodable) {
		return c.deadLetter(ctx, record, err.Error())
	}
	if err != nil {
		return err
	}
	return c.commit(ctx, record, e, effect)
}

// deadLetter moves record to the subscription's dead letters with its key, value and headers and
// the reason. The record is done once Kafka has the dead letter; until then it is fetched again.
func (c *Consumer) deadLetter(ctx context.Context, record *kgo.Record, reason string) error {
	headers := append(slices.Clone(record.Headers), kgo.RecordHeader{Key: "dead-letter-reason", Value: []byte(reason)})
	letter := &kgo.Record{Topic: DeadLetterTopic(c.opts.Subscriber, c.opts.Subscription), Key: record.Key, Value: record.Value, Headers: headers}
	if err := c.client.ProduceSync(ctx, letter).FirstErr(); err != nil {
		return fmt.Errorf("inbox: dead letter: %w", err)
	}
	// A subscription that keeps its position starts from it, not from Kafka's offset, so the
	// position moves past the dead letter too, also when the consumer is stopping. Should that
	// fail, the record is fetched again and sent to the dead letters a second time, which loses
	// nothing.
	if c.opts.KeepPosition {
		if err := c.writePosition(context.WithoutCancel(ctx), c.pool, record); err != nil {
			return err
		}
	}
	c.logger.Warn("a record went to the dead letters", "partition", record.Partition, "offset", record.Offset,
		"dead_letter_topic", letter.Topic, "reason", reason)
	return nil
}

// commit commits the event's inbox marker and its effect in one transaction, with the record's
// position when the subscription keeps it. A marker that another consumer committed first makes
// the event a repeat, and nothing of this one commits.
func (c *Consumer) commit(ctx context.Context, record *kgo.Record, e Event, effect Effect) error {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	marked, err := tx.Exec(ctx,
		"INSERT INTO inbox (subscription, source, event_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
		c.opts.Subscription, e.Source, e.ID)
	if err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	if marked.RowsAffected() == 0 {
		return nil
	}
	if c.opts.KeepPosition {
		if err := c.writePosition(ctx, tx, record); err != nil {
			return err
		}
	}
	if effect != nil {
		if err := effect(ctx, tx); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	return nil
}

// executor is a pool or a transaction.
type executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// writePosition records the record's offset as its partition's position, unless the position is
// already past it.
func (c *Consumer) writePosition(ctx context.Context, db executor, record *kgo.Record) error {
	_, err := db.Exec(ctx,
		`INSERT INTO inbox_positions (subscription, topic, partition, last_offset) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (subscription, topic, partition)
		 DO UPDATE SET last_offset = GREATEST(inbox_positions.last_offset, excluded.last_offset)`,
		c.opts.Subscription, record.Topic, record.Partition, record.Offset)
	if err != nil {
		return fmt.Errorf("inbox: %w", err)
	}
	return nil
}

// parse reads the event's identity from the record's CloudEvents headers, as the Kafka protocol
// binding's binary mode carries them (ADR 0061), and refuses a record whose headers are missing
// or invalid.
func parse(record *kgo.Record) (Event, error) {
	headers := map[string]string{}
	for _, h := range record.Headers {
		if _, seen := headers[h.Key]; !seen {
			headers[h.Key] = string(h.Value)
		}
	}
	for _, name := range []string{"ce_id", "ce_source", "ce_type"} {
		if headers[name] == "" {
			return Event{}, fmt.Errorf("inbox: the header %s is missing", name)
		}
	}
	if headers["ce_specversion"] != "1.0" {
		return Event{}, errors.New("inbox: the header ce_specversion is not 1.0")
	}
	if headers["content-type"] != "application/protobuf" {
		return Event{}, errors.New("inbox: the header content-type is not application/protobuf")
	}
	at, err := time.Parse(time.RFC3339Nano, headers["ce_time"])
	if err != nil {
		return Event{}, errors.New("inbox: the header ce_time is not a time in RFC 3339")
	}
	return Event{
		Source: headers["ce_source"], ID: headers["ce_id"], Type: headers["ce_type"], Time: at,
		Key: record.Key, Value: record.Value,
	}, nil
}
