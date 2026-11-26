// Package outbox commits a domain service's events in the same transaction as its business
// state, and its relay publishes them to Kafka at least once, with the CloudEvents headers of
// ADR 0061 (ADR 0029, ADR 0034).
package outbox

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/lock"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"
)

//go:embed migrations/*.sql
var migrations embed.FS

const (
	// versionTable is goose's table of the outbox's migrations, apart from the service's own, so
	// that the two numberings never meet.
	versionTable = "outbox_goose_version"
	// migrationLock is the advisory lock under which an instance migrates the outbox, so that
	// instances that start together migrate it one at a time. Its value spells "outbox" in ASCII.
	migrationLock int64 = 0x6f7574626f78
)

// Migrate creates or updates the outbox's tables in the service's database.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	files, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	store, err := database.NewStore(database.DialectPostgres, versionTable)
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(migrationLock))
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	provider, err := goose.NewProvider("", db, files, goose.WithStore(store), goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	return nil
}

// Event is one event to publish: the topic of its family, the identifier of the thing it is
// about, which becomes the record's key, and its Protobuf message.
type Event struct {
	Topic   string
	Key     string
	Message proto.Message
}

// Write adds events to the outbox in tx, the caller's transaction, so that they are published
// only if tx commits. Each event gets its stable ID when it is written, and the database's time of
// the transaction, and carries the trace of ctx. The relay publishes in the order of writing, so
// the events about one thing keep their order when the transactions that write them lock that
// thing, as a change of its row does.
func Write(ctx context.Context, tx pgx.Tx, events ...Event) error {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	for _, event := range events {
		if event.Topic == "" || event.Key == "" || event.Message == nil {
			return errors.New("outbox: an event needs a topic, a key and a message")
		}
		payload, err := proto.Marshal(event.Message)
		if err != nil {
			return fmt.Errorf("outbox: %w", err)
		}
		id, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("outbox: %w", err)
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO outbox (topic, key, event_id, type, payload, traceparent, tracestate)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			event.Topic, event.Key, id.String(), string(proto.MessageName(event.Message)),
			payload, carrier.Get("traceparent"), carrier.Get("tracestate"))
		if err != nil {
			return fmt.Errorf("outbox: %w", err)
		}
	}
	return nil
}

// Positions returns, for each partition of topic, the offset of the last record that the relay
// published there, so that a service can read its own topic from it at start (Identity ADR 0008,
// Organization Access ADR 0002). A partition that never received a record is absent.
func Positions(ctx context.Context, pool *pgxpool.Pool, topic string) (map[int32]int64, error) {
	rows, err := pool.Query(ctx, "SELECT partition, last_offset FROM outbox_positions WHERE topic = $1", topic)
	if err != nil {
		return nil, fmt.Errorf("outbox: %w", err)
	}
	positions := map[int32]int64{}
	for rows.Next() {
		var partition int32
		var offset int64
		if err := rows.Scan(&partition, &offset); err != nil {
			rows.Close()
			return nil, fmt.Errorf("outbox: %w", err)
		}
		positions[partition] = offset
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: %w", err)
	}
	return positions, nil
}
