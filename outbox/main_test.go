package outbox_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ancyloce/contelys-cloud/libs/service/outbox"
)

// postgresImage is the PostgreSQL that the tests run, pinned by its digest.
const postgresImage = "postgres:18-alpine@sha256:0115267ab5a509022df9319c77be365cef335adb71556c75f60f5360ee247e71"

// topic is the event family of the tests, Identity's, in three partitions.
const topic = "identity.principal"

// admin is the address of the container's own database, from which each test creates its own.
var admin string

func TestMain(m *testing.M) {
	ctx := context.Background()
	container, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase("outbox"), postgres.WithUsername("outbox"), postgres.WithPassword("outbox"),
		postgres.BasicWaitStrategies())
	if err != nil {
		fmt.Fprintln(os.Stderr, "start PostgreSQL:", err)
		os.Exit(1)
	}
	admin, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, "PostgreSQL's address:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = testcontainers.TerminateContainer(container)
	os.Exit(code)
}

var databases atomic.Int64

// newDatabase creates a database of the test's own, as a domain service's database, and applies
// the outbox's migrations to it.
func newDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := emptyDatabase(t)
	if err := outbox.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return pool
}

// emptyDatabase creates a database of the test's own, without the outbox's tables.
func emptyDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	adminPool, err := pgxpool.New(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	defer adminPool.Close()
	name := fmt.Sprintf("service_%d", databases.Add(1))
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(admin)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// kafka is an in-process Kafka cluster that holds the topic.
type kafka struct {
	cluster *kfake.Cluster
	addrs   []string
}

func newKafka(t *testing.T) *kafka {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	return &kafka{cluster: cluster, addrs: cluster.ListenAddrs()}
}

// createTopic creates a topic of one partition, as Event Center does for a new family.
func (k *kafka) createTopic(t *testing.T, name string) {
	t.Helper()
	request := kmsg.NewPtrCreateTopicsRequest()
	created := kmsg.NewCreateTopicsRequestTopic()
	created.Topic, created.NumPartitions, created.ReplicationFactor = name, 1, 1
	request.Topics = append(request.Topics, created)
	if _, err := request.RequestWith(context.Background(), k.client(t)); err != nil {
		t.Fatal(err)
	}
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

// consume reads the topic from its start until it has n records, for at most ten seconds.
func (k *kafka) consume(t *testing.T, n int) []*kgo.Record {
	t.Helper()
	consumer := k.client(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var records []*kgo.Record
	for len(records) < n {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("consumed %d records, want %d", len(records), n)
		}
		records = append(records, fetches.Records()...)
	}
	return records
}

// consumeAll reads the topic from its start until no record has come for settle, and returns
// every record, duplicates included.
func (k *kafka) consumeAll(t *testing.T, settle time.Duration) []*kgo.Record {
	t.Helper()
	consumer := k.client(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	var records []*kgo.Record
	for {
		ctx, cancel := context.WithTimeout(context.Background(), settle)
		fetches := consumer.PollFetches(ctx)
		cancel()
		got := fetches.Records()
		if len(got) == 0 {
			return records
		}
		records = append(records, got...)
	}
}

// startRelay runs a relay of the service source until the test ends.
func startRelay(t *testing.T, pool *pgxpool.Pool, k *kafka, source string) *outbox.Relay {
	t.Helper()
	relay, _ := runRelay(t, pool, k.addrs, source)
	return relay
}

// newRelay returns a relay of the service source that publishes to brokers.
func newRelay(t *testing.T, pool *pgxpool.Pool, brokers []string, source string) *outbox.Relay {
	t.Helper()
	relay, err := outbox.NewRelay(pool, outbox.RelayOptions{Source: source, Brokers: brokers, Every: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	return relay
}

// runRelay runs a relay that publishes to brokers until stop is called or the test ends.
func runRelay(t *testing.T, pool *pgxpool.Pool, brokers []string, source string) (relay *outbox.Relay, stop func()) {
	t.Helper()
	relay = newRelay(t, pool, brokers, source)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		relay.Run(ctx)
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)
	return relay, stop
}
