package inbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ancyloce/contelys-cloud-libs-service/inbox"
)

// A record that a subscription keeping its position sent to the dead letters moves that position,
// so that the subscription, which starts from its position and not from Kafka's offset, does not
// send it there again.
func TestADeadLetterMovesThePositionOfASubscriptionThatKeepsIt(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t, 1)
	opts := k.options()
	opts.KeepPosition = true
	handle := func(_ context.Context, e inbox.Event) (inbox.Effect, error) { return record(e), nil }
	k.publish(t, newEvent(invitation, "valid").record(), without(newEvent(invitation, "headless"), "ce_id"))
	stop := run(t, pool, opts, handle)
	k.readDeadLetters(t, 1)
	stop()

	run(t, pool, opts, handle)
	k.publish(t, newEvent(invitation, "after").record())
	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 2 }, "the event after the dead letter never committed")

	if letters := k.allDeadLetters(t); len(letters) != 1 {
		t.Errorf("dead letters = %d, want the one record once", len(letters))
	}
}

// allDeadLetters reads the dead-letter topic from its start until no record has come for half a
// second.
func (k *kafka) allDeadLetters(t *testing.T) []*kgo.Record {
	t.Helper()
	consumer := k.client(t, kgo.ConsumeTopics(deadLetters), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	var records []*kgo.Record
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		fetches := consumer.PollFetches(ctx)
		cancel()
		got := fetches.Records()
		if len(got) == 0 {
			return records
		}
		records = append(records, got...)
	}
}
