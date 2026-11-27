package inbox_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ancyloce/contelys-cloud-libs-service/inbox"
)

func TestASubscriptionThatKeepsItsPositionStartsFromTheDatabaseNotFromKafka(t *testing.T) {
	k := newKafka(t, 1)
	opts := k.options()
	opts.KeepPosition = true
	consume := func(pool *pgxpool.Pool) (stop func(), got *handled) {
		got = &handled{}
		stop = run(t, pool, opts, func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
			got.add(e)
			return record(e), nil
		})
		return stop, got
	}
	first, second, third := newEvent(invitation, "first"), newEvent(invitation, "second"), newEvent(invitation, "third")
	restored, fresh := newDatabase(t), newDatabase(t)

	// The database that a restore later brings back records the first two events.
	k.publish(t, first.record(), second.record())
	stop, _ := consume(restored)
	eventually(t, 10*time.Second, func() bool { return len(effects(t, restored)) == 2 }, "the first two effects never committed")
	stop()

	// A database without a position reads the topic from its start, although Kafka holds the
	// group's offset past both events. It consumes the third event too.
	stop, got := consume(fresh)
	k.publish(t, third.record())
	eventually(t, 10*time.Second, func() bool { return len(effects(t, fresh)) == 3 }, "a database without a position did not read from the start")
	stop()
	if ids := got.ids(); !slices.Equal(ids, []string{first.id, second.id, third.id}) {
		t.Errorf("handed over without a position = %v, want every event", ids)
	}

	// The restored database starts after its own position, behind the group's offset in Kafka.
	_, got = consume(restored)
	eventually(t, 10*time.Second, func() bool { return len(effects(t, restored)) == 3 }, "the restored database did not read from its position")
	if ids := got.ids(); !slices.Equal(ids, []string{third.id}) {
		t.Errorf("handed over to the restored database = %v, want the event after its position", ids)
	}
}
