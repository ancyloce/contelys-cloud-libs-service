package inbox_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/ancyloce/contelys-cloud-libs-service/inbox"
)

func TestTheInboxForgetsAnEventIdentifierOnlyOnceItsRetentionHasPassed(t *testing.T) {
	if inbox.Retention != 8*24*time.Hour {
		t.Errorf("Retention = %s, want the 8 days of ADR 0029", inbox.Retention)
	}
	pool := newDatabase(t)
	k := newKafka(t, 1)
	ctx := context.Background()
	issued, second, third := newEvent(invitation, "issued"), newEvent(invitation, "second"), newEvent(invitation, "third")
	var got handled
	run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		got.add(e)
		return record(e), nil
	})
	k.publish(t, issued.record())
	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 1 }, "the effect never committed")

	// The periodic removal leaves an identifier younger than the retention.
	if err := inbox.Purge(ctx, pool, time.Now().Add(-inbox.Retention)); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	k.publish(t, issued.record(), second.record())
	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 2 }, "the next effect never committed")
	if ids := got.ids(); !slices.Equal(ids, []string{issued.id, second.id}) {
		t.Fatalf("handed over = %v, want the repeat recognized while its identifier is kept", ids)
	}

	// Once the retention has passed, the identifier is removed and the event is new again.
	if err := inbox.Purge(ctx, pool, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	k.publish(t, issued.record(), third.record())
	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 4 }, "the effects after the removal never committed")
	if ids := got.ids(); !slices.Equal(ids, []string{issued.id, second.id, issued.id, third.id}) {
		t.Errorf("handed over = %v, want the event handed over again once its identifier was removed", ids)
	}
}
