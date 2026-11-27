package inbox_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ancyloce/contelys-cloud-libs-service/inbox"
)

const invitation = "01JDZ8Q3V7XK2M4N6P8R0T2W4Y"

func TestARecordIsHandedOverWithItsCloudEventsIdentityAndItsEffectCommits(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t, 3)
	issued := newEvent(invitation, "issued")
	var got handled
	run(t, pool, k.options(), func(ctx context.Context, e inbox.Event) (inbox.Effect, error) {
		got.add(e)
		return record(e), nil
	})

	k.publish(t, issued.record())

	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 1 }, "the effect never committed")
	e := got.all()[0]
	if e.Source != "organization-access" || e.ID != issued.id || e.Type != issued.eventType || !e.Time.Equal(issued.time) {
		t.Errorf("event = %s %s %s %s, want the record's ce_source, ce_id, ce_type and ce_time", e.Source, e.ID, e.Type, e.Time)
	}
	if string(e.Key) != invitation || string(e.Value) != "issued" {
		t.Errorf("key and value = %q %q, want the record's", e.Key, e.Value)
	}
	if ids := effects(t, pool); !slices.Equal(ids, []string{issued.id}) {
		t.Errorf("effects = %v, want the one event's", ids)
	}
}

func TestAnEffectThatRollsBackLeavesNoMarkerAndItsRecordIsHandedOverAgainBeforeTheNext(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t, 3)
	issued, later := newEvent(invitation, "issued"), newEvent(invitation, "later")
	var got handled
	var failures atomic.Int32
	run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		got.add(e)
		return func(ctx context.Context, tx pgx.Tx) error {
			if err := record(e)(ctx, tx); err != nil {
				return err
			}
			if e.ID == issued.id && failures.Add(1) == 1 {
				return errors.New("the effect failed after writing")
			}
			return nil
		}, nil
	})

	k.publish(t, issued.record(), later.record())

	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 2 }, "the effects never committed")
	if ids := got.ids(); !slices.Equal(ids, []string{issued.id, issued.id, later.id}) {
		t.Errorf("handed over = %v, want the failed event again before the next", ids)
	}
	if ids := effects(t, pool); !slices.Equal(ids, []string{issued.id, later.id}) {
		t.Errorf("effects = %v, want each event's once: the failed effect rolled back", ids)
	}
}

func TestAHandlerThatFailsGetsTheSameRecordAgainAndNothingAfterIt(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t, 3)
	issued, later := newEvent(invitation, "issued"), newEvent(invitation, "later")
	var got handled
	var failures atomic.Int32
	run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		got.add(e)
		// A neighbour cannot answer the lookup the first three times.
		if e.ID == issued.id && failures.Add(1) <= 3 {
			return nil, errors.New("the neighbour cannot be reached")
		}
		return record(e), nil
	})

	k.publish(t, issued.record(), later.record())

	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 2 }, "the effects never committed")
	if ids := got.ids(); !slices.Equal(ids, []string{issued.id, issued.id, issued.id, issued.id, later.id}) {
		t.Errorf("handed over = %v, want the failing event until it succeeded, then the next", ids)
	}
}

func TestASkippedEventCommitsItsMarkerAloneAndIsNotHandedOverAgain(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t, 3)
	joined := newEvent(invitation, "joined")
	joined.eventType = "contelys.organization_access.v1.MemberJoined"
	issued := newEvent(invitation, "issued")
	var got handled
	run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		got.add(e)
		if e.Type == joined.eventType {
			return nil, nil
		}
		return record(e), nil
	})

	k.publish(t, joined.record(), joined.record(), issued.record())

	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 1 }, "the effect never committed")
	if ids := got.ids(); !slices.Equal(ids, []string{joined.id, issued.id}) {
		t.Errorf("handed over = %v, want the skipped event once, then the next", ids)
	}
	if ids := effects(t, pool); !slices.Equal(ids, []string{issued.id}) {
		t.Errorf("effects = %v, want none for the skipped event", ids)
	}
}

func TestAnEventDeliveredAgainIsNotHandedOverAgain(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t, 3)
	issued, later := newEvent(invitation, "issued"), newEvent(invitation, "later")
	var got handled
	run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		got.add(e)
		return record(e), nil
	})

	// The same event twice, as at least once delivers it, then once more after another event, as
	// a replay through Event Center delivers it with the same ce_id.
	k.publish(t, issued.record(), issued.record(), later.record(), issued.record())

	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 2 }, "the effects never committed")
	time.Sleep(200 * time.Millisecond)
	if ids := got.ids(); !slices.Equal(ids, []string{issued.id, later.id}) {
		t.Errorf("handed over = %v, want each event once", ids)
	}
	if ids := effects(t, pool); !slices.Equal(ids, []string{issued.id, later.id}) {
		t.Errorf("effects = %v, want each event's once", ids)
	}
}
