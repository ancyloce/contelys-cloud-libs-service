package inbox_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ancyloce/contelys-cloud-libs-service/inbox"
)

// refuseOffsetCommits makes Kafka refuse every offset commit while refusing holds, and counts
// the refusals.
func (k *kafka) refuseOffsetCommits(refusing *atomic.Bool, refused *atomic.Int32) {
	k.cluster.ControlKey(int16(kmsg.OffsetCommit), func(request kmsg.Request) (kmsg.Response, error, bool) {
		if !refusing.Load() {
			return nil, nil, false
		}
		k.cluster.KeepControl()
		refused.Add(1)
		commit := request.(*kmsg.OffsetCommitRequest)
		response := commit.ResponseKind().(*kmsg.OffsetCommitResponse)
		for _, topic := range commit.Topics {
			answered := kmsg.NewOffsetCommitResponseTopic()
			answered.Topic = topic.Topic
			for _, partition := range topic.Partitions {
				p := kmsg.NewOffsetCommitResponseTopicPartition()
				p.Partition, p.ErrorCode = partition.Partition, kerr.GroupAuthorizationFailed.Code
				answered.Partitions = append(answered.Partitions, p)
			}
			response.Topics = append(response.Topics, answered)
		}
		return response, nil, true
	})
}

func TestAStopBetweenTheEffectsCommitAndTheOffsetCommitDeliversTheRecordAgainAsARepeat(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t, 1)
	var refusing atomic.Bool
	var refused atomic.Int32
	refusing.Store(true)
	k.refuseOffsetCommits(&refusing, &refused)
	issued, later := newEvent(invitation, "issued"), newEvent(invitation, "later")
	stop := run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		return record(e), nil
	})

	k.publish(t, issued.record())
	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 1 && refused.Load() > 0 },
		"the effect never committed, or its offset commit was never refused")
	stop()
	refusing.Store(false)

	// Another instance of the subscriber takes the partition over from the last committed offset.
	var got handled
	run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		got.add(e)
		return record(e), nil
	})
	k.publish(t, later.record())

	eventually(t, 15*time.Second, func() bool { return len(effects(t, pool)) == 2 }, "the next record was never handed over")
	if ids := got.ids(); !slices.Equal(ids, []string{later.id}) {
		t.Errorf("handed over after the stop = %v, want only the next record: the first is a repeat", ids)
	}
	if ids := effects(t, pool); !slices.Equal(ids, []string{issued.id, later.id}) {
		t.Errorf("effects = %v, want each event's once", ids)
	}
}

func TestNoOffsetIsCommittedPastARecordWhoseTransactionHasNotCommitted(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t, 1)
	issued, later := newEvent(invitation, "issued"), newEvent(invitation, "later")
	var attempts atomic.Int32
	stop := run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		if e.ID == issued.id {
			attempts.Add(1)
			return nil, errors.New("the neighbour cannot be reached")
		}
		return record(e), nil
	})

	k.publish(t, issued.record(), later.record())
	eventually(t, 10*time.Second, func() bool { return attempts.Load() >= 3 }, "the failing record was not handed over again")
	stop()

	var got handled
	run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		got.add(e)
		return record(e), nil
	})
	eventually(t, 15*time.Second, func() bool { return len(effects(t, pool)) == 2 }, "the records were not handed over again")
	if ids := got.ids(); !slices.Equal(ids, []string{issued.id, later.id}) {
		t.Errorf("handed over after the stop = %v, want both records from the one that never committed", ids)
	}
}
