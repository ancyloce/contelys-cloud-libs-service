package inbox_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ancyloce/contelys-cloud-libs-service/inbox"
)

// deadLetters is the dead-letter topic of the test's subscription.
const deadLetters = "notifications.invitations.dead-letter"

// readDeadLetters reads the dead-letter topic from its start until it has n records, for at most
// ten seconds.
func (k *kafka) readDeadLetters(t *testing.T, n int) []*kgo.Record {
	t.Helper()
	consumer := k.client(t, kgo.ConsumeTopics(deadLetters), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var records []*kgo.Record
	for len(records) < n {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("dead letters = %d, want %d", len(records), n)
		}
		records = append(records, fetches.Records()...)
	}
	return records
}

func header(record *kgo.Record, key string) string {
	for _, h := range record.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// without returns the record of e with the header key removed, or set to value when one is given.
func without(e event, key string, value ...string) *kgo.Record {
	r := e.record()
	var headers []kgo.RecordHeader
	for _, h := range r.Headers {
		if h.Key != key {
			headers = append(headers, h)
		} else if len(value) > 0 {
			headers = append(headers, kgo.RecordHeader{Key: key, Value: []byte(value[0])})
		}
	}
	r.Headers = headers
	return r
}

func TestARecordWithMissingOrInvalidCloudEventsHeadersGoesToTheDeadLettersAndTheNextIsHandedOver(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t, 1)
	var got handled
	run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		got.add(e)
		return record(e), nil
	})
	bad := map[string]*kgo.Record{}
	for name, r := range map[string]func(event) *kgo.Record{
		"no ce_id":            func(e event) *kgo.Record { return without(e, "ce_id") },
		"no ce_source":        func(e event) *kgo.Record { return without(e, "ce_source") },
		"no ce_type":          func(e event) *kgo.Record { return without(e, "ce_type") },
		"no ce_time":          func(e event) *kgo.Record { return without(e, "ce_time") },
		"no ce_specversion":   func(e event) *kgo.Record { return without(e, "ce_specversion") },
		"another specversion": func(e event) *kgo.Record { return without(e, "ce_specversion", "0.3") },
		"a time not RFC 3339": func(e event) *kgo.Record { return without(e, "ce_time", "26/11/2026 09:30") },
		"JSON":                func(e event) *kgo.Record { return without(e, "content-type", "application/json") },
	} {
		bad[name] = r(newEvent(invitation, name))
	}
	valid := newEvent(invitation, "valid")
	var records []*kgo.Record
	for _, r := range bad {
		records = append(records, r)
	}
	k.publish(t, append(records, valid.record())...)

	letters := k.readDeadLetters(t, len(bad))
	for _, letter := range letters {
		name := string(letter.Value)
		original, ok := bad[name]
		if !ok {
			t.Errorf("dead letter %q is not one of the bad records", name)
			continue
		}
		if string(letter.Key) != invitation {
			t.Errorf("%s: key = %q, want the record's", name, letter.Key)
		}
		for _, h := range original.Headers {
			if header(letter, h.Key) != string(h.Value) {
				t.Errorf("%s: header %s = %q, want the record's %q", name, h.Key, header(letter, h.Key), h.Value)
			}
		}
		if header(letter, "dead-letter-reason") == "" {
			t.Errorf("%s: no dead-letter-reason", name)
		}
	}
	eventually(t, 10*time.Second, func() bool { return len(effects(t, pool)) == 1 }, "the valid record after them was never handed over")
	if ids := got.ids(); !slices.Equal(ids, []string{valid.id}) {
		t.Errorf("handed over = %v, want only the valid record", ids)
	}
}

func TestARecordThatTheHandlerCannotDecodeGoesToTheDeadLettersWithTheReason(t *testing.T) {
	pool := newDatabase(t)
	k := newKafka(t, 1)
	garbled, valid := newEvent(invitation, "garbled"), newEvent(invitation, "valid")
	run(t, pool, k.options(), func(_ context.Context, e inbox.Event) (inbox.Effect, error) {
		if string(e.Value) == "garbled" {
			return nil, fmt.Errorf("%w: proto: cannot parse invalid wire-format data", inbox.ErrUndecodable)
		}
		return record(e), nil
	})

	k.publish(t, garbled.record(), valid.record())

	letter := k.readDeadLetters(t, 1)[0]
	if header(letter, "ce_id") != garbled.id || string(letter.Value) != "garbled" {
		t.Errorf("dead letter = %s %q, want the garbled record", header(letter, "ce_id"), letter.Value)
	}
	if reason := header(letter, "dead-letter-reason"); reason != "inbox: the record cannot be decoded: proto: cannot parse invalid wire-format data" {
		t.Errorf("reason = %q, want the handler's", reason)
	}
	eventually(t, 10*time.Second, func() bool { return slices.Equal(effects(t, pool), []string{valid.id}) },
		"the record after the dead letter was never handed over")
}
