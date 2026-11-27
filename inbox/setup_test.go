package inbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/ancyloce/contelys-cloud-libs-service/inbox"
	"github.com/ancyloce/contelys-cloud-libs-service/internal/pgtest"
)

func TestAConsumerNeedsItsSubscriptionItsTopicItsBrokersABackoffAndAHandler(t *testing.T) {
	pool := newDatabase(t)
	valid := inbox.Options{
		Subscriber: "notifications", Subscription: "invitations", Topic: topic,
		Brokers: []string{"127.0.0.1:9092"}, Backoff: time.Second,
	}
	skip := func(context.Context, inbox.Event) (inbox.Effect, error) { return nil, nil }
	for name, change := range map[string]func(*inbox.Options, *inbox.Handler){
		"without a subscriber":   func(o *inbox.Options, _ *inbox.Handler) { o.Subscriber = "" },
		"without a subscription": func(o *inbox.Options, _ *inbox.Handler) { o.Subscription = "" },
		"without a topic":        func(o *inbox.Options, _ *inbox.Handler) { o.Topic = "" },
		"without brokers":        func(o *inbox.Options, _ *inbox.Handler) { o.Brokers = nil },
		"without a backoff":      func(o *inbox.Options, _ *inbox.Handler) { o.Backoff = 0 },
		"without a handler":      func(_ *inbox.Options, h *inbox.Handler) { *h = nil },
	} {
		t.Run(name, func(t *testing.T) {
			opts, handle := valid, inbox.Handler(skip)
			change(&opts, &handle)
			if _, err := inbox.New(pool, opts, handle); err == nil {
				t.Error("New accepted the options")
			}
		})
	}
}

func TestInstancesThatStartTogetherMigrateTheInboxOnce(t *testing.T) {
	for round := range 5 {
		pool := pgtest.Database(t)
		errs := make(chan error, 4)
		for range 4 {
			go func() { errs <- inbox.Migrate(context.Background(), pool) }()
		}
		for range 4 {
			if err := <-errs; err != nil {
				t.Errorf("round %d: Migrate = %v, want nil on every instance", round, err)
			}
		}
	}
}
