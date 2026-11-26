package barrier_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ancyloce/contelys-cloud/libs/service/barrier"
)

func TestAWriteBelowTheInstalledMinimumIsRefused(t *testing.T) {
	pool := newDatabase(t)
	install(t, pool, barrier.Principal, principal, 4)

	if err := check(t, pool, barrier.AuthorityVersion{Kind: barrier.Principal, ID: principal, Version: 3}); !errors.Is(err, barrier.ErrRevoked) {
		t.Errorf("Check at version 3 = %v, want ErrRevoked", err)
	}
	for _, version := range []int64{4, 9} {
		if err := check(t, pool, barrier.AuthorityVersion{Kind: barrier.Principal, ID: principal, Version: version}); err != nil {
			t.Errorf("Check at version %d = %v, want nil", version, err)
		}
	}
	if err := check(t, pool, barrier.AuthorityVersion{Kind: barrier.Principal, ID: "01JDZ9A1B2C3D4E5F6G7H8J9KA", Version: 1}); err != nil {
		t.Errorf("Check of a principal without a minimum = %v, want nil", err)
	}
}

// waiting reports whether done stays open for a moment, as an operation that waits for a lock.
func waiting(done <-chan error) bool {
	select {
	case <-done:
		return false
	case <-time.After(300 * time.Millisecond):
		return true
	}
}

func TestAWriteThatHasCheckedCommitsBeforeAnInstallation(t *testing.T) {
	pool := newDatabase(t)
	ctx := context.Background()
	write, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = write.Rollback(ctx) }()
	if err := barrier.Check(ctx, write, barrier.AuthorityVersion{Kind: barrier.Principal, ID: principal, Version: 3}); err != nil {
		t.Fatalf("Check before the installation = %v, want nil", err)
	}

	installed := make(chan error, 1)
	go func() {
		installed <- inTx(t, pool, func(tx pgx.Tx) error { return barrier.Install(ctx, tx, barrier.Principal, principal, 4) })
	}()
	if !waiting(installed) {
		t.Fatal("the installation did not wait for the write that had checked")
	}
	if err := write.Commit(ctx); err != nil {
		t.Fatalf("the write that had checked did not commit: %v", err)
	}
	if err := <-installed; err != nil {
		t.Fatalf("Install: %v", err)
	}
	if err := check(t, pool, barrier.AuthorityVersion{Kind: barrier.Principal, ID: principal, Version: 3}); !errors.Is(err, barrier.ErrRevoked) {
		t.Errorf("Check after the installation = %v, want ErrRevoked", err)
	}
}

func TestAWriteThatChecksDuringAnInstallationSeesItsMinimum(t *testing.T) {
	pool := newDatabase(t)
	ctx := context.Background()
	installation, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = installation.Rollback(ctx) }()
	if err := barrier.Install(ctx, installation, barrier.Membership, membership, 4); err != nil {
		t.Fatalf("Install: %v", err)
	}

	checked := make(chan error, 1)
	go func() {
		checked <- check(t, pool, barrier.AuthorityVersion{Kind: barrier.Membership, ID: membership, Version: 3})
	}()
	if !waiting(checked) {
		t.Fatal("the write did not wait for the installation in progress")
	}
	if err := installation.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-checked; !errors.Is(err, barrier.ErrRevoked) {
		t.Errorf("Check during the installation = %v, want ErrRevoked once it committed", err)
	}
}

func TestAWriteIsRefusedWhenAnyOfItsAuthoritiesIsRevoked(t *testing.T) {
	pool := newDatabase(t)
	install(t, pool, barrier.Principal, principal, 2)
	install(t, pool, barrier.Membership, membership, 6)
	// As Organization Access's design has it: the internal token's version and the role answer's.
	write := []barrier.AuthorityVersion{
		{Kind: barrier.Principal, ID: principal, Version: 2},
		{Kind: barrier.Membership, ID: membership, Version: 5},
	}

	if err := check(t, pool, write...); !errors.Is(err, barrier.ErrRevoked) {
		t.Errorf("Check with a revoked membership = %v, want ErrRevoked", err)
	}
	write[1].Version = 6
	if err := check(t, pool, write...); err != nil {
		t.Errorf("Check with both versions at their minimum = %v, want nil", err)
	}
}

func TestAWriteUnderAStricterIsolationIsRefusedRatherThanCheckedAgainstAnOldSnapshot(t *testing.T) {
	pool := newDatabase(t)
	ctx := context.Background()
	for _, level := range []pgx.TxIsoLevel{pgx.RepeatableRead, pgx.Serializable} {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: level})
		if err != nil {
			t.Fatal(err)
		}
		// Its snapshot is taken before the lock is granted, so it could not see an installation
		// that it waited for.
		err = barrier.Check(ctx, tx, barrier.AuthorityVersion{Kind: barrier.Principal, ID: principal, Version: 3})
		_ = tx.Rollback(ctx)
		if !errors.Is(err, barrier.ErrIsolation) {
			t.Errorf("Check under %s = %v, want ErrIsolation", level, err)
		}
	}
}

func TestACheckFailsClosedOnAnUnknownKindOrNothingToCheck(t *testing.T) {
	pool := newDatabase(t)
	if err := check(t, pool, barrier.AuthorityVersion{Kind: "Principal", ID: principal, Version: 3}); err == nil {
		t.Error("Check of an unknown kind = nil, want an error")
	}
	if err := check(t, pool); err == nil {
		t.Error("Check of no authority version = nil, want an error")
	}
}
