// Package barrier is the write barrier of a registered domain service (ADR 0050): the minimum
// authority version of each principal and each membership, installed by its authority through the
// shared operation (ADR 0068), and the check that a commit makes against it.
package barrier

import (
	"cmp"
	"context"
	"embed"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ancyloce/contelys-cloud/libs/service/internal/migration"
)

// ErrRevoked is a protected write whose caller carries an authority version below the stored
// minimum. A service answers it with 403 principal_not_active.
var ErrRevoked = errors.New("the authority version is below the installed minimum")

// ErrIsolation is a check made in a transaction whose isolation is stricter than READ COMMITTED.
// Such a transaction reads from a snapshot taken before the barrier's lock is granted, so it could
// miss an installation that it waited for; the check refuses rather than pass on an old minimum.
var ErrIsolation = errors.New("the write barrier needs a READ COMMITTED transaction")

// Kind is what a minimum belongs to.
type Kind string

const (
	// Principal is a principal, whose authority version Identity keeps.
	Principal Kind = "principal"
	// Membership is a membership, one principal in one organization, whose authority version
	// Organization Access keeps.
	Membership Kind = "membership"
)

// AuthorityVersion is one authority version that a protected write carries: the internal token's
// for its principal, or the role answer's for its membership.
type AuthorityVersion struct {
	Kind    Kind
	ID      string
	Version int64
}

//go:embed migrations/*.sql
var migrations embed.FS

const (
	// versionTable is goose's table of the barrier's migrations, apart from the service's own.
	versionTable = "barrier_goose_version"
	// migrationLock is the advisory lock under which an instance migrates the barrier. Its value
	// spells "barrier" in ASCII.
	migrationLock int64 = 0x62617272696572
)

// Migrate creates or updates the barrier's table in the service's database.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if err := migration.Up(ctx, pool, migrations, versionTable, migrationLock); err != nil {
		return fmt.Errorf("barrier: %w", err)
	}
	return nil
}

// lockKey is the advisory lock of one minimum. A check takes it shared and an installation
// exclusive, both until their transaction ends, so a write that has checked commits before an
// installation of the same minimum, and a write that checks during one sees its minimum. Two
// minimums that share a key only wait for each other.
const lockKey = "hashtextextended($1 || '/' || $2, 0)"

// Check is the check that a protected write makes inside the transaction that commits it, which
// must be READ COMMITTED, PostgreSQL's default. It refuses with ErrRevoked an authority whose
// version is below the minimum stored for it, and holds each authority's lock until tx ends. It
// fails closed: an unknown kind, or nothing to check, is an error.
func Check(ctx context.Context, tx pgx.Tx, versions ...AuthorityVersion) error {
	if len(versions) == 0 {
		return errors.New("barrier: a protected write has at least one authority version to check")
	}
	for _, version := range versions {
		if version.Kind != Principal && version.Kind != Membership {
			return fmt.Errorf("barrier: unknown kind %q", version.Kind)
		}
	}
	var isolation string
	if err := tx.QueryRow(ctx, "SELECT current_setting('transaction_isolation')").Scan(&isolation); err != nil {
		return fmt.Errorf("barrier: %w", err)
	}
	if isolation != "read committed" {
		return fmt.Errorf("barrier: %s: %w", isolation, ErrIsolation)
	}
	// One order of locks for every write, so that two writes cannot wait for each other.
	sorted := slices.SortedFunc(slices.Values(versions), func(a, b AuthorityVersion) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.ID, b.ID))
	})
	for _, version := range sorted {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared("+lockKey+")", version.Kind, version.ID); err != nil {
			return fmt.Errorf("barrier: %w", err)
		}
		var minimum int64
		err := tx.QueryRow(ctx, "SELECT minimum FROM minimum_authority_versions WHERE kind = $1 AND id = $2",
			version.Kind, version.ID).Scan(&minimum)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf("barrier: %w", err)
		}
		if version.Version < minimum {
			return fmt.Errorf("barrier: %s %s at version %d: %w", version.Kind, version.ID, version.Version, ErrRevoked)
		}
	}
	return nil
}

// install keeps the higher of the stored minimum of kind and id and minimum, in tx, under the
// minimum's lock. A minimum lower than or equal to the stored one changes nothing. Only the shared
// operation installs a minimum, after it has checked the caller.
func install(ctx context.Context, tx pgx.Tx, kind Kind, id string, minimum int64) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock("+lockKey+")", kind, id); err != nil {
		return fmt.Errorf("barrier: %w", err)
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO minimum_authority_versions (kind, id, minimum) VALUES ($1, $2, $3)
		 ON CONFLICT (kind, id) DO UPDATE SET minimum = GREATEST(minimum_authority_versions.minimum, excluded.minimum)`,
		kind, id, minimum)
	if err != nil {
		return fmt.Errorf("barrier: %w", err)
	}
	return nil
}
