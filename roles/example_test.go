package roles_test

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ancyloce/contelys-cloud-libs-service/barrier"
	"github.com/ancyloce/contelys-cloud-libs-service/roles"
	"github.com/ancyloce/contelys-cloud-libs-service/tokens"
)

// The errors of a use case, which the domain service's interface layer answers as 503
// unavailable, 404 not_found, 403 role_too_low and 403 principal_not_active.
var (
	errUnavailable = errors.New("unavailable")
	errNotFound    = errors.New("not found")
	errRoleTooLow  = errors.New("role too low")
	errNotActive   = errors.New("principal not active")
)

// The use case of a protected write whose minimum role, as the domain service's contract states
// it, is Editor. It asks again for the caller's role, and hands the caller's authority version
// and the membership's to the write barrier in the transaction that it commits. In a domain
// service the use case reaches the client and the pool through the ports that it declares.
func ExampleClient_Write() {
	var (
		client *roles.Client // roles.New(roles.Options{OrganizationAccess: …, Transport: …})
		pool   *pgxpool.Pool
	)
	renameProject := func(ctx context.Context, project, name string) error {
		answer, err := client.Write(ctx, roles.Scope{Kind: roles.Project, ID: project})
		switch {
		case err != nil:
			return errUnavailable
		case answer.Role == roles.None:
			return errNotFound // the caller learns nothing about what they hold no role on
		case !answer.Role.AtLeast(roles.Editor):
			return errRoleTooLow
		}
		caller, _ := tokens.InternalFrom(ctx)
		membership, _ := answer.Authority()
		tx, err := pool.Begin(ctx)
		if err != nil {
			return errUnavailable
		}
		defer func() { _ = tx.Rollback(ctx) }()
		err = barrier.Check(ctx, tx,
			barrier.AuthorityVersion{Kind: barrier.Principal, ID: caller.Principal, Version: caller.AuthorityVersion},
			membership)
		if errors.Is(err, barrier.ErrRevoked) {
			return errNotActive
		}
		if err != nil {
			return errUnavailable
		}
		// … the change to the project, in tx.
		return tx.Commit(ctx)
	}
	_ = renameProject
}
