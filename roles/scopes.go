package roles

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/ancyloce/contelys-cloud-libs-service/roles/internal/gen"
)

// HeldScope is a scope on which a principal holds a role itself.
type HeldScope struct {
	Scope Scope
	// Role is the role held on Scope itself. A role on a scope stands for the scopes it contains.
	Role Role
	// ContainedIn are the scopes that contain Scope, the nearest first, ending with its
	// organization, and none for an organization.
	ContainedIn []Scope
}

// Scopes lists the scopes on which the caller holds a role itself, in every organization of
// which they are an active member: the scopes question, for reads. It is asked and kept by the
// rules of the role question, and the caller may change the list it gets. The caller is the
// principal of the internal token that tokens.RequireInternal admitted into ctx.
func (c *Client) Scopes(ctx context.Context) ([]HeldScope, error) {
	principal, err := caller(ctx)
	if err != nil {
		return nil, err
	}
	held, err := reading(ctx, c, &c.scopes, principal, func(ctx context.Context) ([]HeldScope, time.Time, error) {
		response, err := c.forCaller.ListScopesWithResponse(ctx)
		if err != nil {
			return nil, time.Time{}, unreachable(err)
		}
		if response.JSON200 == nil {
			return nil, time.Time{}, refused(response.StatusCode(), response.Body)
		}
		held, err := scopeList(*response.JSON200)
		return held, response.JSON200.ValidUntil, err
	})
	if err != nil {
		return nil, err
	}
	copied := slices.Clone(held)
	for i := range copied {
		copied[i].ContainedIn = slices.Clone(copied[i].ContainedIn)
	}
	return copied, nil
}

// scopeList checks a scope list against the contract: scopes and containers of a known kind,
// each scope with a role, and a validity.
func scopeList(list gen.ScopeList) ([]HeldScope, error) {
	invalid := fmt.Errorf("roles: %w: the scope list does not fit the contract", ErrNoAnswer)
	if list.ValidUntil.IsZero() {
		return nil, invalid
	}
	held := make([]HeldScope, 0, len(list.Scopes))
	for _, h := range list.Scopes {
		if !h.Role.Valid() || !h.Scope.Kind.Valid() {
			return nil, invalid
		}
		one := HeldScope{Scope: Scope{Kind: Kind(h.Scope.Kind), ID: h.Scope.Id}, Role: Role(h.Role)}
		for _, container := range h.ContainedIn {
			if !container.Kind.Valid() {
				return nil, invalid
			}
			one.ContainedIn = append(one.ContainedIn, Scope{Kind: Kind(container.Kind), ID: container.Id})
		}
		held = append(held, one)
	}
	return held, nil
}
