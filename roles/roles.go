// Package roles asks Organization Access which role a principal holds on a scope, the role
// question of ADR 0056, and keeps the answers for reads (ADR 0031). A read may use an answer
// until its validity ends, a protected write asks again every time and hands the membership's
// authority version to the write barrier, and when no answer can be had the question fails,
// never giving a role (issue 54 of the phase 1 platform). Each domain service decides from the
// role whether an operation is allowed.
package roles

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/ancyloce/contelys-cloud-libs-service/barrier"
	"github.com/ancyloce/contelys-cloud-libs-service/roles/internal/gen"
	"github.com/ancyloce/contelys-cloud-libs-service/tokens"
)

// ErrNoAnswer is a question that got no answer: Organization Access could not be reached,
// passed the deadline, answered 503 or answered anything but a role. A domain service refuses
// the operation, answering 503 unavailable, and never takes it for a role.
var ErrNoAnswer = errors.New("no answer from Organization Access")

const (
	// deadline bounds one question, which is not retried (Organization Access's detailed design,
	// section 05).
	deadline = 250 * time.Millisecond
)

// Kind is the kind of a scope.
type Kind string

const (
	Organization Kind = "organization"
	Project      Kind = "project"
	Site         Kind = "site"
)

// Scope is an organization, a project or a site.
type Scope struct {
	Kind Kind
	ID   string
}

// identifier is the pattern of every scope's identifier in Organization Access's contract.
var identifier = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)

// possible tells whether some scope can be s: one of the three kinds, and an identifier of the
// contract's pattern.
func (s Scope) possible() bool {
	return (s.Kind == Organization || s.Kind == Project || s.Kind == Site) && identifier.MatchString(s.ID)
}

// Role is a role on a scope, or None (ADR 0055).
type Role string

const (
	None      Role = "none"
	Viewer    Role = "viewer"
	Editor    Role = "editor"
	Publisher Role = "publisher"
	Admin     Role = "admin"
	Owner     Role = "owner"
)

// ladder ranks the roles: each includes the ones below it, and Owner includes Admin (ADR 0055).
var ladder = map[Role]int{None: 0, Viewer: 1, Editor: 2, Publisher: 3, Admin: 4, Owner: 5}

// AtLeast tells whether r includes minimum, the minimum role of an operation as the domain
// service's contract states it. An unknown role includes none and is included by none.
func (r Role) AtLeast(minimum Role) bool {
	held, known := ladder[r]
	needed, needs := ladder[minimum]
	return known && needs && held >= needed
}

// Answer is Organization Access's answer: the role that a principal holds on a scope, and the
// membership whose authority version a protected write hands to the write barrier.
type Answer struct {
	Role Role
	// Membership is the identifier of the principal's membership in the scope's organization, or
	// "" when the principal never was a member of it.
	Membership string
	// AuthorityVersion is the membership's authority version, or 0 without a membership.
	AuthorityVersion int64
}

// Authority is the membership's authority version, which a protected write hands to
// barrier.Check, and false when the answer names no membership.
func (a Answer) Authority() (barrier.AuthorityVersion, bool) {
	if a.Membership == "" {
		return barrier.AuthorityVersion{}, false
	}
	return barrier.AuthorityVersion{Kind: barrier.Membership, ID: a.Membership, Version: a.AuthorityVersion}, true
}

// Options say where Organization Access is.
type Options struct {
	// OrganizationAccess is Organization Access's base address: Edge's internal address followed
	// by /organization-access, as its contract states.
	OrganizationAccess string
	// Transport reaches Edge's internal listener, or http.DefaultTransport when nil.
	Transport http.RoundTripper
	// Holder, when set, presents the workload tokens of the questions asked with no user, by
	// ReadFor and WriteFor. Identity issues them as Organization Access's access file permits
	// getRole to the deployable.
	Holder *tokens.Holder
}

// Client asks Organization Access the role question, and keeps the answers for reads.
type Client struct {
	// forCaller asks for a caller, with the caller's internal token.
	forCaller *gen.ClientWithResponses
	// withNoUser asks with no user, with a workload token. It is nil without a Holder.
	withNoUser *gen.ClientWithResponses
	now        func() time.Time
	roles      cache[question, Answer]
	// scopes keeps the scopes question's answers, under the principal they are about.
	scopes cache[string, []HeldScope]
}

// question is what a role answer is about, and whether the question names its principal, as one
// with no user does, or is about the caller of an internal token.
type question struct {
	principal string
	scope     Scope
	named     bool
}

// New returns a client. It asks nothing until its first question.
func New(opts Options) (*Client, error) {
	if opts.OrganizationAccess == "" {
		return nil, errors.New("roles: a client needs the address of Organization Access")
	}
	c := &Client{now: time.Now}
	var err error
	if c.forCaller, err = clientOf(opts.OrganizationAccess, tokens.PassOn(opts.Transport)); err != nil {
		return nil, err
	}
	if opts.Holder != nil {
		if c.withNoUser, err = clientOf(opts.OrganizationAccess, opts.Holder.Transport("organization-access", opts.Transport)); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// clientOf is a generated client of Organization Access whose calls go through transport.
func clientOf(address string, transport http.RoundTripper) (*gen.ClientWithResponses, error) {
	client, err := gen.NewClientWithResponses(address, gen.WithHTTPClient(&http.Client{
		Transport: transport,
		// A redirect is no answer, and would carry the token elsewhere.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}))
	if err != nil {
		return nil, fmt.Errorf("roles: %w", err)
	}
	return client, nil
}

// Read answers which role the caller holds on scope, for a read. The caller is the principal of
// the internal token that tokens.RequireInternal admitted into ctx, and the question carries
// that token.
func (c *Client) Read(ctx context.Context, scope Scope) (Answer, error) {
	principal, err := caller(ctx)
	if err != nil {
		return Answer{}, err
	}
	return c.role(ctx, question{principal: principal, scope: scope}, true)
}

// Write answers which role the caller holds on scope, for a protected write: it asks
// Organization Access every time. The write hands the answer's Authority to barrier.Check in
// the transaction that commits it.
func (c *Client) Write(ctx context.Context, scope Scope) (Answer, error) {
	principal, err := caller(ctx)
	if err != nil {
		return Answer{}, err
	}
	return c.role(ctx, question{principal: principal, scope: scope}, false)
}

// ReadFor answers which role principal holds on scope, for a read made with no user, such as
// one of a job. The question carries a workload token of the Holder and names the principal.
func (c *Client) ReadFor(ctx context.Context, principal string, scope Scope) (Answer, error) {
	q, err := c.named(principal, scope)
	if err != nil {
		return Answer{}, err
	}
	return c.role(ctx, q, true)
}

// WriteFor answers which role principal holds on scope, for a protected write made with no
// user: it asks every time, as Write does, with a workload token, as ReadFor does.
func (c *Client) WriteFor(ctx context.Context, principal string, scope Scope) (Answer, error) {
	q, err := c.named(principal, scope)
	if err != nil {
		return Answer{}, err
	}
	return c.role(ctx, q, false)
}

// caller is the principal of the internal token that tokens.RequireInternal admitted into ctx.
func caller(ctx context.Context) (string, error) {
	admitted, ok := tokens.InternalFrom(ctx)
	if !ok {
		return "", fmt.Errorf("roles: %w: %v", ErrNoAnswer, tokens.ErrNoInternalToken)
	}
	return admitted.Principal, nil
}

// named is the question about scope that names principal, which only a client with a Holder can
// ask.
func (c *Client) named(principal string, scope Scope) (question, error) {
	switch {
	case c.withNoUser == nil:
		return question{}, fmt.Errorf("roles: %w: a question with no user needs a Holder", ErrNoAnswer)
	case principal == "":
		return question{}, fmt.Errorf("roles: %w: a question with no user names its principal", ErrNoAnswer)
	}
	return question{principal: principal, scope: scope, named: true}, nil
}

// role answers q, for a read with the answer kept for it if any. A scope that no scope can be
// holds no role, like one that was never registered, and is not asked about, because
// Organization Access would refuse the question as invalid, which is no outage.
func (c *Client) role(ctx context.Context, q question, read bool) (Answer, error) {
	if !q.scope.possible() {
		return Answer{Role: None}, nil
	}
	client, params := c.forCaller, &gen.GetRoleParams{Kind: gen.ScopeKind(q.scope.Kind), Scope: q.scope.ID}
	if q.named {
		client, params.Principal = c.withNoUser, &q.principal
	}
	ask := func(ctx context.Context) (Answer, time.Time, error) {
		response, err := client.GetRoleWithResponse(ctx, params)
		if err != nil {
			return Answer{}, time.Time{}, unreachable(err)
		}
		if response.JSON200 == nil {
			return Answer{}, time.Time{}, refused(response.StatusCode(), response.Body)
		}
		got, err := roleAnswer(*response.JSON200)
		return got, response.JSON200.ValidUntil, err
	}
	if read {
		return reading(ctx, c, &c.roles, q, ask)
	}
	return asking(ctx, c, &c.roles, q, ask)
}

// reading answers a read with the answer kept under key, or else asks as asking does.
func reading[K comparable, V any](ctx context.Context, c *Client, kept *cache[K, V], key K,
	ask func(context.Context) (V, time.Time, error)) (V, error) {
	if value, ok := kept.get(key, c.now()); ok {
		return value, nil
	}
	return asking(ctx, c, kept, key, ask)
}

// asking asks one question within the deadline, once, and keeps its answer under key in kept.
// ask returns the answer and the time until which it is valid, or an error that wraps
// ErrNoAnswer.
func asking[K comparable, V any](ctx context.Context, c *Client, kept *cache[K, V], key K,
	ask func(context.Context) (V, time.Time, error)) (V, error) {
	asked := c.now()
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	value, validUntil, err := ask(ctx)
	if err != nil {
		var none V
		return none, err
	}
	kept.put(key, value, asked, validUntil, c.now())
	return value, nil
}

// unreachable is the error of a question that got no response: Organization Access could not be
// reached, the question passed its deadline, or it could not be sent.
func unreachable(err error) error {
	return fmt.Errorf("roles: %w: %v", ErrNoAnswer, err)
}

// refused is the error of an answer that is not a role, named by its status and its code.
func refused(status int, body []byte) error {
	var problem struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(body, &problem)
	if problem.Code == "" {
		problem.Code = http.StatusText(status)
	}
	return fmt.Errorf("roles: %w: Organization Access answered %d %s", ErrNoAnswer, status, problem.Code)
}

// roleAnswer checks a role answer against the contract: a known role, a membership with its
// authority version or neither, a membership with every role but none, and a validity.
func roleAnswer(a gen.RoleAnswer) (Answer, error) {
	membership := a.Membership != nil
	switch {
	case !a.Role.Valid(), a.ValidUntil.IsZero(), membership != (a.AuthorityVersion != nil),
		membership && (*a.Membership == "" || *a.AuthorityVersion < 1),
		a.Role != gen.RoleOrNoneNone && !membership:
		return Answer{}, fmt.Errorf("roles: %w: the role answer does not fit the contract", ErrNoAnswer)
	}
	got := Answer{Role: Role(a.Role)}
	if membership {
		got.Membership, got.AuthorityVersion = *a.Membership, *a.AuthorityVersion
	}
	return got, nil
}
