package roles_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ancyloce/contelys-cloud-libs-service/barrier"
	"github.com/ancyloce/contelys-cloud-libs-service/internal/identitytest"
	"github.com/ancyloce/contelys-cloud-libs-service/roles"
	"github.com/ancyloce/contelys-cloud-libs-service/tokens"
)

const (
	// member is a principal, and membership their membership in the project's organization.
	member     = "01JDZ8Q3V7XK2M4N6P8R0T2W4Y"
	membership = "01JE0A7M3MB3R5H1P0000000AB"
)

var project = roles.Scope{Kind: roles.Project, ID: "prj-7f3a"}

// fixture is a domain service, Projects, that asks a fake Organization Access through the library.
type fixture struct {
	id       *identitytest.Identity
	key      identitytest.Key
	clock    *clock
	oa       *organizationAccess
	verifier *tokens.Verifier
	client   *roles.Client
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	id := identitytest.New(t)
	key := identitytest.NewKey(t, "2026-11")
	id.Publish(key)
	clock := newClock()
	f := &fixture{
		id:       id,
		key:      key,
		clock:    clock,
		oa:       newOrganizationAccess(t, id, clock),
		verifier: tokens.New(tokens.Options{KeySet: id.KeySet(), Client: http.DefaultClient, Deployable: "projects"}),
	}
	f.client = f.newClient(t, roles.Options{})
	return f
}

// newClient returns a client of the fixture's Organization Access, on the fixture's clock.
func (f *fixture) newClient(t *testing.T, opts roles.Options) *roles.Client {
	t.Helper()
	opts.OrganizationAccess = f.oa.address()
	client, err := roles.New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	roles.SetClock(client, f.clock.Now)
	return client
}

// as returns the context of a request of principal's that the domain service admitted, as
// tokens.RequireInternal puts it.
func (f *fixture) as(t *testing.T, principal string) context.Context {
	t.Helper()
	var admitted context.Context
	handler := f.verifier.RequireInternal(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		admitted = r.Context()
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+identitytest.Sign(t, f.key, "internal+jwt", identitytest.Internal(principal)))
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if admitted == nil {
		t.Fatal("the domain service did not admit the caller")
	}
	return admitted
}

func TestAQuestionForACallerCarriesTheCallersTokenAndAnswersTheirRole(t *testing.T) {
	f := newFixture(t)
	f.oa.give(member, "project", "prj-7f3a", held{role: "editor", membership: membership, authorityVersion: 4})

	got, err := f.client.Read(f.as(t, member), project)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if want := (roles.Answer{Role: roles.Editor, Membership: membership, AuthorityVersion: 4}); got != want {
		t.Errorf("answer = %+v, want %+v", got, want)
	}
	asked := f.oa.asked()
	if want := (question{token: "internal", kind: "project", scope: "prj-7f3a"}); len(asked) != 1 || asked[0] != want {
		t.Errorf("questions = %+v, want one with the caller's internal token and no principal", asked)
	}
}

// read makes a read of the project in ctx, and fails the test without an answer.
func (f *fixture) read(t *testing.T, ctx context.Context) roles.Answer {
	t.Helper()
	got, err := f.client.Read(ctx, project)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return got
}

func TestAReadUsesAnAnswerUntilItsValidityEnds(t *testing.T) {
	f := newFixture(t)
	f.oa.give(member, "project", "prj-7f3a", held{role: "editor", membership: membership, authorityVersion: 4})
	f.oa.set(func(oa *organizationAccess) { oa.validity = 20 * time.Second })
	ctx := f.as(t, member)

	f.read(t, ctx)
	f.oa.give(member, "project", "prj-7f3a", held{role: "viewer", membership: membership, authorityVersion: 4})
	f.clock.advance(19 * time.Second)
	if got := f.read(t, ctx); got.Role != roles.Editor || len(f.oa.asked()) != 1 {
		t.Errorf("within the validity: role %s after %d questions, want the kept editor after 1", got.Role, len(f.oa.asked()))
	}
	f.clock.advance(time.Second)
	if got := f.read(t, ctx); got.Role != roles.Viewer || len(f.oa.asked()) != 2 {
		t.Errorf("at the end of the validity: role %s after %d questions, want viewer after 2", got.Role, len(f.oa.asked()))
	}
}

func TestAReadNeverUsesAnAnswerLongerThan60Seconds(t *testing.T) {
	f := newFixture(t)
	f.oa.give(member, "project", "prj-7f3a", held{role: "editor", membership: membership, authorityVersion: 4})
	f.oa.set(func(oa *organizationAccess) { oa.validity = 10 * time.Minute })
	ctx := f.as(t, member)

	f.read(t, ctx)
	f.clock.advance(59 * time.Second)
	f.read(t, ctx)
	if got := len(f.oa.asked()); got != 1 {
		t.Errorf("after 59 seconds, %d questions, want 1", got)
	}
	f.clock.advance(time.Second)
	f.read(t, ctx)
	if got := len(f.oa.asked()); got != 2 {
		t.Errorf("after 60 seconds, %d questions, want 2: an answer is kept for at most 60 seconds", got)
	}
}

func TestAnAnswerIsKeptForItsPrincipalAndItsScopeAlone(t *testing.T) {
	f := newFixture(t)
	other := "01JDZ9A1B2C3D4E5F6G7H8J9KA"
	f.oa.give(member, "project", "prj-7f3a", held{role: "editor", membership: membership, authorityVersion: 4})
	f.oa.give(other, "project", "prj-7f3a", held{role: "viewer", membership: "01JE0A7M3MB3R5H1P0000000CD", authorityVersion: 1})

	f.read(t, f.as(t, member))
	if got := f.read(t, f.as(t, other)); got.Role != roles.Viewer {
		t.Errorf("another principal's role = %s, want their own viewer", got.Role)
	}
	if got, err := f.client.Read(f.as(t, member), roles.Scope{Kind: roles.Site, ID: "prj-7f3a"}); err != nil || got.Role != roles.None {
		t.Errorf("a site of the same identifier = %+v, %v, want its own answer, none", got, err)
	}
	f.read(t, f.as(t, member))
	if got := len(f.oa.asked()); got != 3 {
		t.Errorf("%d questions, want 3: one for each principal and scope", got)
	}
}

func TestAProtectedWriteAsksAgainEveryTimeAndHandsTheMembershipToTheWriteBarrier(t *testing.T) {
	f := newFixture(t)
	f.oa.give(member, "project", "prj-7f3a", held{role: "editor", membership: membership, authorityVersion: 4})
	ctx := f.as(t, member)
	f.read(t, ctx)

	f.oa.give(member, "project", "prj-7f3a", held{role: "viewer", membership: membership, authorityVersion: 4})
	got, err := f.client.Write(ctx, project)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got.Role != roles.Viewer {
		t.Errorf("a write's role = %s, want the lowered viewer: a write never uses a kept answer", got.Role)
	}
	authority, ok := got.Authority()
	if want := (barrier.AuthorityVersion{Kind: barrier.Membership, ID: membership, Version: 4}); !ok || authority != want {
		t.Errorf("Authority() = %+v, %t, want %+v", authority, ok, want)
	}
	if _, err := f.client.Write(ctx, project); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := len(f.oa.asked()); got != 3 {
		t.Errorf("%d questions after a read and two writes, want 3", got)
	}

	// The answer of a write is the newest, so later reads use it.
	if got := f.read(t, ctx); got.Role != roles.Viewer || len(f.oa.asked()) != 3 {
		t.Errorf("a read after the writes: role %s after %d questions, want the write's viewer after 3", got.Role, len(f.oa.asked()))
	}
}

func TestWithoutAnAnswerAWriteAndAReadWithNoAnswerLeftAreRefused(t *testing.T) {
	failures := map[string]func(f *fixture){
		"Organization Access answers 503":       func(f *fixture) { f.oa.set(func(oa *organizationAccess) { oa.down = true }) },
		"Organization Access cannot be reached": func(f *fixture) { f.oa.server.Close() },
		"the question passes its deadline": func(f *fixture) {
			f.oa.set(func(oa *organizationAccess) { oa.delay = 400 * time.Millisecond })
		},
		"the answer is not a role": func(f *fixture) {
			f.oa.set(func(oa *organizationAccess) { oa.raw = `{"role":"superuser","validUntil":"2026-11-26T09:01:00Z"}` })
		},
	}
	for name, fail := range failures {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.oa.give(member, "project", "prj-7f3a", held{role: "editor", membership: membership, authorityVersion: 4})
			ctx := f.as(t, member)
			f.read(t, ctx)
			fail(f)

			if got := f.read(t, ctx); got.Role != roles.Editor {
				t.Errorf("a read that holds an answer = %s, want the kept editor", got.Role)
			}
			started := time.Now()
			got, err := f.client.Write(ctx, project)
			if !errors.Is(err, roles.ErrNoAnswer) || got != (roles.Answer{}) {
				t.Errorf("a protected write = %+v, %v, want no role and ErrNoAnswer", got, err)
			}
			if took := time.Since(started); took > 350*time.Millisecond {
				t.Errorf("the write was refused after %s, want within the deadline of 250 ms", took)
			}
			f.clock.advance(60 * time.Second)
			if got, err := f.client.Read(ctx, project); !errors.Is(err, roles.ErrNoAnswer) || got != (roles.Answer{}) {
				t.Errorf("a read with no answer left = %+v, %v, want no role and ErrNoAnswer", got, err)
			}
			if name != "Organization Access cannot be reached" && len(f.oa.asked()) != 3 {
				t.Errorf("%d questions, want 3: a question that gets no answer is not asked again", len(f.oa.asked()))
			}
		})
	}
}

func TestAnAnswerThatDoesNotFitTheContractIsNoAnswer(t *testing.T) {
	answers := map[string]string{
		"an unknown role":                           `{"role":"superuser","membership":"m-1","authorityVersion":1,"validUntil":"2026-11-26T09:01:00Z"}`,
		"a role without a membership":               `{"role":"editor","validUntil":"2026-11-26T09:01:00Z"}`,
		"a membership without an authority version": `{"role":"editor","membership":"m-1","validUntil":"2026-11-26T09:01:00Z"}`,
		"an authority version below 1":              `{"role":"editor","membership":"m-1","authorityVersion":0,"validUntil":"2026-11-26T09:01:00Z"}`,
		"no validity":                               `{"role":"none"}`,
		"no JSON object":                            `[]`,
	}
	for name, raw := range answers {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.oa.set(func(oa *organizationAccess) { oa.raw = raw })
			if got, err := f.client.Read(f.as(t, member), project); !errors.Is(err, roles.ErrNoAnswer) || got != (roles.Answer{}) {
				t.Errorf("Read = %+v, %v, want no role and ErrNoAnswer", got, err)
			}
		})
	}
}

func TestNoRoleIsAnAnswer(t *testing.T) {
	f := newFixture(t)
	removed := "01JDZ9A1B2C3D4E5F6G7H8J9KA"
	f.oa.give(removed, "project", "prj-7f3a", held{role: "none", membership: "01JE0A7M3MB3R5H1P0000000CD", authorityVersion: 5})

	got, err := f.client.Read(f.as(t, member), project)
	if err != nil || got != (roles.Answer{Role: roles.None}) {
		t.Errorf("a principal who never was a member = %+v, %v, want none and no error", got, err)
	}
	if _, ok := got.Authority(); ok {
		t.Error("an answer without a membership has an Authority, want none")
	}
	got, err = f.client.Write(f.as(t, removed), project)
	if want := (roles.Answer{Role: roles.None, Membership: "01JE0A7M3MB3R5H1P0000000CD", AuthorityVersion: 5}); err != nil || got != want {
		t.Errorf("a removed member = %+v, %v, want %+v", got, err, want)
	}
	f.read(t, f.as(t, member))
	if got := len(f.oa.asked()); got != 2 {
		t.Errorf("%d questions, want 2: none is kept like any answer", got)
	}
}

func TestAQuestionForACallerNeedsTheCallersInternalToken(t *testing.T) {
	f := newFixture(t)
	for name, ask := range map[string]func(context.Context) (roles.Answer, error){
		"Read":  func(ctx context.Context) (roles.Answer, error) { return f.client.Read(ctx, project) },
		"Write": func(ctx context.Context) (roles.Answer, error) { return f.client.Write(ctx, project) },
	} {
		if _, err := ask(context.Background()); !errors.Is(err, roles.ErrNoAnswer) {
			t.Errorf("%s outside an admitted request = %v, want ErrNoAnswer", name, err)
		}
	}
	if got := len(f.oa.asked()); got != 0 {
		t.Errorf("%d questions, want none", got)
	}
}

// withHolder returns a client whose questions with no user carry Projects' workload tokens, which
// the fixture's Identity issues for getRole.
func (f *fixture) withHolder(t *testing.T) *roles.Client {
	t.Helper()
	f.id.Register("projects", "projects-secret")
	f.id.Grant(f.key, "projects", "organization-access", "getRole")
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("projects-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	holder, err := tokens.NewHolder(tokens.HolderOptions{
		WorkloadTokens: f.id.WorkloadTokens(),
		Client:         http.DefaultClient,
		Deployable:     "projects",
		SecretFile:     secret,
	})
	if err != nil {
		t.Fatalf("NewHolder: %v", err)
	}
	return f.newClient(t, roles.Options{Holder: holder})
}

func TestAQuestionWithNoUserCarriesAWorkloadTokenAndNamesThePrincipal(t *testing.T) {
	f := newFixture(t)
	client := f.withHolder(t)
	f.oa.give(member, "project", "prj-7f3a", held{role: "editor", membership: membership, authorityVersion: 4})
	ctx := context.Background()

	got, err := client.ReadFor(ctx, member, project)
	if want := (roles.Answer{Role: roles.Editor, Membership: membership, AuthorityVersion: 4}); err != nil || got != want {
		t.Errorf("ReadFor = %+v, %v, want %+v", got, err, want)
	}
	if _, err := client.ReadFor(ctx, member, project); err != nil {
		t.Errorf("ReadFor: %v", err)
	}
	if _, err := client.WriteFor(ctx, member, project); err != nil {
		t.Errorf("WriteFor: %v", err)
	}
	want := question{token: "workload", principal: member, kind: "project", scope: "prj-7f3a"}
	if asked := f.oa.asked(); len(asked) != 2 || asked[0] != want || asked[1] != want {
		t.Errorf("questions = %+v, want two with a workload token naming the principal: a read and a write", asked)
	}
}

func TestAQuestionWithNoUserFailsWithoutAWorkloadTokenOrAPrincipal(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if _, err := f.client.ReadFor(ctx, member, project); !errors.Is(err, roles.ErrNoAnswer) {
		t.Errorf("ReadFor of a client without a Holder = %v, want ErrNoAnswer", err)
	}
	client := f.withHolder(t)
	if _, err := client.WriteFor(ctx, "", project); !errors.Is(err, roles.ErrNoAnswer) {
		t.Errorf("WriteFor of no principal = %v, want ErrNoAnswer", err)
	}
	f.id.SetDown(true)
	if _, err := client.WriteFor(ctx, member, project); !errors.Is(err, roles.ErrNoAnswer) {
		t.Errorf("WriteFor with Identity down = %v, want ErrNoAnswer", err)
	}
	if got := len(f.oa.asked()); got != 0 {
		t.Errorf("%d questions, want none", got)
	}
}

func TestTheScopesQuestionIsAskedAndKeptByTheSameRules(t *testing.T) {
	f := newFixture(t)
	organization := map[string]any{"kind": "organization", "id": "org-1"}
	f.oa.list(member,
		map[string]any{"scope": organization, "role": "admin", "containedIn": []any{}},
		map[string]any{"scope": map[string]any{"kind": "site", "id": "site-9"}, "role": "editor",
			"containedIn": []any{map[string]any{"kind": "project", "id": "prj-7f3a"}, organization}},
	)
	f.oa.set(func(oa *organizationAccess) { oa.validity = 10 * time.Minute })
	ctx := f.as(t, member)
	scopes := func() []roles.HeldScope {
		t.Helper()
		got, err := f.client.Scopes(ctx)
		if err != nil {
			t.Fatalf("Scopes: %v", err)
		}
		return got
	}

	want := []roles.HeldScope{
		{Scope: roles.Scope{Kind: roles.Organization, ID: "org-1"}, Role: roles.Admin},
		{Scope: roles.Scope{Kind: roles.Site, ID: "site-9"}, Role: roles.Editor, ContainedIn: []roles.Scope{
			{Kind: roles.Project, ID: "prj-7f3a"}, {Kind: roles.Organization, ID: "org-1"},
		}},
	}
	got := scopes()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Scopes = %+v\nwant %+v", got, want)
	}
	if asked := f.oa.asked(); len(asked) != 1 || asked[0] != (question{token: "internal", principal: member, kind: "scopes"}) {
		t.Errorf("questions = %+v, want one with the caller's internal token", asked)
	}

	got[0].Role, got[1].ContainedIn[0].ID = roles.Owner, "prj-0000"
	f.clock.advance(59 * time.Second)
	if got := scopes(); !reflect.DeepEqual(got, want) || len(f.oa.asked()) != 1 {
		t.Errorf("after 59 seconds = %+v after %d questions, want the kept list, unchanged by its caller, after 1", got, len(f.oa.asked()))
	}

	f.oa.set(func(oa *organizationAccess) { oa.down = true })
	f.clock.advance(time.Second)
	if got, err := f.client.Scopes(ctx); !errors.Is(err, roles.ErrNoAnswer) || got != nil {
		t.Errorf("after 60 seconds with Organization Access down = %+v, %v, want no list and ErrNoAnswer", got, err)
	}
	f.oa.set(func(oa *organizationAccess) { oa.down, oa.delay = false, 400*time.Millisecond })
	started := time.Now()
	if _, err := f.client.Scopes(ctx); !errors.Is(err, roles.ErrNoAnswer) || time.Since(started) > 350*time.Millisecond {
		t.Errorf("a slow answer = %v after %s, want ErrNoAnswer within the deadline of 250 ms", err, time.Since(started))
	}
	if got := len(f.oa.asked()); got != 3 {
		t.Errorf("%d questions, want 3: a question that gets no answer is not asked again", got)
	}
	if _, err := f.client.Scopes(context.Background()); !errors.Is(err, roles.ErrNoAnswer) {
		t.Errorf("Scopes outside an admitted request = %v, want ErrNoAnswer", err)
	}
}

func TestAScopeListThatDoesNotFitTheContractIsNoAnswer(t *testing.T) {
	lists := map[string]string{
		"a scope with no role":   `{"scopes":[{"scope":{"kind":"project","id":"prj-7f3a"},"role":"none","containedIn":[]}],"validUntil":"2026-11-26T09:01:00Z"}`,
		"a scope of no kind":     `{"scopes":[{"scope":{"kind":"page","id":"pg-1"},"role":"editor","containedIn":[]}],"validUntil":"2026-11-26T09:01:00Z"}`,
		"a container of no kind": `{"scopes":[{"scope":{"kind":"site","id":"s-1"},"role":"editor","containedIn":[{"kind":"page","id":"pg-1"}]}],"validUntil":"2026-11-26T09:01:00Z"}`,
		"no validity":            `{"scopes":[]}`,
	}
	for name, raw := range lists {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.oa.set(func(oa *organizationAccess) { oa.raw = raw })
			if got, err := f.client.Scopes(f.as(t, member)); !errors.Is(err, roles.ErrNoAnswer) || got != nil {
				t.Errorf("Scopes = %+v, %v, want no list and ErrNoAnswer", got, err)
			}
		})
	}
}

func TestARoleIncludesTheRolesBelowItOnTheLadder(t *testing.T) {
	cases := []struct {
		held, minimum roles.Role
		want          bool
	}{
		{roles.Viewer, roles.Viewer, true},
		{roles.Editor, roles.Viewer, true},
		{roles.Viewer, roles.Editor, false},
		{roles.Publisher, roles.Editor, true},
		{roles.Editor, roles.Publisher, false},
		{roles.Admin, roles.Publisher, true},
		{roles.Owner, roles.Admin, true},
		{roles.Admin, roles.Owner, false},
		{roles.None, roles.Viewer, false},
		{roles.Role(""), roles.Viewer, false},
		{roles.Role("superuser"), roles.Viewer, false},
		{roles.Owner, roles.Role("superuser"), false},
	}
	for _, c := range cases {
		if got := c.held.AtLeast(c.minimum); got != c.want {
			t.Errorf("%q.AtLeast(%q) = %t, want %t", c.held, c.minimum, got, c.want)
		}
	}
}

func TestAScopeThatNoScopeCanBeHoldsNoRoleAndIsNotAskedAbout(t *testing.T) {
	f := newFixture(t)
	ctx := f.as(t, member)
	impossible := []roles.Scope{
		{Kind: roles.Project, ID: ""},
		{Kind: roles.Project, ID: "prj-7f3a/../org-1"},
		{Kind: roles.Project, ID: strings.Repeat("a", 65)},
		{Kind: roles.Kind("page"), ID: "pg-1"},
	}
	for _, scope := range impossible {
		for name, ask := range map[string]func(context.Context, roles.Scope) (roles.Answer, error){
			"Read": f.client.Read, "Write": f.client.Write,
		} {
			if got, err := ask(ctx, scope); err != nil || got != (roles.Answer{Role: roles.None}) {
				t.Errorf("%s of %+v = %+v, %v, want none and no error, as for a scope that was never registered", name, scope, got, err)
			}
		}
	}
	if got := len(f.oa.asked()); got != 0 {
		t.Errorf("%d questions, want none: Organization Access refuses such a question as invalid", got)
	}
}
