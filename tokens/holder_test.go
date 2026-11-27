package tokens_test

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/ancyloce/contelys-cloud-libs-service/internal/identitytest"
	"github.com/ancyloce/contelys-cloud-libs-service/tokens"
)

const secret = "s3cret-of-organization-access"

// target is a deployable that serves one operation to calls with a workload token, verified with
// the library, and counts the calls that reach it.
type target struct {
	server *httptest.Server
	calls  atomic.Int64
	// answer, when set, answers a call before its token is verified: it receives the call's
	// Authorization header, may wait, and returns the status to answer, or 0 to verify the call.
	answer atomic.Pointer[func(authorization string) int]
	// seen is the Authorization header of the last call.
	seen atomic.Value
}

func newTarget(t *testing.T, id *identitytest.Identity, slug, operation string) *target {
	t.Helper()
	tg := &target{}
	verifier := tokens.New(tokens.Options{KeySet: id.KeySet(), Client: http.DefaultClient, Deployable: slug})
	operationHandler := verifier.RequireWorkload(operation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if caller, ok := tokens.WorkloadFrom(r.Context()); !ok || caller.Caller != "organization-access" {
			t.Errorf("the target saw the caller %+v, want organization-access", caller)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	tg.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tg.calls.Add(1)
		authorization := r.Header.Get("Authorization")
		tg.seen.Store(authorization)
		if answer := tg.answer.Load(); answer != nil {
			if status := (*answer)(authorization); status != 0 {
				w.WriteHeader(status)
				return
			}
		}
		operationHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(tg.server.Close)
	return tg
}

// answering makes the target answer each call with answer first.
func (tg *target) answering(answer func(authorization string) int) { tg.answer.Store(&answer) }

// bearer is the token of the last call that reached the target.
func (tg *target) bearer() string {
	authorization, _ := tg.seen.Load().(string)
	return strings.TrimPrefix(authorization, "Bearer ")
}

// fileOf writes content to a file, as Ansible renders a secret, and returns its path.
func fileOf(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workload-secret")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// unreachable is the address of an Identity that is gone.
func unreachable(t *testing.T) string {
	t.Helper()
	gone := httptest.NewServer(http.NotFoundHandler())
	gone.Close()
	return gone.URL + "/internal/v1/workload-tokens"
}

// newHolder returns Organization Access's holder, which asks id with the right secret unless
// opts say otherwise.
func newHolder(t *testing.T, id *identitytest.Identity, opts tokens.HolderOptions) *tokens.Holder {
	t.Helper()
	if opts.WorkloadTokens == "" {
		opts.WorkloadTokens = id.WorkloadTokens()
	}
	if opts.Client == nil {
		opts.Client = http.DefaultClient
	}
	if opts.Deployable == "" {
		opts.Deployable = "organization-access"
	}
	if opts.SecretFile == "" {
		opts.SecretFile = fileOf(t, secret+"\n")
	}
	holder, err := tokens.NewHolder(opts)
	if err != nil {
		t.Fatalf("NewHolder: %v", err)
	}
	return holder
}

// identityGranting is a fake Identity that knows Organization Access by its secret and whose
// access files let it call the operation of each target.
func identityGranting(t *testing.T, targets ...string) (*identitytest.Identity, identitytest.Key) {
	t.Helper()
	id := identitytest.New(t)
	key := identitytest.NewKey(t, "workload-2026-11")
	id.Publish(key)
	id.Register("organization-access", secret)
	for _, slug := range targets {
		id.Grant(key, "organization-access", slug, "installMinimumAuthorityVersion")
	}
	return id, key
}

// send makes one call of the operation through client and returns its status.
func send(t *testing.T, ctx context.Context, client *http.Client, tg *target) (int, error) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, tg.server.URL+"/internal/v1/minimum-authority-versions", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}

// sendOK makes one call that the target must accept.
func sendOK(t *testing.T, client *http.Client, tg *target) {
	t.Helper()
	status, err := send(t, context.Background(), client, tg)
	if err != nil || status != http.StatusNoContent {
		t.Fatalf("the call answered %d, %v; want 204", status, err)
	}
}

// to is a client whose calls go to target with holder's tokens.
func to(holder *tokens.Holder, target string) *http.Client {
	return &http.Client{Transport: holder.Transport(target, nil)}
}

// eventually waits up to five seconds for done.
func eventually(t *testing.T, done func() bool, failure string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal(failure)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestACallWithNoUserPresentsAWorkloadTokenThatIdentityIssuedForItsTarget(t *testing.T) {
	id, _ := identityGranting(t, "projects")
	projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
	holder := newHolder(t, id, tokens.HolderOptions{})

	status, err := send(t, context.Background(), to(holder, "projects"), projects)
	if err != nil {
		t.Fatalf("the call failed: %v", err)
	}
	if status != http.StatusNoContent {
		t.Errorf("status = %d, want 204: the target refused the token", status)
	}
}

func TestATokenIsKeptForLaterCallsToItsTargetAndAnotherTargetGetsItsOwn(t *testing.T) {
	id, _ := identityGranting(t, "projects", "sites")
	projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
	sites := newTarget(t, id, "sites", "installMinimumAuthorityVersion")
	holder := newHolder(t, id, tokens.HolderOptions{})
	toProjects, toSites := to(holder, "projects"), to(holder, "sites")

	sendOK(t, toProjects, projects)
	sendOK(t, toProjects, projects)
	sendOK(t, toProjects, projects)
	if got := id.TokenRequests(); got != 1 {
		t.Errorf("three calls to one target requested %d tokens, want 1", got)
	}
	first := projects.bearer()

	sendOK(t, toSites, sites)
	if got := id.TokenRequests(); got != 2 {
		t.Errorf("a call to a second target made %d requests in all, want 2", got)
	}
	sendOK(t, toProjects, projects)
	if projects.bearer() != first {
		t.Error("the call to the first target after the second target's token was not given the first target's token")
	}
}

func TestATokenIsRenewedShortlyBeforeItExpiresWhileTheCallsGoOn(t *testing.T) {
	id, _ := identityGranting(t, "projects")
	projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
	holder := newHolder(t, id, tokens.HolderOptions{})
	client := to(holder, "projects")
	id.SetLifetime(tokens.RenewBefore + 500*time.Millisecond)

	sendOK(t, client, projects)
	sendOK(t, client, projects)
	if got := id.TokenRequests(); got != 1 {
		t.Fatalf("two calls well before the renewal requested %d tokens, want 1", got)
	}
	old := projects.bearer()

	time.Sleep(700 * time.Millisecond)
	id.SetDelay(time.Second)
	start := time.Now()
	sendOK(t, client, projects)
	if waited := time.Since(start); waited > 500*time.Millisecond {
		t.Errorf("the call within %v of the expiry waited %v for the renewal", tokens.RenewBefore, waited)
	}
	if projects.bearer() != old {
		t.Error("the call within the renewal margin did not present the token still held")
	}
	eventually(t, func() bool { return id.TokenRequests() == 2 }, "the token was never renewed")

	id.SetDelay(0)
	eventually(t, func() bool {
		sendOK(t, client, projects)
		return projects.bearer() != old
	}, "the renewed token was never presented")
	if got := id.TokenRequests(); got != 2 {
		t.Errorf("Identity received %d requests, want 2", got)
	}
}

func TestAHeldTokenOutlivesAnOutageOfIdentityUntilShortlyBeforeItExpires(t *testing.T) {
	id, _ := identityGranting(t, "projects")
	projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
	holder := newHolder(t, id, tokens.HolderOptions{})
	client := to(holder, "projects")
	id.SetLifetime(tokens.ExpireBefore + 400*time.Millisecond)

	sendOK(t, client, projects)
	id.SetDown(true)
	sendOK(t, client, projects)
	sendOK(t, client, projects)

	time.Sleep(500 * time.Millisecond)
	before := projects.calls.Load()
	if _, err := send(t, context.Background(), client, projects); !errors.Is(err, tokens.ErrNoWorkloadToken) {
		t.Errorf("a call within %v of the expiry, with Identity down, got %v; want ErrNoWorkloadToken", tokens.ExpireBefore, err)
	}
	if got := projects.calls.Load() - before; got != 0 {
		t.Errorf("the target received %d calls with the expiring token, want none", got)
	}
}

func TestConcurrentCallsToATargetShareOneRequestForItsToken(t *testing.T) {
	id, _ := identityGranting(t, "projects")
	projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
	client := to(newHolder(t, id, tokens.HolderOptions{}), "projects")
	id.SetDelay(150 * time.Millisecond)

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { sendOK(t, client, projects) })
	}
	wg.Wait()
	if got := id.TokenRequests(); got != 1 {
		t.Errorf("ten concurrent calls requested %d tokens, want 1", got)
	}
}

func TestACallerThatGivesUpDoesNotSpoilTheRequestForTheOthers(t *testing.T) {
	id, _ := identityGranting(t, "projects")
	projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
	client := to(newHolder(t, id, tokens.HolderOptions{}), "projects")
	id.SetDelay(200 * time.Millisecond)

	impatient, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, err := send(t, impatient, client, projects); !errors.Is(err, tokens.ErrNoWorkloadToken) {
			t.Errorf("the caller that gave up got %v, want ErrNoWorkloadToken", err)
		}
	})
	eventually(t, func() bool { return id.TokenRequests() == 1 }, "the first caller never requested a token")
	sendOK(t, client, projects)
	wg.Wait()
	if got := id.TokenRequests(); got != 1 {
		t.Errorf("Identity received %d requests, want 1 that both callers shared", got)
	}
}

func TestWithoutATokenTheCallIsNotSentAndFailsApartFromARefusalByTheTarget(t *testing.T) {
	cases := []struct {
		name   string
		target string
		setup  func(id *identitytest.Identity, opts *tokens.HolderOptions)
		cause  string
	}{
		{"the secret is wrong", "projects", func(_ *identitytest.Identity, opts *tokens.HolderOptions) {
			opts.SecretFile = fileOf(t, "not-the-secret\n")
		}, "Identity answered 401 credentials_invalid"},
		{"no access file lists the target", "sites", nil, "Identity answered 403 target_not_permitted"},
		{"Identity is down", "projects", func(id *identitytest.Identity, _ *tokens.HolderOptions) {
			id.SetDown(true)
		}, "Identity answered 503 unavailable"},
		{"Identity cannot be reached", "projects", func(_ *identitytest.Identity, opts *tokens.HolderOptions) {
			opts.WorkloadTokens = unreachable(t)
		}, "Identity cannot be reached"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, _ := identityGranting(t, "projects")
			tg := newTarget(t, id, c.target, "installMinimumAuthorityVersion")
			var opts tokens.HolderOptions
			if c.setup != nil {
				c.setup(id, &opts)
			}

			_, err := send(t, context.Background(), to(newHolder(t, id, opts), c.target), tg)
			if !errors.Is(err, tokens.ErrNoWorkloadToken) {
				t.Fatalf("err = %v, want ErrNoWorkloadToken", err)
			}
			if !strings.Contains(err.Error(), c.cause) {
				t.Errorf("err = %q, want it to name the cause %q", err, c.cause)
			}
			if got := tg.calls.Load(); got != 0 {
				t.Errorf("the target received %d calls, want none", got)
			}
		})
	}

	t.Run("Identity's answer holds no token", func(t *testing.T) {
		id, _ := identityGranting(t, "projects")
		projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
		for _, answer := range []string{`{}`, `{"token":"x"}`, `{"token":"","expiresAt":"2099-01-01T00:00:00Z"}`, `not json`} {
			broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(answer))
			}))
			t.Cleanup(broken.Close)
			holder := newHolder(t, id, tokens.HolderOptions{WorkloadTokens: broken.URL})
			if _, err := send(t, context.Background(), to(holder, "projects"), projects); !errors.Is(err, tokens.ErrNoWorkloadToken) {
				t.Errorf("the answer %s gave err = %v, want ErrNoWorkloadToken", answer, err)
			}
		}
		if got := projects.calls.Load(); got != 0 {
			t.Errorf("the target received %d calls, want none", got)
		}
	})

	t.Run("a refusal by the target is an answer", func(t *testing.T) {
		id, key := identityGranting(t)
		id.Grant(key, "organization-access", "projects", "lookUpProject")
		projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")

		status, err := send(t, context.Background(), to(newHolder(t, id, tokens.HolderOptions{}), "projects"), projects)
		if err != nil || status != http.StatusForbidden {
			t.Errorf("a token without the operation got %d, %v; want the target's 403 and no error", status, err)
		}
	})
}

// closeCounter is a request body that counts its closes.
type closeCounter struct {
	io.Reader
	closed atomic.Int64
}

func (c *closeCounter) Close() error { c.closed.Add(1); return nil }

func TestACallThatIsNotSentHasItsBodyClosed(t *testing.T) {
	id, _ := identityGranting(t, "projects")
	projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
	holder := newHolder(t, id, tokens.HolderOptions{})
	id.SetDown(true)

	body := &closeCounter{Reader: strings.NewReader(`{"kind":"membership"}`)}
	request, err := http.NewRequest(http.MethodPut, projects.server.URL, body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := to(holder, "projects").Do(request); !errors.Is(err, tokens.ErrNoWorkloadToken) {
		t.Fatalf("err = %v, want ErrNoWorkloadToken", err)
	}
	if got := body.closed.Load(); got != 1 {
		t.Errorf("the body was closed %d times, want 1", got)
	}
}

// buffer is a log output that tests read while the holder writes to it.
type buffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// failures returns the count of workload_token.failures for each target.
func failures(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == "workload_token.failures" {
				for _, point := range sum.DataPoints {
					if point.Attributes.Len() != 1 {
						t.Errorf("attributes = %v, want the target alone", point.Attributes.ToSlice())
					}
					target, _ := point.Attributes.Value("target")
					counts[target.AsString()] = point.Value
				}
			}
		}
	}
	return counts
}

func TestEveryCallThatIsNotSentIsASignalAndEveryFailedRequestIsLogged(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	id, _ := identityGranting(t, "projects")
	projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
	sites := newTarget(t, id, "sites", "installMinimumAuthorityVersion")
	var log buffer
	holder := newHolder(t, id, tokens.HolderOptions{Logger: slog.New(slog.NewJSONHandler(&log, nil))})

	id.SetDelay(200 * time.Millisecond)
	impatient, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := send(t, impatient, to(holder, "projects"), projects); !errors.Is(err, tokens.ErrNoWorkloadToken) {
		t.Fatalf("the impatient call got %v, want ErrNoWorkloadToken", err)
	}
	id.SetDelay(0)
	sendOK(t, to(holder, "projects"), projects)
	if got := failures(t, reader); got["projects"] != 1 {
		t.Errorf("failures = %v, want 1 for the call to projects that gave up while it waited", got)
	}

	_, _ = send(t, context.Background(), to(holder, "sites"), sites)
	_, _ = send(t, context.Background(), to(holder, "sites"), sites)
	if got := failures(t, reader); got["sites"] != 2 || got["projects"] != 1 {
		t.Errorf("failures = %v, want 2 for sites and 1 for projects", got)
	}
	line := log.String()
	if !strings.Contains(line, `"level":"WARN"`) || !strings.Contains(line, `"target":"sites"`) ||
		!strings.Contains(line, "target_not_permitted") {
		t.Errorf("the log = %q, want a warning that names the target and Identity's code", line)
	}
	if strings.Contains(line, `"target":"projects"`) {
		t.Errorf("the log = %q, want no line for projects, whose request succeeded", line)
	}
}

func TestATokenThatTheTargetAnswersWith401IsDroppedAndTheCallIsNotSentAgain(t *testing.T) {
	id, _ := identityGranting(t, "projects")
	projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
	client := to(newHolder(t, id, tokens.HolderOptions{}), "projects")

	sendOK(t, client, projects)
	refused := projects.bearer()
	projects.answering(func(string) int { return http.StatusUnauthorized })
	status, err := send(t, context.Background(), client, projects)
	if err != nil || status != http.StatusUnauthorized {
		t.Fatalf("the call answered %d, %v; want the target's 401 handed to the caller", status, err)
	}
	if got := projects.calls.Load(); got != 2 {
		t.Errorf("the target received %d calls, want 2: the transport sent the refused call again", got)
	}

	projects.answering(func(string) int { return 0 })
	sendOK(t, client, projects)
	if got := id.TokenRequests(); got != 2 {
		t.Errorf("Identity received %d requests, want 2: the refused token was kept", got)
	}
	if projects.bearer() == refused {
		t.Error("the call after the 401 presented the refused token")
	}
}

func TestALate401ForAnOldTokenDoesNotDropTheTokenThatReplacedIt(t *testing.T) {
	id, _ := identityGranting(t, "projects")
	projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
	client := to(newHolder(t, id, tokens.HolderOptions{}), "projects")
	id.SetLifetime(tokens.RenewBefore + 300*time.Millisecond)

	sendOK(t, client, projects)
	old := projects.bearer()
	id.SetLifetime(5 * time.Minute)
	release := make(chan struct{})
	var held atomic.Bool
	projects.answering(func(authorization string) int {
		if authorization == "Bearer "+old && held.CompareAndSwap(false, true) {
			<-release
			return http.StatusUnauthorized
		}
		return 0
	})
	var wg sync.WaitGroup
	wg.Go(func() {
		if status, err := send(t, context.Background(), client, projects); status != http.StatusUnauthorized {
			t.Errorf("the held call answered %d, %v; want 401", status, err)
		}
	})
	eventually(t, held.Load, "the call with the old token never reached the target")

	time.Sleep(400 * time.Millisecond)
	sendOK(t, client, projects)
	eventually(t, func() bool {
		sendOK(t, client, projects)
		return projects.bearer() != old
	}, "the old token was never replaced")
	close(release)
	wg.Wait()
	sendOK(t, client, projects)
	if got := id.TokenRequests(); got != 2 {
		t.Errorf("Identity received %d requests, want 2: the 401 for the old token dropped the new one", got)
	}
}

func TestNoLogLineOrErrorCarriesTheSecretOrAToken(t *testing.T) {
	id, _ := identityGranting(t, "projects")
	projects := newTarget(t, id, "projects", "installMinimumAuthorityVersion")
	sites := newTarget(t, id, "sites", "installMinimumAuthorityVersion")
	var log buffer
	logger := slog.New(slog.NewJSONHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	holder := newHolder(t, id, tokens.HolderOptions{Logger: logger})
	toProjects := to(holder, "projects")

	var errs []error
	record := func(_ int, err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	sendOK(t, toProjects, projects)
	presented := []string{projects.bearer()}
	projects.answering(func(string) int { return http.StatusUnauthorized })
	record(send(t, context.Background(), toProjects, projects))
	projects.answering(func(string) int { return 0 })
	sendOK(t, toProjects, projects)
	presented = append(presented, projects.bearer())

	record(send(t, context.Background(), to(holder, "sites"), sites))
	wrong := newHolder(t, id, tokens.HolderOptions{SecretFile: fileOf(t, "not-the-secret"), Logger: logger})
	record(send(t, context.Background(), to(wrong, "projects"), projects))
	id.SetDown(true)
	record(send(t, context.Background(), to(holder, "sites"), sites))
	gone := newHolder(t, id, tokens.HolderOptions{WorkloadTokens: unreachable(t), Logger: logger})
	record(send(t, context.Background(), to(gone, "projects"), projects))

	if len(errs) != 4 {
		t.Fatalf("%d calls failed, want the 4 without a token: %v", len(errs), errs)
	}
	secrets := append([]string{secret, "not-the-secret",
		base64.StdEncoding.EncodeToString([]byte("organization-access:" + secret)),
		base64.StdEncoding.EncodeToString([]byte("organization-access:not-the-secret"))}, presented...)
	texts := []string{log.String()}
	for _, err := range errs {
		texts = append(texts, err.Error())
	}
	for _, text := range texts {
		for _, s := range secrets {
			if strings.Contains(text, s) {
				t.Errorf("%q carries the secret or a token", text)
			}
		}
	}
}

func TestAHolderNeedsItsOptionsAndASecretInItsFile(t *testing.T) {
	complete := tokens.HolderOptions{
		WorkloadTokens: "http://identity.invalid/internal/v1/workload-tokens", Client: http.DefaultClient,
		Deployable: "organization-access", SecretFile: fileOf(t, secret+"\n"),
	}
	if _, err := tokens.NewHolder(complete); err != nil {
		t.Fatalf("NewHolder with every option: %v", err)
	}
	cases := map[string]func(*tokens.HolderOptions){
		"no address of Identity": func(o *tokens.HolderOptions) { o.WorkloadTokens = "" },
		"no client":              func(o *tokens.HolderOptions) { o.Client = nil },
		"no slug":                func(o *tokens.HolderOptions) { o.Deployable = "" },
		"no secret file":         func(o *tokens.HolderOptions) { o.SecretFile = "" },
		"a missing secret file":  func(o *tokens.HolderOptions) { o.SecretFile = filepath.Join(t.TempDir(), "missing") },
		"an empty secret file":   func(o *tokens.HolderOptions) { o.SecretFile = fileOf(t, "\n") },
	}
	for name, change := range cases {
		opts := complete
		change(&opts)
		if _, err := tokens.NewHolder(opts); err == nil {
			t.Errorf("NewHolder with %s succeeded, want an error", name)
		}
	}
}
