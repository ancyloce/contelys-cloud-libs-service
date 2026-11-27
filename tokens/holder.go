package tokens

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ErrNoWorkloadToken is a call that was not sent, because no workload token for its target could
// be had from Identity. A refusal by the target is an answer, never this error.
var ErrNoWorkloadToken = errors.New("no workload token")

const (
	// renewBefore is how long before its expiry a token is renewed. The renewal runs in the
	// background, and the token is presented until it succeeds, so that a short outage of
	// Identity does not stop calls that hold a token (ADR 0040).
	renewBefore = 30 * time.Second
	// expireBefore is how long before its expiry a token is no longer presented. It covers the
	// clock skew between the deployable and Identity and the time that a call takes to reach its
	// target. A call with no token that it may present waits for a new one.
	expireBefore = 5 * time.Second
	// answerLimit is the most bytes of an answer of Identity's that the holder reads.
	answerLimit = 16 << 10
)

// HolderOptions say where Identity issues workload tokens and who asks for them.
type HolderOptions struct {
	// WorkloadTokens is the address of Identity's issueWorkloadToken, POST
	// /internal/v1/workload-tokens of its contract.
	WorkloadTokens string
	// Client calls Identity, with the transport that the internal listener needs.
	Client *http.Client
	// Deployable is this deployable's slug, which it presents to Identity with its secret.
	Deployable string
	// SecretFile is the file that holds this deployable's secret (ADR 0020). It is read once, by
	// NewHolder, because a rotation restarts the deployable.
	SecretFile string
	// Logger, when set, receives each request to Identity that got no token, with its target and
	// its cause. No line carries the secret or a token.
	Logger *slog.Logger
}

// Holder requests the workload tokens of calls with no user from Identity, keeps each until
// shortly before it expires, and presents it (ADR 0040). It holds one token for each target.
type Holder struct {
	opts     HolderOptions
	secret   string
	logger   *slog.Logger
	failures metric.Int64Counter

	// mu guards the tokens held and the requests in flight, both by target.
	mu       sync.Mutex
	held     map[string]issued
	inFlight map[string]*flight[issued]
}

// issued is a token and its expiry, as Identity answered them.
type issued struct {
	raw       string
	expiresAt time.Time
}

// NewHolder reads the deployable's secret from its file and returns a holder. It requests no
// token until the first call. Each call that is not sent for lack of a token adds one to the
// counter workload_token.failures of the process's meter provider, under the attribute target.
func NewHolder(opts HolderOptions) (*Holder, error) {
	if opts.WorkloadTokens == "" || opts.Client == nil || opts.Deployable == "" || opts.SecretFile == "" {
		return nil, errors.New("tokens: a holder needs the address of Identity's workload tokens, a client, a slug and a secret file")
	}
	data, err := os.ReadFile(opts.SecretFile)
	if err != nil {
		return nil, fmt.Errorf("tokens: the deployable's secret: %w", err)
	}
	secret := strings.TrimRight(string(data), "\r\n")
	if secret == "" {
		return nil, fmt.Errorf("tokens: the deployable's secret: %s is empty", opts.SecretFile)
	}
	failures, err := otel.Meter("github.com/ancyloce/contelys-cloud-libs-service/tokens").Int64Counter(
		"workload_token.failures",
		metric.WithUnit("{call}"),
		metric.WithDescription("The calls with no user that were not sent, because no workload token for their target could be had."))
	if err != nil {
		return nil, fmt.Errorf("tokens: %w", err)
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Holder{
		opts:     opts,
		secret:   secret,
		logger:   logger,
		failures: failures,
		held:     map[string]issued{},
		inFlight: map[string]*flight[issued]{},
	}, nil
}

// Transport makes every call through base, or http.DefaultTransport when base is nil, carry a
// workload token for target, a deployable's slug, as its bearer token. A call for which no token
// can be had is not sent, and fails with ErrNoWorkloadToken, which http.Client wraps in a
// *url.Error. When target answers 401, the transport drops the token that it presented, so that
// the caller's next call gets a new one; it never sends a call again itself.
func (h *Holder) Transport(target string, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &presenting{holder: h, target: target, base: base}
}

// presenting is the transport of one target.
type presenting struct {
	holder *Holder
	target string
	base   http.RoundTripper
}

func (p *presenting) RoundTrip(r *http.Request) (*http.Response, error) {
	token, err := p.holder.token(r.Context(), p.target)
	if err != nil {
		p.holder.failures.Add(r.Context(), 1, metric.WithAttributes(attribute.String("target", p.target)))
		if r.Body != nil {
			_ = r.Body.Close()
		}
		return nil, err
	}
	call := r.Clone(r.Context())
	call.Header.Set("Authorization", "Bearer "+token)
	response, err := p.base.RoundTrip(call)
	if err == nil && response.StatusCode == http.StatusUnauthorized {
		p.holder.drop(p.target, token)
	}
	return response, err
}

// token returns a token for target that it may present. A token within renewBefore of its expiry
// starts a renewal and is still returned until expireBefore; without such a token, the call waits
// for a new one. One request for a target's token runs at a time, and every call waits for it no
// longer than its own context allows.
func (h *Holder) token(ctx context.Context, target string) (string, error) {
	now := time.Now()
	h.mu.Lock()
	kept, ok := h.held[target]
	if ok && now.Before(kept.expiresAt.Add(-renewBefore)) {
		h.mu.Unlock()
		return kept.raw, nil
	}
	call := h.inFlight[target]
	if call == nil {
		call = newFlight[issued]()
		h.inFlight[target] = call
		go h.refresh(context.WithoutCancel(ctx), target, call)
	}
	h.mu.Unlock()
	if ok && now.Before(kept.expiresAt.Add(-expireBefore)) {
		return kept.raw, nil
	}

	fresh, err := call.wait(ctx)
	if err != nil {
		return "", fmt.Errorf("tokens: %w for %s: %v", ErrNoWorkloadToken, target, err)
	}
	return fresh.raw, nil
}

// refresh requests target's token for call, within requestTimeout, and keeps it. A failure is
// logged, and leaves the token held, if any, in place.
func (h *Holder) refresh(ctx context.Context, target string, call *flight[issued]) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	token, err := h.issue(ctx, target)
	if err != nil {
		h.logger.WarnContext(ctx, "no workload token could be had", "target", target, "error", err)
	}

	h.mu.Lock()
	if err == nil {
		h.held[target] = token
	}
	delete(h.inFlight, target)
	h.mu.Unlock()
	call.land(token, err)
}

// issue requests a workload token for target from Identity, with HTTP Basic made of the
// deployable's slug and its secret. Its errors name Identity's status and code, never the secret
// or a token.
func (h *Holder) issue(ctx context.Context, target string) (issued, error) {
	body, err := json.Marshal(map[string]string{"target": target})
	if err != nil {
		return issued{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.opts.WorkloadTokens, bytes.NewReader(body))
	if err != nil {
		return issued{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth(h.opts.Deployable, h.secret)
	response, err := h.opts.Client.Do(request)
	if err != nil {
		return issued{}, fmt.Errorf("Identity cannot be reached: %w", err)
	}
	defer response.Body.Close()
	answer := io.LimitReader(response.Body, answerLimit)
	if response.StatusCode != http.StatusOK {
		var refusal struct {
			Code string `json:"code"`
		}
		_ = json.NewDecoder(answer).Decode(&refusal)
		if refusal.Code == "" {
			refusal.Code = http.StatusText(response.StatusCode)
		}
		return issued{}, fmt.Errorf("Identity answered %d %s", response.StatusCode, refusal.Code)
	}
	var token struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expiresAt"`
	}
	if err := json.NewDecoder(answer).Decode(&token); err != nil {
		return issued{}, errors.New("Identity's answer is not a workload token")
	}
	if token.Token == "" || !time.Now().Before(token.ExpiresAt) {
		return issued{}, errors.New("Identity's answer holds no token that is still valid")
	}
	return issued{raw: token.Token, expiresAt: token.ExpiresAt}, nil
}

// drop forgets target's token if it is still token, so that a late refusal of an old token
// leaves the one that replaced it.
func (h *Holder) drop(target, token string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.held[target].raw == token {
		delete(h.held, target)
	}
}
