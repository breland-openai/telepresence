package auth

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/token/cache"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/kubernetes"

	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
)

// ErrInvalidToken is returned by Authenticate when the Kubernetes API server rejects the bearer token.
var ErrInvalidToken = errors.New("invalid bearer token")

// errTooManyReviews is returned when review admission rejects a TokenReview.
// The token cache stores it for the failure TTL, so a throttled token backs
// off for those ten seconds instead of re-entering admission per call.
var errTooManyReviews = errors.New("too many authentication attempts")

const (
	podNameExtraKey = "authentication.kubernetes.io/pod-name"
	podUIDExtraKey  = "authentication.kubernetes.io/pod-uid"

	successCacheTTL = 2 * time.Minute
	failureCacheTTL = 10 * time.Second
)

// Authenticator validates bearer tokens using cached Kubernetes TokenReviews, or,
// first, a store of tokens minted by the x509 auth listener.
type Authenticator struct {
	token         authenticator.Token
	managerToken  authenticator.Token
	minted        *MintedTokens
	reviewer      *tokenReviewer
	metrics       *Metrics
	reviewMetrics *tokenReviewMetrics
}

// Option configures an Authenticator constructed by NewAuthenticator.
type Option func(*Authenticator)

// WithMintedTokens makes the Authenticator recognize tokens minted by the x509 auth
// listener, ahead of the TokenReview path.
func WithMintedTokens(m *MintedTokens) Option {
	return func(a *Authenticator) {
		a.minted = m
	}
}

// WithMetrics makes the Authenticator record cache hits, first/fallback TokenReview
// calls, invalid tokens, and API-server failures on m. Intended for the external
// listener, whose Authenticator is otherwise unshared with the internal one.
func WithMetrics(m *Metrics) Option {
	return func(a *Authenticator) {
		a.metrics = m
	}
}

// WithReviewAdmission bounds the rate and concurrency of TokenReviews -- the
// expensive, API-server-bound step -- with the external admission constants.
// Cached tokens never enter admission; only a review pays it.
func WithReviewAdmission() Option {
	return func(a *Authenticator) {
		a.reviewer.limiter = rate.NewLimiter(rate.Limit(externalAuthQPS), externalAuthBurst)
		a.reviewer.sem = make(chan struct{}, externalMaxConcurrentAuth)
	}
}

// NewAuthenticator creates an Authenticator that validates tokens with the TokenReview API of ci.
func NewAuthenticator(ci kubernetes.Interface, opts ...Option) *Authenticator {
	reviewer := &tokenReviewer{client: ci}
	a := &Authenticator{
		token:         cache.New(reviewer, true, successCacheTTL, failureCacheTTL),
		managerToken:  cache.New(authenticator.TokenFunc(reviewer.authenticateManager), true, successCacheTTL, failureCacheTTL),
		reviewer:      reviewer,
		metrics:       unregisteredMetrics(),
		reviewMetrics: newTokenReviewMetrics(),
	}
	for _, opt := range opts {
		opt(a)
	}
	// The reviewer only needs metrics wired once opts (which may set a.metrics) have
	// all run.
	reviewer.metrics = a.metrics
	reviewer.reviewMetrics = a.reviewMetrics
	return a
}

// Authenticate validates a bearer token and returns the caller's Principal. A token
// minted by the x509 auth listener is recognized without a TokenReview call.
func (a *Authenticator) Authenticate(ctx context.Context, token string) (*Principal, error) {
	if a.minted != nil {
		if p, ok := a.minted.Lookup(token); ok {
			return p, nil
		}
	}
	return a.authenticateReviewed(ctx, token, a.token)
}

// AuthenticateManagerKubernetes validates only a Kubernetes-issued token for the
// traffic-manager audience. Its independent cache never reuses ordinary client
// authentication, which also accepts Kubernetes API-audience and x509-minted tokens.
func (a *Authenticator) AuthenticateManagerKubernetes(ctx context.Context, token string) (*Principal, error) {
	return a.authenticateReviewed(ctx, token, a.managerToken)
}

func (a *Authenticator) authenticateReviewed(ctx context.Context, token string, tokenCache authenticator.Token) (*Principal, error) {
	resp, ok, err := a.reviewCounted(ctx, token, tokenCache)
	if err != nil {
		return nil, fmt.Errorf("token review: %w", err)
	}
	if !ok {
		a.metrics.InvalidTokens.Inc()
		return nil, ErrInvalidToken
	}
	return principalFromInfo(resp.User), nil
}

// reviewCounted authenticates the token, counting a cache hit when the call completed
// without a new TokenReview. Concurrent calls can mask a hit, so the metric is a
// proportional signal, not an exact count.
func (a *Authenticator) reviewCounted(reviewCtx context.Context, token string, tokenCache authenticator.Token) (*authenticator.Response, bool, error) {
	before := a.reviewer.calls.Load()
	resp, ok, err := tokenCache.AuthenticateToken(reviewCtx, token)
	if a.reviewer.calls.Load() == before {
		a.metrics.CacheHits.Inc()
	}
	return resp, ok, err
}

func principalFromInfo(info user.Info) *Principal {
	p := &Principal{
		Username: info.GetName(),
		UID:      info.GetUID(),
		Groups:   info.GetGroups(),
	}
	extra := info.GetExtra()
	if len(extra) > 0 {
		p.Extra = extra
	}
	if v := extra[podNameExtraKey]; len(v) > 0 {
		p.PodName = v[0]
	}
	if v := extra[podUIDExtraKey]; len(v) > 0 {
		p.PodUID = v[0]
	}
	return p
}

// tokenReviewer implements authenticator.Token by delegating to the Kubernetes TokenReview API.
type tokenReviewer struct {
	client kubernetes.Interface
	// metrics is set by NewAuthenticator once its options have run; it is
	// never nil.
	metrics       *Metrics
	reviewMetrics *tokenReviewMetrics
	// calls counts AuthenticateToken invocations -- i.e. cache misses.
	calls atomic.Uint64
	// limiter and sem, when set by WithReviewAdmission, bound the rate and
	// concurrency of reviews.
	limiter *rate.Limiter
	sem     chan struct{}
}

func (t *tokenReviewer) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	resp, ok, err := t.authenticateManager(ctx, token)
	if err != nil || ok {
		return resp, ok, err
	}
	// API-audience client credentials share one cached decision with the manager-audience attempt.
	auds, _ := authenticator.AudiencesFrom(ctx)
	return t.review(ctx, token, auds, "api")
}

func (t *tokenReviewer) authenticateManager(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	t.calls.Add(1)
	return t.review(ctx, token, []string{agentconfig.ManagerTokenAudience}, "manager")
}

func (t *tokenReviewer) review(ctx context.Context, token string, audiences []string, audienceLabel string) (*authenticator.Response, bool, error) {
	if t.limiter != nil {
		if !t.limiter.Allow() {
			t.metrics.RateLimited.Inc()
			return nil, false, errTooManyReviews
		}
		select {
		case t.sem <- struct{}{}:
			defer func() { <-t.sem }()
		default:
			t.metrics.RateLimited.Inc()
			return nil, false, errTooManyReviews
		}
	}
	review := &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{Token: token, Audiences: audiences},
	}
	start := time.Now()
	result, err := t.client.AuthenticationV1().TokenReviews().Create(ctx, review, metav1.CreateOptions{})
	if audienceLabel == "manager" {
		t.metrics.FirstReviews.Inc()
	} else {
		t.metrics.FallbackReviews.Inc()
	}
	if err != nil {
		t.metrics.APIFailures.Inc()
	}
	outcome := "error"
	if err == nil {
		outcome = "rejected"
		if result.Status.Authenticated {
			outcome = "authenticated"
		}
	}
	t.reviewMetrics.observe(audienceLabel, outcome, time.Since(start))
	if err != nil {
		return nil, false, err
	}
	status := result.Status
	if !status.Authenticated {
		return nil, false, nil
	}
	extra := make(map[string][]string, len(status.User.Extra))
	for k, v := range status.User.Extra {
		extra[k] = v
	}
	resp := &authenticator.Response{
		User: &user.DefaultInfo{
			Name:   status.User.Username,
			UID:    status.User.UID,
			Groups: status.User.Groups,
			Extra:  extra,
		},
		Audiences: status.Audiences,
	}
	return resp, true, nil
}
