package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

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

const (
	podNameExtraKey = "authentication.kubernetes.io/pod-name"
	podUIDExtraKey  = "authentication.kubernetes.io/pod-uid"

	successCacheTTL = 2 * time.Minute
	failureCacheTTL = 10 * time.Second
)

// Authenticator validates bearer tokens using cached Kubernetes TokenReviews, or,
// first, a store of tokens minted by the x509 auth listener.
type Authenticator struct {
	token     authenticator.Token
	minted    *MintedTokens
	metrics   *tokenReviewMetrics
	delegated selfSubjectReviewer
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

// NewAuthenticator creates an Authenticator that validates tokens with the TokenReview API of ci.
func NewAuthenticator(ci kubernetes.Interface, opts ...Option) *Authenticator {
	metrics := newTokenReviewMetrics()
	a := &Authenticator{
		metrics: metrics,
	}
	for _, opt := range opts {
		opt(a)
	}
	a.token = cache.New(&tokenReviewer{client: ci, metrics: metrics, delegated: a.delegated}, true, successCacheTTL, failureCacheTTL)
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
	resp, ok, err := a.token.AuthenticateToken(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("token review: %w", err)
	}
	if !ok {
		return nil, ErrInvalidToken
	}
	return principalFromInfo(resp.User), nil
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
	client    kubernetes.Interface
	metrics   *tokenReviewMetrics
	delegated selfSubjectReviewer
}

func (t *tokenReviewer) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	resp, ok, err := t.review(ctx, token, []string{agentconfig.ManagerTokenAudience}, "manager")
	if err != nil || ok {
		return resp, ok, err
	}
	// API-audience client credentials share one cached decision with the manager-audience attempt.
	auds, _ := authenticator.AudiencesFrom(ctx)
	resp, ok, err = t.review(ctx, token, auds, "api")
	if err != nil || ok || t.delegated == nil {
		return resp, ok, err
	}
	start := time.Now()
	resp, ok, err = t.delegated.review(ctx, token)
	outcome := "rejected"
	if err != nil {
		outcome = "error"
	} else if ok {
		outcome = "authenticated"
	}
	t.metrics.observe("delegated", outcome, time.Since(start))
	return resp, ok, err
}

func (t *tokenReviewer) review(ctx context.Context, token string, audiences []string, audienceLabel string) (*authenticator.Response, bool, error) {
	review := &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{Token: token, Audiences: audiences},
	}
	start := time.Now()
	result, err := t.client.AuthenticationV1().TokenReviews().Create(ctx, review, metav1.CreateOptions{})
	outcome := "error"
	if err == nil {
		outcome = "rejected"
		if result.Status.Authenticated {
			outcome = "authenticated"
		}
	}
	t.metrics.observe(audienceLabel, outcome, time.Since(start))
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
