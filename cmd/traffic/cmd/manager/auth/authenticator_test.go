package auth_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authnv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/auth"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

// unregisteredMetrics builds a *auth.Metrics whose counters are plain, unregistered
// prometheus.Counters -- safe to construct repeatedly across tests, unlike
// auth.NewMetrics which registers with the global default registry.
func unregisteredMetrics() *auth.Metrics {
	c := func() prometheus.Counter { return prometheus.NewCounter(prometheus.CounterOpts{Name: "c"}) }
	return &auth.Metrics{
		CacheHits:       c(),
		FirstReviews:    c(),
		FallbackReviews: c(),
		RateLimited:     c(),
		InvalidTokens:   c(),
		APIFailures:     c(),
	}
}

func authenticatedStatus(name, uid string, groups ...string) *authnv1.TokenReviewStatus {
	return &authnv1.TokenReviewStatus{
		Authenticated: true,
		User: authnv1.UserInfo{
			Username: name,
			UID:      uid,
			Groups:   groups,
		},
	}
}

func TestAuthenticate_ValidBoundToken(t *testing.T) {
	ci := fake.NewClientset()
	k8sapi.InstallFakeTokenReviews(ci, func(token string, audiences []string) *authnv1.TokenReviewStatus {
		if token != "good-token" {
			return &authnv1.TokenReviewStatus{Authenticated: false}
		}
		status := authenticatedStatus("system:serviceaccount:demo:my-sa", "1234", "system:serviceaccounts", "system:serviceaccounts:demo")
		status.User.Extra = map[string]authnv1.ExtraValue{
			"authentication.kubernetes.io/pod-name": {"my-pod"},
			"authentication.kubernetes.io/pod-uid":  {"pod-uid-1"},
		}
		return status
	})

	a := auth.NewAuthenticator(ci)
	p, err := a.Authenticate(context.Background(), "good-token")
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Equal(t, "system:serviceaccount:demo:my-sa", p.Username)
	assert.Equal(t, "1234", p.UID)
	assert.Equal(t, []string{"system:serviceaccounts", "system:serviceaccounts:demo"}, p.Groups)
	assert.Equal(t, "my-pod", p.PodName)
	assert.Equal(t, "pod-uid-1", p.PodUID)
}

func TestAuthenticate_AudienceFallback(t *testing.T) {
	var firstAudiences []string
	var calls int

	ci := fake.NewClientset()
	k8sapi.InstallFakeTokenReviews(ci, func(token string, audiences []string) *authnv1.TokenReviewStatus {
		if calls == 0 {
			firstAudiences = audiences
		}
		calls++
		switch {
		case token == "manager-token" && len(audiences) == 1 && audiences[0] == agentconfig.ManagerTokenAudience:
			return authenticatedStatus("manager-sa", "u1")
		case token == "user-token" && len(audiences) == 0:
			return authenticatedStatus("some-user", "u2")
		default:
			return &authnv1.TokenReviewStatus{Authenticated: false}
		}
	})

	a := auth.NewAuthenticator(ci)

	p, err := a.Authenticate(context.Background(), "manager-token")
	require.NoError(t, err)
	assert.Equal(t, "manager-sa", p.Username)
	assert.Equal(t, []string{agentconfig.ManagerTokenAudience}, firstAudiences)

	p, err = a.Authenticate(context.Background(), "user-token")
	require.NoError(t, err)
	assert.Equal(t, "some-user", p.Username)
}

func TestAuthenticateManagerKubernetesSeparatesOrdinaryCredentialCache(t *testing.T) {
	for _, observerFirst := range []bool{false, true} {
		name := "ordinary first"
		if observerFirst {
			name = "observer first"
		}
		t.Run(name, func(t *testing.T) {
			ci := fake.NewClientset()
			var managerReviews, apiReviews int
			k8sapi.InstallFakeTokenReviews(ci, func(token string, audiences []string) *authnv1.TokenReviewStatus {
				if len(audiences) == 1 && audiences[0] == agentconfig.ManagerTokenAudience {
					managerReviews++
				} else {
					apiReviews++
					require.Equal(t, []string{"https://kubernetes.example"}, audiences)
					if token == "api-only" {
						s := authenticatedStatus("system:serviceaccount:routing:observer", "observer-uid")
						s.Audiences = audiences
						return s
					}
				}
				return &authnv1.TokenReviewStatus{Authenticated: false}
			})
			a := auth.NewAuthenticator(ci)
			ctx := authenticator.WithAudiences(context.Background(), authenticator.Audiences{"https://kubernetes.example"})
			ordinary := func() {
				p, err := a.Authenticate(ctx, "api-only")
				require.NoError(t, err)
				require.Equal(t, "observer-uid", p.UID)
			}
			observer := func() {
				p, err := a.AuthenticateManagerKubernetes(ctx, "api-only")
				require.ErrorIs(t, err, auth.ErrInvalidToken)
				require.Nil(t, p)
			}
			if observerFirst {
				observer()
				ordinary()
			} else {
				ordinary()
				observer()
			}
			for range 3 {
				ordinary()
				observer()
			}
			require.Equal(t, 2, managerReviews, "each independent cache performs its first Kubernetes review once")
			require.Equal(t, 1, apiReviews, "only the ordinary credential path may use the API audience")
		})
	}
}

func TestAuthenticateManagerKubernetesRequiresKubernetesAndCachesManagerReview(t *testing.T) {
	ci := fake.NewClientset()
	var reviews []string
	k8sapi.InstallFakeTokenReviews(ci, func(token string, audiences []string) *authnv1.TokenReviewStatus {
		reviews = append(reviews, token)
		require.Equal(t, []string{agentconfig.ManagerTokenAudience}, audiences)
		if token == "manager-only" {
			s := authenticatedStatus("system:serviceaccount:routing:observer", "observer-uid")
			s.Audiences = audiences
			return s
		}
		return &authnv1.TokenReviewStatus{Authenticated: false}
	})
	minted := auth.NewMintedTokens()
	minted.InvalidateAll(1)
	certToken, _, err := minted.Mint(&auth.Principal{Username: "x509-user", UID: "cert-uid"}, "certificate-fingerprint", time.Now().Add(time.Hour), 1)
	require.NoError(t, err)
	a := auth.NewAuthenticator(ci, auth.WithMintedTokens(minted))
	for range 3 {
		p, authenticateErr := a.Authenticate(context.Background(), certToken)
		require.NoError(t, authenticateErr)
		require.Equal(t, "cert-uid", p.UID)
		p, authenticateErr = a.AuthenticateManagerKubernetes(context.Background(), certToken)
		require.ErrorIs(t, authenticateErr, auth.ErrInvalidToken)
		require.Nil(t, p)
		p, authenticateErr = a.AuthenticateManagerKubernetes(context.Background(), "manager-only")
		require.NoError(t, authenticateErr)
		require.Equal(t, "observer-uid", p.UID)
	}
	require.Equal(t, []string{certToken, "manager-only"}, reviews)
}

func TestAuthenticateManagerKubernetesInfrastructureErrorCannotFallback(t *testing.T) {
	ci := fake.NewClientset()
	failure := errors.New("Kubernetes manager-audience review unavailable")
	var reviews int
	ci.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		reviews++
		review := action.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview)
		require.Equal(t, []string{agentconfig.ManagerTokenAudience}, review.Spec.Audiences)
		return true, nil, failure
	})
	a := auth.NewAuthenticator(ci)
	for range 2 {
		p, err := a.AuthenticateManagerKubernetes(context.Background(), "outage")
		require.ErrorIs(t, err, failure)
		require.NotErrorIs(t, err, auth.ErrInvalidToken)
		require.Nil(t, p)
	}
	require.Equal(t, 1, reviews)
}

func TestAuthenticate_InvalidToken(t *testing.T) {
	ci := fake.NewClientset()
	k8sapi.InstallFakeTokenReviews(ci, nil)

	a := auth.NewAuthenticator(ci)
	p, err := a.Authenticate(context.Background(), "bad-token")
	assert.Nil(t, p)
	assert.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestAuthenticate_InfrastructureError(t *testing.T) {
	ci := fake.NewClientset()
	k8sapi.InstallFakeTokenReviews(ci, nil)
	failure := errors.New("connection refused")
	ci.PrependReactor("create", "tokenreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, failure
	})

	a := auth.NewAuthenticator(ci)
	p, err := a.Authenticate(context.Background(), "any-token")
	assert.Nil(t, p)
	assert.False(t, errors.Is(err, auth.ErrInvalidToken))
	require.Error(t, err)
}

// TestAuthenticate_Metrics verifies the external listener's authentication metrics: a
// first (manager-audience) TokenReview for a manager-audience token, a fallback
// (no-audience) TokenReview for a token that only verifies without an audience
// constraint, a cache hit for a repeated lookup, and an invalid-token count for a token
// that fails both reviews.
func TestAuthenticate_Metrics(t *testing.T) {
	ci := fake.NewClientset()
	k8sapi.InstallFakeTokenReviews(ci, func(token string, audiences []string) *authnv1.TokenReviewStatus {
		switch {
		case token == "manager-token" && len(audiences) == 1 && audiences[0] == agentconfig.ManagerTokenAudience:
			return authenticatedStatus("manager-sa", "u1")
		case token == "user-token" && len(audiences) == 0:
			return authenticatedStatus("some-user", "u2")
		default:
			return &authnv1.TokenReviewStatus{Authenticated: false}
		}
	})

	m := unregisteredMetrics()
	a := auth.NewAuthenticator(ci, auth.WithMetrics(m))

	_, err := a.Authenticate(context.Background(), "manager-token")
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(m.FirstReviews))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.FallbackReviews))
	assert.Equal(t, float64(0), testutil.ToFloat64(m.CacheHits))

	// A repeat lookup of the same token is served from cache.
	_, err = a.Authenticate(context.Background(), "manager-token")
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(m.FirstReviews))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.CacheHits))

	// A user token fails the manager-audience review and succeeds on fallback.
	_, err = a.Authenticate(context.Background(), "user-token")
	require.NoError(t, err)
	assert.Equal(t, float64(2), testutil.ToFloat64(m.FirstReviews))
	assert.Equal(t, float64(1), testutil.ToFloat64(m.FallbackReviews))

	// A token that fails both reviews counts as invalid.
	_, err = a.Authenticate(context.Background(), "bad-token")
	assert.ErrorIs(t, err, auth.ErrInvalidToken)
	assert.Equal(t, float64(1), testutil.ToFloat64(m.InvalidTokens))
}

// TestAuthenticate_Metrics_APIFailure verifies that a TokenReview call failing for
// infrastructure reasons is counted as an API failure.
func TestAuthenticate_Metrics_APIFailure(t *testing.T) {
	ci := fake.NewClientset()
	k8sapi.InstallFakeTokenReviews(ci, nil)
	ci.PrependReactor("create", "tokenreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection refused")
	})

	m := unregisteredMetrics()
	a := auth.NewAuthenticator(ci, auth.WithMetrics(m))

	_, err := a.Authenticate(context.Background(), "any-token")
	require.Error(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(m.APIFailures))
}

func TestAuthenticate_Caching(t *testing.T) {
	var calls atomic.Int32

	ci := fake.NewClientset()
	k8sapi.InstallFakeTokenReviews(ci, func(token string, audiences []string) *authnv1.TokenReviewStatus {
		calls.Add(1)
		if token == "good" && len(audiences) == 1 && audiences[0] == agentconfig.ManagerTokenAudience {
			return authenticatedStatus("u", "1")
		}
		return &authnv1.TokenReviewStatus{Authenticated: false}
	})

	a := auth.NewAuthenticator(ci)

	_, err := a.Authenticate(context.Background(), "good")
	require.NoError(t, err)
	afterFirst := calls.Load()
	assert.Equal(t, int32(1), afterFirst)

	_, err = a.Authenticate(context.Background(), "good")
	require.NoError(t, err)
	assert.Equal(t, afterFirst, calls.Load())

	_, err = a.Authenticate(context.Background(), "bad")
	assert.ErrorIs(t, err, auth.ErrInvalidToken)
	afterBad := calls.Load()
	assert.Greater(t, afterBad, afterFirst)

	_, err = a.Authenticate(context.Background(), "bad")
	assert.ErrorIs(t, err, auth.ErrInvalidToken)
	assert.Equal(t, afterBad, calls.Load())
}

// TestAuthenticate_AudiencePathMemoized: after a user token authenticates via
// the no-audience fallback, a repeat Authenticate never re-tries the doomed
// manager-audience review, since a token's audiences are a property of the
// token string.
func TestAuthenticate_AudiencePathMemoized(t *testing.T) {
	var mu sync.Mutex
	var audienceScoped, total int
	ci := fake.NewClientset()
	k8sapi.InstallFakeTokenReviews(ci, func(token string, audiences []string) *authnv1.TokenReviewStatus {
		mu.Lock()
		total++
		if len(audiences) > 0 {
			audienceScoped++
		}
		mu.Unlock()
		if len(audiences) == 0 && token == "user-token" {
			return authenticatedStatus("some-user", "u2")
		}
		return &authnv1.TokenReviewStatus{Authenticated: false}
	})

	a := auth.NewAuthenticator(ci)
	for range 3 {
		p, err := a.Authenticate(context.Background(), "user-token")
		require.NoError(t, err)
		assert.Equal(t, "some-user", p.Username)
	}
	assert.Equal(t, 1, audienceScoped, "the manager-audience review must run only once per token")
	assert.Equal(t, 2, total, "later calls are served by the token cache")
}
