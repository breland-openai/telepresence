package auth

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authnv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func cacheTestStatus() *authnv1.TokenReviewStatus {
	return &authnv1.TokenReviewStatus{
		Authenticated: true,
		User:          authnv1.UserInfo{Username: "user", UID: "uid"},
	}
}

func TestAuthenticateCachesCombinedAudienceDecision(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		var revoked atomic.Bool
		ci := fake.NewClientset()
		k8sapi.InstallFakeTokenReviews(ci, func(_ string, audiences []string) *authnv1.TokenReviewStatus {
			calls.Add(1)
			if len(audiences) == 0 && !revoked.Load() {
				return cacheTestStatus()
			}
			return &authnv1.TokenReviewStatus{Error: "credential rejected for this audience"}
		})
		a := NewAuthenticator(ci)
		p, err := a.Authenticate(context.Background(), "api-token")
		require.NoError(t, err)
		require.Equal(t, "user", p.Username)
		require.Equal(t, int32(2), calls.Load())

		revoked.Store(true)
		time.Sleep(failureCacheTTL + time.Second)
		_, err = a.Authenticate(context.Background(), "api-token")
		require.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load(), "the final successful decision outlives an audience rejection")

		time.Sleep(successCacheTTL)
		_, err = a.Authenticate(context.Background(), "api-token")
		require.ErrorIs(t, err, ErrInvalidToken)
		assert.Equal(t, int32(4), calls.Load(), "an expired success must be verified again")
	})
}

func TestAuthenticateFailureCacheExpires(t *testing.T) {
	for _, transportFailure := range []bool{false, true} {
		name := "invalid"
		if transportFailure {
			name = "transport"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int32
				var recovered atomic.Bool
				failure := errors.New("connection refused")
				ci := fake.NewClientset()
				ci.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
					calls.Add(1)
					tr := action.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview)
					if recovered.Load() {
						tr.Status = *cacheTestStatus()
					} else if transportFailure {
						return true, nil, failure
					}
					return true, tr, nil
				})
				a := NewAuthenticator(ci)
				_, err := a.Authenticate(context.Background(), "token")
				wantCalls := int32(2)
				wantErr := ErrInvalidToken
				if transportFailure {
					wantCalls = 1
					wantErr = failure
				}
				require.ErrorIs(t, err, wantErr)
				require.Equal(t, wantCalls, calls.Load())

				recovered.Store(true)
				_, err = a.Authenticate(context.Background(), "token")
				require.ErrorIs(t, err, wantErr)
				require.Equal(t, wantCalls, calls.Load())

				time.Sleep(failureCacheTTL + time.Second)
				_, err = a.Authenticate(context.Background(), "token")
				require.NoError(t, err)
				assert.Equal(t, wantCalls+1, calls.Load())
			})
		})
	}
}

func TestAuthenticateCoalescesAudienceFallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		release := make(chan struct{})
		ci := fake.NewClientset()
		k8sapi.InstallFakeTokenReviews(ci, func(_ string, audiences []string) *authnv1.TokenReviewStatus {
			calls.Add(1)
			if len(audiences) > 0 {
				<-release
				return &authnv1.TokenReviewStatus{}
			}
			return cacheTestStatus()
		})
		a := NewAuthenticator(ci)
		const callers = 24
		results := make(chan error, callers)
		for range callers {
			go func() {
				_, err := a.Authenticate(context.Background(), "api-token")
				results <- err
			}()
		}
		synctest.Wait()
		require.Equal(t, int32(1), calls.Load())
		close(release)
		for range callers {
			require.NoError(t, <-results)
		}
		assert.Equal(t, int32(2), calls.Load())
	})
}

func TestAuthenticateCanceledWaiterDoesNotPoisonCache(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		release := make(chan struct{})
		ci := fake.NewClientset()
		k8sapi.InstallFakeTokenReviews(ci, func(_ string, audiences []string) *authnv1.TokenReviewStatus {
			calls.Add(1)
			if len(audiences) > 0 {
				<-release
				return &authnv1.TokenReviewStatus{}
			}
			return cacheTestStatus()
		})
		a := NewAuthenticator(ci)
		ctx, cancel := context.WithCancel(context.Background())
		first := make(chan error, 1)
		go func() {
			_, err := a.Authenticate(ctx, "api-token")
			first <- err
		}()
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-first, context.Canceled)
		require.Equal(t, int32(1), calls.Load())

		second := make(chan error, 1)
		go func() {
			_, err := a.Authenticate(context.Background(), "api-token")
			second <- err
		}()
		synctest.Wait()
		close(release)
		require.NoError(t, <-second)
		_, err := a.Authenticate(context.Background(), "api-token")
		require.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})
}

func TestAuthenticateFallbackTransportFailureDoesNotAuthenticate(t *testing.T) {
	ci := fake.NewClientset()
	failure := errors.New("connection refused")
	var calls atomic.Int32
	ci.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		calls.Add(1)
		tr := action.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview)
		if len(tr.Spec.Audiences) > 0 {
			assert.Equal(t, []string{agentconfig.ManagerTokenAudience}, tr.Spec.Audiences)
			tr.Status.Error = "invalid audience"
			return true, tr, nil
		}
		return true, nil, failure
	})
	a := NewAuthenticator(ci)
	p, err := a.Authenticate(context.Background(), "api-token")
	require.ErrorIs(t, err, failure)
	require.Nil(t, p)
	assert.False(t, errors.Is(err, ErrInvalidToken))
	assert.Equal(t, int32(2), calls.Load())
}
