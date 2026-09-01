package manager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

func TestTokenReviewClientHasIndependentRateLimiter(t *testing.T) {
	t.Parallel()
	requests := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(&authenticationv1.TokenReview{
			TypeMeta: metav1.TypeMeta{APIVersion: "authentication.k8s.io/v1", Kind: "TokenReview"},
			Status: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				User:          authenticationv1.UserInfo{Username: "test-user"},
			},
		}); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)

	bulkLimiter := flowcontrol.NewFakeNeverRateLimiter()
	cfg := &rest.Config{Host: server.URL, QPS: 50, Burst: 100, RateLimiter: bulkLimiter}
	bulkClient, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	authClient, err := newTokenReviewClient(cfg)
	require.NoError(t, err)
	authLimiter := authClient.AuthenticationV1().RESTClient().GetRateLimiter()
	require.NotSame(t, bulkLimiter, authLimiter)
	require.Equal(t, float32(tokenReviewQPS), authLimiter.QPS())
	require.Same(t, bulkLimiter, cfg.RateLimiter)
	require.Equal(t, float32(50), cfg.QPS)
	require.Equal(t, 100, cfg.Burst)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err = bulkClient.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
	require.ErrorContains(t, err, "rate limiter")
	result, err := authClient.AuthenticationV1().TokenReviews().Create(ctx, &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{Token: "test-token"},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	require.True(t, result.Status.Authenticated)
	require.Equal(t, "/apis/authentication.k8s.io/v1/tokenreviews", <-requests)
}
