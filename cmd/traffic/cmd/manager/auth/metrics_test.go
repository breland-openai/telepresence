package auth

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authnv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestTokenReviewMetricsExcludeCacheHits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ci := fake.NewClientset()
		ci.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
			time.Sleep(250 * time.Millisecond)
			tr := action.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview)
			switch {
			case tr.Spec.Token == "secret-error":
				return true, nil, errors.New("connection refused")
			case tr.Spec.Token == "secret-manager", tr.Spec.Token == "secret-api" && len(tr.Spec.Audiences) == 0:
				tr.Status = *cacheTestStatus()
			}
			return true, tr, nil
		})
		a := NewAuthenticator(ci)
		registry := prometheus.NewRegistry()
		a.RegisterMetrics(registry)

		for range 2 {
			for _, token := range []string{"secret-manager", "secret-api"} {
				_, err := a.Authenticate(context.Background(), token)
				require.NoError(t, err)
			}
			_, err := a.Authenticate(context.Background(), "secret-invalid")
			require.ErrorIs(t, err, ErrInvalidToken)
			_, err = a.Authenticate(context.Background(), "secret-error")
			require.Error(t, err)
		}

		wantCounts := map[[2]string]float64{
			{"manager", "authenticated"}: 1,
			{"manager", "rejected"}:      2,
			{"manager", "error"}:         1,
			{"api", "authenticated"}:     1,
			{"api", "rejected"}:          1,
		}
		families, err := registry.Gather()
		require.NoError(t, err)
		require.Len(t, families, 2)
		for _, family := range families {
			for _, metric := range family.Metric {
				require.Len(t, metric.Label, 2)
				var key [2]string
				for _, label := range metric.Label {
					switch label.GetName() {
					case "audience":
						assert.Contains(t, []string{"manager", "api"}, label.GetValue())
						key[0] = label.GetValue()
					case "outcome":
						assert.Contains(t, []string{"authenticated", "rejected", "error"}, label.GetValue())
						key[1] = label.GetValue()
					default:
						t.Errorf("unexpected metric label %q", label.GetName())
					}
				}
				if counter := metric.GetCounter(); counter != nil {
					assert.Equal(t, wantCounts[key], counter.GetValue())
					delete(wantCounts, key)
				}
				if histogram := metric.GetHistogram(); histogram != nil {
					assert.InDelta(t, float64(histogram.GetSampleCount())*0.25, histogram.GetSampleSum(), 0.001)
				}
			}
		}
		assert.Empty(t, wantCounts)
	})
}

func TestTokenReviewMetricsHavePerAuthenticatorRegistration(t *testing.T) {
	for range 2 {
		a := NewAuthenticator(fake.NewClientset())
		a.RegisterMetrics(prometheus.NewRegistry())
	}
}
