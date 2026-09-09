package auth

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type tokenReviewMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func newTokenReviewMetrics() *tokenReviewMetrics {
	labels := []string{"audience", "outcome"}
	return &tokenReviewMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "telepresence_token_review_requests_total",
			Help: "Kubernetes TokenReview or delegated SelfSubjectReview API requests, excluding cached authentication results.",
		}, labels),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "telepresence_token_review_duration_seconds",
			Help:    "Kubernetes TokenReview or delegated SelfSubjectReview request duration, including client-side rate-limit waits.",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}, labels),
	}
}

func (m *tokenReviewMetrics) observe(audience, outcome string, duration time.Duration) {
	m.requests.WithLabelValues(audience, outcome).Inc()
	m.duration.WithLabelValues(audience, outcome).Observe(duration.Seconds())
}

// RegisterMetrics registers this authenticator's TokenReview collectors with r.
func (a *Authenticator) RegisterMetrics(r prometheus.Registerer) {
	r.MustRegister(a.metrics.requests, a.metrics.duration)
}
