package manager

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	tokenReviewQPS   = 50
	tokenReviewBurst = 100
)

func newTokenReviewClient(cfg *rest.Config) (*kubernetes.Clientset, error) {
	// Authentication must not queue behind informer or workload reconciliation.
	authConfig := rest.CopyConfig(cfg)
	authConfig.QPS = tokenReviewQPS
	authConfig.Burst = tokenReviewBurst
	authConfig.RateLimiter = nil
	return kubernetes.NewForConfig(authConfig)
}
