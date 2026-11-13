package manager

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func newTokenReviewClient(cfg *rest.Config, qps float32, burst int) (*kubernetes.Clientset, error) {
	// Authentication must not queue behind informer or workload reconciliation.
	authConfig := rest.CopyConfig(cfg)
	authConfig.QPS = qps
	authConfig.Burst = burst
	authConfig.RateLimiter = nil
	return kubernetes.NewForConfig(authConfig)
}
