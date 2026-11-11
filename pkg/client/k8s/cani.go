package k8s

import (
	"context"
	"errors"

	v1 "k8s.io/api/authorization/v1"

	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

// CanPortForward checks the transports available to this client. An empty pod
// name checks a namespace-wide grant; a name includes resourceNames grants.
func CanPortForward(ctx context.Context, namespace, podName string) (bool, error) {
	attrs := &v1.ResourceAttributes{Verb: "get", Resource: "pods", Subresource: "portforward", Namespace: namespace, Name: podName}
	var websocketErr error
	if !client.GetConfig(ctx).Cluster().ForceSPDY {
		var allowed bool
		allowed, websocketErr = k8sapi.CanI(ctx, attrs)
		if allowed {
			return true, nil
		}
	}
	attrs.Verb = "create"
	allowed, err := k8sapi.CanI(ctx, attrs)
	if allowed {
		return true, nil
	}
	return false, errors.Join(websocketErr, err)
}
