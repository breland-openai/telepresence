package k8s

import (
	"context"

	v1 "k8s.io/api/authorization/v1"

	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

// CanPortForward answers the question if this client has the RBAC permissions necessary
// to perform a port-forward to the connected namespace.
func CanPortForward(ctx context.Context, namespace string) bool {
	can := func(verb string) bool {
		ok, err := k8sapi.CanI(ctx, &v1.ResourceAttributes{
			Verb:        verb,
			Resource:    "pods",
			Subresource: "portforward",
			Namespace:   namespace,
		})
		return err == nil && ok
	}
	// The default transport first tries a WebSocket upgrade, which Kubernetes
	// authorizes as get. The legacy SPDY POST is authorized as create and is the
	// only transport available when forceSPDY is set.
	if !client.GetConfig(ctx).Cluster().ForceSPDY && can("get") {
		return true
	}
	return can("create")
}
