package k8s

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	auth "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func TestCanPortForwardUsesEnabledTransportPermissions(t *testing.T) {
	reviewError := errors.New("temporary access review failure")
	tests := []struct {
		name       string
		forceSPDY  bool
		allowed    map[string]bool
		errors     map[string]error
		want       bool
		wantChecks []string
	}{
		{name: "websocket only", allowed: map[string]bool{"get": true}, want: true, wantChecks: []string{"get"}},
		{name: "both allowed", allowed: map[string]bool{"get": true, "create": true}, want: true, wantChecks: []string{"get"}},
		{name: "SPDY fallback", allowed: map[string]bool{"create": true}, want: true, wantChecks: []string{"get", "create"}},
		{name: "neither allowed", wantChecks: []string{"get", "create"}},
		{name: "websocket review error preserves SPDY", allowed: map[string]bool{"create": true}, errors: map[string]error{"get": reviewError}, want: true, wantChecks: []string{"get", "create"}},
		{name: "failed reviews do not allow forwarding", errors: map[string]error{"get": reviewError, "create": reviewError}, wantChecks: []string{"get", "create"}},
		{name: "forced SPDY rejects websocket-only access", forceSPDY: true, allowed: map[string]bool{"get": true}, wantChecks: []string{"create"}},
		{name: "forced SPDY accepts create", forceSPDY: true, allowed: map[string]bool{"create": true}, want: true, wantChecks: []string{"create"}},
		{name: "forced SPDY handles review failure", forceSPDY: true, allowed: map[string]bool{"get": true}, errors: map[string]error{"create": reviewError}, wantChecks: []string{"create"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var checks []string
			cs := fake.NewSimpleClientset()
			cs.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
				review := action.(k8stesting.CreateAction).GetObject().(*auth.SelfSubjectAccessReview)
				attrs := review.Spec.ResourceAttributes
				require.Equal(t, "pilot", attrs.Namespace)
				require.Equal(t, "pods", attrs.Resource)
				require.Equal(t, "portforward", attrs.Subresource)
				require.Empty(t, attrs.Name)
				checks = append(checks, attrs.Verb)
				if err := tt.errors[attrs.Verb]; err != nil {
					return true, nil, err
				}
				review.Status.Allowed = tt.allowed[attrs.Verb]
				return true, review, nil
			})
			cfg := client.GetDefaultConfig()
			cfg.Cluster().ForceSPDY = tt.forceSPDY
			ctx := k8sapi.WithK8sInterface(client.WithConfig(t.Context(), cfg), cs)
			require.Equal(t, tt.want, CanPortForward(ctx, "pilot"))
			require.Equal(t, tt.wantChecks, checks)
		})
	}
}
