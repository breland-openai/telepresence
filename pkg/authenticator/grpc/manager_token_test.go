package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/authenticator"
)

type recordingManagerTokenProvider struct {
	capability []byte
	podUID     string
	audience   string
}

func (p *recordingManagerTokenProvider) ManagerToken(_ context.Context, capability []byte, podUID, audience string) (string, error) {
	p.capability, p.podUID, p.audience = capability, podUID, audience
	return "test-token", nil
}

func TestManagerTokenCallbackForwardsNegotiatedBinding(t *testing.T) {
	for _, tc := range []struct{ name, pod, audience string }{
		{name: "explicit token remains unbound"},
		{name: "negotiation preserves both bindings", pod: "manager-pod-uid", audience: "manager-only-audience"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := &recordingManagerTokenProvider{}
			server := &AuthenticatorServer{managerTokenProvider: provider}
			capability := []byte("private-capability")
			res, err := server.GetManagerToken(t.Context(), &rpc.GetManagerTokenRequest{
				Capability: capability, ManagerPodUid: tc.pod, DevboxProxyAudience: tc.audience,
			})
			require.NoError(t, err)
			require.Equal(t, "test-token", res.GetToken())
			require.Equal(t, capability, provider.capability)
			require.Equal(t, tc.pod, provider.podUID)
			require.Equal(t, tc.audience, provider.audience)
		})
	}
	_, err := (&AuthenticatorServer{}).GetManagerToken(t.Context(), &rpc.GetManagerTokenRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}
