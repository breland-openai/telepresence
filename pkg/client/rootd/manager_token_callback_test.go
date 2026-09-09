package rootd

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
)

func TestRootManagerTokenCallbackStatusAdvertisesCapabilityWithoutCredential(t *testing.T) {
	cfg := client.GetDefaultConfig()
	ctx := client.WithConfig(t.Context(), cfg)
	s := newService(cfg, true)
	s.Context = ctx
	result, err := s.Status(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	require.True(t, result.GetSupportsManagerTokenCallback())
	require.True(t, result.GetSupportsNegotiatedDevboxProxy())
	require.False(t, result.GetManagerTokenCallbackActive())
	require.Empty(t, result.GetManagerTokenCallbackId())
	require.Nil(t, result.GetOutboundConfig())
}

func TestRootManagerTokenCallbackConnectionInputOverridesProcessEnvironment(t *testing.T) {
	kc := &k8s.Kubeconfig{ManagerTokenFile: "/root/inherited", ManagerTokenFileSet: true}
	require.NoError(t, configureManagerTokenCallback(t.Context(), kc, &rpc.NetworkConfig{}))
	require.Empty(t, kc.ManagerTokenFile)
	require.False(t, kc.ManagerTokenFileSet)
	err := configureManagerTokenCallback(t.Context(), kc, &rpc.NetworkConfig{ManagerTokenCallback: &rpc.ManagerTokenCallback{Address: "127.0.0.1:1234", Capability: []byte("secret-malformed")}})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
}
