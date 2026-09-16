package trafficmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/rpc/v2/connector"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
)

func TestCheckStatusUsesMappedNamespaceSet(t *testing.T) {
	ctx := client.WithConfig(context.Background(), client.GetDefaultConfig())
	ctx = client.WithEnv(ctx, &client.Env{})
	kubeconfigData := []byte(`apiVersion: v1
kind: Config
current-context: fixture
clusters:
- name: fixture
  cluster:
    server: https://127.0.0.1:1
contexts:
- name: fixture
  context:
    cluster: fixture
    user: fixture
users:
- name: fixture
  user: {}
`)
	newRequest := func(namespaces []string) *connector.ConnectRequest {
		return &connector.ConnectRequest{KubeconfigData: kubeconfigData, MappedNamespaces: namespaces}
	}
	config, err := k8s.DaemonKubeconfig(ctx, newRequest(nil))
	require.NoError(t, err)
	s := &session{Cluster: &k8s.Cluster{Kubeconfig: config, MappedNamespaces: []string{"alpha", "beta"}}}

	for _, tt := range []struct {
		name       string
		namespaces []string
		wantError  bool
	}{
		{name: "canonical order", namespaces: []string{"alpha", "beta"}},
		{name: "same set reverse order", namespaces: []string{"beta", "alpha"}},
		{name: "same set duplicates", namespaces: []string{"beta", "alpha", "beta"}},
		{name: "implicit selection"},
		{name: "explicit all", namespaces: []string{"all"}},
		{name: "changed selection", namespaces: []string{"alpha", "gamma"}, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := s.CheckStatus(newRequest(tt.namespaces))
			if tt.wantError {
				require.ErrorContains(t, err, "Cluster configuration changed")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEffectiveMappedNamespacesAllowsUnmanagedRequestedNamespace(t *testing.T) {
	// A requested namespace the traffic-manager does not manage is still mapped
	// for DNS; mapping does not require management.
	namespaces := effectiveMappedNamespaces(
		[]string{"alpha", "beta"},
		nil,
		[]string{"alpha"},
	)
	require.Equal(t, []string{"alpha", "beta"}, namespaces)
}

func TestEffectiveMappedNamespacesAllowsUnmanagedClientConfigNamespace(t *testing.T) {
	namespaces := effectiveMappedNamespaces(
		nil,
		[]string{"beta"},
		[]string{"alpha"},
	)
	require.Equal(t, []string{"beta"}, namespaces)
}

func TestEffectiveMappedNamespacesUsesManagerNamespacesWhenAllRequested(t *testing.T) {
	namespaces := effectiveMappedNamespaces(
		[]string{"all"},
		[]string{"beta"},
		[]string{"alpha", "gamma"},
	)
	require.Equal(t, []string{"alpha", "gamma"}, namespaces)
}

func TestEffectiveMappedNamespacesAllowsGlobalManager(t *testing.T) {
	namespaces := effectiveMappedNamespaces(
		[]string{"beta"},
		nil,
		nil,
	)
	require.Equal(t, []string{"beta"}, namespaces)
}
