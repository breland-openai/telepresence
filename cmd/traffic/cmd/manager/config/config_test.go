package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/clog/testutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/tmconfig"
)

func TestGetClientConfigYamlPreservesLocalDNSNamesWhenAmended(t *testing.T) {
	ctx := managerutil.WithEnv(testutil.NewContext(t, false), &managerutil.Env{})
	watcher := &config{clientYAML: []byte(`cluster:
  mappedNamespaces:
    - application
dns:
  preserveLocalClusterDNSNames:
    - artifact-gateway.platform.svc.cluster.local
`)}

	servedYAML := watcher.GetClientConfigYaml(ctx)
	cfg, err := client.ParseConfigYAML(ctx, tmconfig.ClientConfigFileName, servedYAML)
	require.NoError(t, err)
	require.Empty(t, cfg.Cluster().MappedNamespaces, "namespace changes must force manager reserialization")
	require.Equal(t, []string{"artifact-gateway.platform.svc.cluster.local"}, cfg.DNS().PreserveLocalClusterDNSNames)
}
