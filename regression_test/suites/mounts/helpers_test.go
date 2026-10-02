package mounts

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/env"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/cli"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/rt"
)

func TestRemoteMountPathsWhenEnvironmentRedacted(t *testing.T) {
	a := &rt.Attach{Intercept: &cli.InterceptInfo{
		Environment: map[string]string{"TELEPRESENCE_MOUNTS": env.Redacted},
		Mount: &cli.MountInfo{Mounts: map[string]string{
			"/config": "RemoteReadOnly", "/data": "Remote",
			"/tmp": "Local", "/ignored": "Ignore",
		}},
	}}
	require.Equal(t, []string{"/config", "/data"}, mountedPaths(a))
	require.Empty(t, mountedPaths(&rt.Attach{Intercept: &cli.InterceptInfo{
		Environment: map[string]string{"TELEPRESENCE_MOUNTS": env.Redacted},
	}}))
}
