package rt

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/env"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/ingest"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/intercept"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/mount"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/cli"
)

func TestMountRootFromPresentedMetadata(t *testing.T) {
	m := &mount.Info{
		LocalDir: t.TempDir(),
		Mounts: types.MountPolicies{
			"/data":    types.MountPolicyRemote,
			"/ro":      types.MountPolicyRemoteReadOnly,
			"/local":   types.MountPolicyLocal,
			"/ignored": types.MountPolicyIgnore,
		},
	}
	values := map[string]string{"TELEPRESENCE_ROOT": m.LocalDir, "TELEPRESENCE_MOUNTS": "/data:/ro"}
	for _, verb := range []string{"intercept", "replace", "wiretap", "ingest"} {
		t.Run(verb, func(t *testing.T) {
			a := &Attach{}
			if verb == "ingest" {
				b, err := json.Marshal((&ingest.Info{Mount: m, Environment: values}).Presentation(false))
				require.NoError(t, err)
				var info cli.IngestInfo
				require.NoError(t, json.Unmarshal(b, &info))
				require.Equal(t, env.Redacted, info.Environment["TELEPRESENCE_ROOT"])
				a.Ingest = &info
			} else {
				b, err := json.Marshal((&intercept.Info{Mount: m, Environment: values}).Presentation(false))
				require.NoError(t, err)
				var info cli.InterceptInfo
				require.NoError(t, json.Unmarshal(b, &info))
				require.Equal(t, env.Redacted, info.Environment["TELEPRESENCE_ROOT"])
				switch verb {
				case "intercept":
					a.Intercept = &info
				case "replace":
					a.Replace = &info
				case "wiretap":
					a.Wiretap = &info
				}
			}
			root, ok := MountRoot(a)
			require.True(t, ok)
			require.Equal(t, m.LocalDir, root)
			require.Equal(t, map[string]string{
				"/data": "Remote", "/ro": "RemoteReadOnly", "/local": "Local", "/ignored": "Ignore",
			}, a.Mount().Mounts)
		})
	}

	for _, a := range []*Attach{
		{},
		{Intercept: &cli.InterceptInfo{}},
		{Ingest: &cli.IngestInfo{Mount: &cli.MountInfo{}}},
	} {
		root, ok := MountRoot(a)
		require.False(t, ok)
		require.Empty(t, root)
	}
}
