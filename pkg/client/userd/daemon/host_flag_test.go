package daemon_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	userd "github.com/telepresenceio/telepresence/v2/pkg/client/userd/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/filelocation"
)

func TestHostOwnerFlagIsHiddenFromExplicitDaemonHelpButCanBeParsed(t *testing.T) {
	cmd := userd.Command(filelocation.WithAppUserLogDir(t.Context(), t.TempDir()))
	require.True(t, cmd.Hidden)
	flag := cmd.Flags().Lookup("host-id")
	require.NotNil(t, flag)
	require.True(t, flag.Hidden)
	require.NotContains(t, cmd.UsageString(), "--host-id")
	require.Contains(t, cmd.UsageString(), "--address")
	require.NoError(t, cmd.ParseFlags([]string{"--host-id", "physical-test-owner"}))
	value, err := cmd.Flags().GetString("host-id")
	require.NoError(t, err)
	require.Equal(t, "physical-test-owner", value)
}
