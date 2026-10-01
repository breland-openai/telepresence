package compose

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/cmd/cobraparser/v2/types"
)

func TestConfigurationExportsRequireOptIn(t *testing.T) {
	for _, name := range []string{"config", "bridge", "publish", "build", "up", "exec"} {
		t.Run(name, func(t *testing.T) {
			c := &config{parentConfig: &parentConfig{}}
			cmd := &cobra.Command{Use: name}
			cmd.Flags().Bool("print", false, "")
			err := c.checkEnvironmentExport(cmd)
			if name == "build" || name == "up" || name == "exec" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "--show-env")
			}
			require.NoError(t, cmd.Flags().Set("print", "true"))
			if name == "build" {
				require.ErrorContains(t, c.checkEnvironmentExport(cmd), "--show-env")
			}
			c.showEnv = true
			require.NoError(t, c.checkEnvironmentExport(cmd))
		})
	}
	c := &config{parentConfig: &parentConfig{}}
	cmd := c.subCommand(&types.CommandInfo{Name: "config"})
	require.NotNil(t, cmd.Flags().Lookup("show-env"))
	require.NoError(t, cmd.Flags().Set("show-env", "true"))
	require.True(t, c.showEnv)
	require.Empty(t, c.appendFlags(c.subCommandFlags, nil), "Telepresence flag must not reach Docker")
}

func TestBuildPrintAfterServiceRequiresOptIn(t *testing.T) {
	for _, printArg := range []string{"--print", "--print=true", "--print=false"} {
		t.Run(printArg, func(t *testing.T) {
			parent := &cobra.Command{Use: "compose"}
			for _, cmd := range GenerateSubCommands(parent) {
				if cmd.Name() != "build" {
					continue
				}
				cmd.SetArgs([]string{"example", printArg})
				cmd.SilenceUsage = true
				cmd.SilenceErrors = true
				require.ErrorContains(t, cmd.Execute(), "--show-env")
			}
		})
	}
}
