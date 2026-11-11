package env

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/v2/pkg/proc"
)

func TestPresentation(t *testing.T) {
	for _, values := range []map[string]string{nil, {}, {"ORDINARY": "ordinary-sentinel", "PASSWORD": "credential-sentinel", "MULTILINE": "first-sentinel\nsecond-sentinel"}} {
		before := maps.Clone(values)
		for _, show := range []bool{false, true} {
			result := Presentation(values, show)
			require.Equal(t, values == nil, result == nil)
			require.Len(t, result, len(values))
			for key, value := range values {
				if show {
					require.Equal(t, value, result[key])
				} else {
					require.Equal(t, Redacted, result[key])
				}
				result[key] = "changed"
			}
			require.Equal(t, before, values)
		}
	}
}

func TestExplicitEnvironmentExports(t *testing.T) {
	values := map[string]string{"ORDINARY": "ordinary-sentinel", "PASSWORD": "credential-sentinel"}
	for _, show := range []bool{false, true} {
		dir := t.TempDir()
		flags := Flags{File: filepath.Join(dir, "env"), JSON: filepath.Join(dir, "env.json"), Show: show}
		require.NoError(t, flags.MaybeWrite(values))
		for _, path := range []string{flags.File, flags.JSON} {
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Contains(t, string(data), "ordinary-sentinel")
			require.Contains(t, string(data), "credential-sentinel")
		}
	}
}

func TestRejectedEnvironmentValueIsNotInError(t *testing.T) {
	for _, syntax := range []Syntax{SyntaxDocker, SyntaxCmd} {
		text, err := syntax.WriteEntry("ORDINARY", "first-sentinel\nsecond-sentinel")
		require.Empty(t, text)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ORDINARY")
		require.NotContains(t, err.Error(), "sentinel")
		flags := Flags{File: filepath.Join(t.TempDir(), "env"), Syntax: syntax, Show: true}
		err = flags.MaybeWrite(map[string]string{"ORDINARY": "first-sentinel\nsecond-sentinel"})
		require.Error(t, err)
		require.NotContains(t, err.Error(), "sentinel")
	}
}

func TestExplicitStdoutExport(t *testing.T) {
	if mode := os.Getenv("TP_TEST_ENV_EXPORT"); mode != "" {
		flags := Flags{}
		if mode == "json" {
			flags.JSON = "-"
		} else {
			flags.File = "-"
		}
		if err := flags.MaybeWrite(map[string]string{"ORDINARY": "stdout-sentinel"}); err != nil {
			os.Exit(1)
		}
		if _, err := os.Stdout.WriteString("\nafter-export\n"); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	for _, mode := range []string{"json", "file"} {
		cmd := proc.CommandContext(context.Background(), executable, "-test.run=^TestExplicitStdoutExport$")
		cmd.Env = append(os.Environ(), "TP_TEST_ENV_EXPORT="+mode)
		data, err := cmd.CombinedOutput()
		require.NoError(t, err)
		require.Contains(t, string(data), "stdout-sentinel")
		require.Contains(t, string(data), "after-export")
	}
}
