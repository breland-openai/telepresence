package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionForDescription(t *testing.T) {
	t.Chdir(t.TempDir())
	_, err := versionForDescription("ba1100")
	require.ErrorContains(t, err, "without a reachable tag")

	const changelog = "items:\n  - version: 2.33.0-next6-pre\n    date: (TBD)\n"
	require.NoError(t, os.WriteFile("CHANGELOG.yml", []byte(changelog), 0o600))
	for description, want := range map[string]string{
		"ba1100":            "2.33.0-next6-pre.gba1100",
		"v2.32.0-2-gba1100": "2.33.0-2-gba1100",
		"v2.33.0-next6-pre": "2.33.0-next6-pre",
	} {
		v, err := versionForDescription(description)
		require.NoError(t, err)
		require.Equal(t, want, v.String())
	}

	const released = "items:\n  - version: 2.33.0\n    date: 2026-11-10\n"
	require.NoError(t, os.WriteFile("CHANGELOG.yml", []byte(released), 0o600))
	v, err := versionForDescription("ba1100")
	require.NoError(t, err)
	require.Equal(t, "2.33.1-gba1100", v.String())
}
