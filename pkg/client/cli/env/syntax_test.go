package env

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyntax_WriteEntry(t *testing.T) {
	tests := []struct {
		name  string
		e     Syntax
		key   string
		value string
		want  string
	}{
		{
			`sh A=B C`,
			SyntaxSh,
			`A`,
			`B C`,
			`A='B C'`,
		},
		{
			`sh A=B "C"`,
			SyntaxSh,
			`A`,
			`B "C"`,
			`A='B "C"'`,
		},
		{
			`sh A="B C"`,
			SyntaxSh,
			`A`,
			`"B C"`,
			`A='"B C"'`,
		},
		{
			`sh A=B 'C X'`,
			SyntaxSh,
			`A`,
			`B 'C X'`,
			`A='B '\''C X'\'`,
		},
		{
			`compose A=B 'C X'`,
			SyntaxCompose,
			`A`,
			`B 'C X'`,
			`A='B \'C X\''`,
		},
		{
			`compose A=B\nC\t"D"`,
			SyntaxCompose,
			`A`,
			"B\nC\t\"D\"",
			`A="B\nC\t\"D\""`,
		},
		{
			`sh A='B C'`,
			SyntaxSh,
			`A`,
			`'B C'`,
			`A=\''B C'\'`,
		},
		{
			`sh A=\"B\" \"C\"`,
			SyntaxSh,
			`A`,
			`\"B\" \"C\"`,
			`A='\"B\" \"C\"'`,
		},
		{
			`ps A=B C`,
			SyntaxPS,
			`A`,
			`B C`,
			`$Env:A='B C'`,
		},
		{
			`ps A='B C'`,
			SyntaxPS,
			`A`,
			`'B C'`,
			`$Env:A='''B C'''`,
		},
		{
			`ps:export A='B C'`,
			SyntaxPSExport,
			`A`,
			`'B C'`,
			`[Environment]::SetEnvironmentVariable('A', '''B C''', 'User')`,
		},
		{
			`ps:export A=B C`,
			SyntaxPSExport,
			`A`,
			`B C`,
			`[Environment]::SetEnvironmentVariable('A', 'B C', 'User')`,
		},
		{
			`ps:export A="B C"`,
			SyntaxPSExport,
			`A`,
			`"B C"`,
			`[Environment]::SetEnvironmentVariable('A', '"B C"', 'User')`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := tt.e.WriteEntry(tt.key, tt.value)
			require.NoError(t, err)
			require.Equal(t, tt.want, r)
		})
	}
}

func TestFlagsWriteEnvFiles(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, "test.env")
	jsonPath := filepath.Join(dir, "test.json")
	require.NoError(t, os.WriteFile(envPath, []byte("PREVIOUS=1\n"), 0o644))
	require.NoError(t, os.WriteFile(jsonPath, []byte("{}"), 0o644))

	flags := Flags{File: envPath, JSON: jsonPath, Syntax: SyntaxShExport}
	require.NoError(t, flags.MaybeWrite(map[string]string{"B": "two words", "A": "one"}))
	gotEnv, err := os.ReadFile(envPath)
	require.NoError(t, err)
	require.Equal(t, "export A=one\nexport B='two words'\n", string(gotEnv))
	gotJSON, err := os.ReadFile(jsonPath)
	require.NoError(t, err)
	require.JSONEq(t, "{\"A\":\"one\",\"B\":\"two words\"}", string(gotJSON))

	for _, path := range []string{envPath, jsonPath} {
		info, err := os.Stat(path)
		require.NoError(t, err)
		if runtime.GOOS != "windows" {
			require.Zero(t, info.Mode().Perm()&0o077)
		}
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

func TestWriteFilePreservesOutputOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.env")
	previous := []byte("PREVIOUS=1\n")
	require.NoError(t, os.WriteFile(path, previous, 0o600))

	// The first entry exceeds the buffer, so a direct write would expose partial output.
	env := map[string]string{"A": strings.Repeat("a", 8192), "B": "invalid\nvalue"}
	require.ErrorContains(t, SyntaxDocker.writeFile(path, env), "multi-line")
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, previous, got)

	missingPath := filepath.Join(dir, "missing.env")
	require.ErrorContains(t, SyntaxDocker.writeFile(missingPath, env), "multi-line")
	require.NoFileExists(t, missingPath)

	destinationDir := filepath.Join(dir, "destination")
	require.NoError(t, os.Mkdir(destinationDir, 0o700))
	require.ErrorContains(t, SyntaxDocker.writeFile(destinationDir, map[string]string{"A": "one"}), "failed to publish")
	require.DirExists(t, destinationDir)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 2)
}
