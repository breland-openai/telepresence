package k8s

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDevboxManagerPolicyFileCannotBeSubstituted(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "policy-directory")
	require.NoError(t, os.Mkdir(dir, 0o755))
	path := filepath.Join(dir, "policy.json")
	data := encodedDevboxManagerPolicy(t, devboxTestPolicy())
	require.NoError(t, os.WriteFile(path, data, 0o644))
	allowTempParents := func(info os.FileInfo) bool {
		return (info.Name() != "policy-directory" && info.Name() != "policy.json") || info.Mode().Perm()&0o022 == 0
	}
	require.NotNil(t, readDevboxManagerPolicy(path, allowTempParents))
	require.Nil(t, readDevboxManagerPolicy("relative.json", allowTempParents))
	require.Nil(t, readDevboxManagerPolicy(path, func(info os.FileInfo) bool { return info.Name() != "policy-directory" }))
	require.Nil(t, readDevboxManagerPolicy(path, func(info os.FileInfo) bool { return info.Name() != "policy.json" }))
	require.NoError(t, os.Chmod(path, 0o666))
	require.Nil(t, readDevboxManagerPolicy(path, allowTempParents))
	require.NoError(t, os.Chmod(path, 0o644))
	require.NoError(t, os.Chmod(dir, 0o777))
	require.Nil(t, readDevboxManagerPolicy(path, allowTempParents))
	require.NoError(t, os.Chmod(dir, 0o755))
	link := filepath.Join(dir, "symlink.json")
	require.NoError(t, os.Symlink(path, link))
	require.Nil(t, readDevboxManagerPolicy(link, allowTempParents))
	linkDir := filepath.Join(base, "symlink-dir")
	require.NoError(t, os.Symlink(dir, linkDir))
	require.Nil(t, readDevboxManagerPolicy(filepath.Join(linkDir, "policy.json"), allowTempParents))
	info, err := os.Lstat(path)
	require.NoError(t, err)
	require.Equal(t, os.Geteuid() == 0, rootOwnedDevboxManagerPolicyPath(info))
	require.NoError(t, os.WriteFile(path, make([]byte, maxDevboxManagerPolicySize+1), 0o644))
	require.Nil(t, readDevboxManagerPolicy(path, allowTempParents))
}
