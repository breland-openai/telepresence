package k8s

import (
	"io"
	"os"
	"path/filepath"
	"syscall" //nolint:depguard // os.FileInfo.Sys uses syscall.Stat_t for ownership.
)

const systemDevboxManagerPolicyFile = "/etc/telepresence/manager-auth-policy.json"

func loadSystemDevboxManagerPolicy() *devboxManagerPolicy {
	return readDevboxManagerPolicy(systemDevboxManagerPolicyFile, rootOwnedDevboxManagerPolicyPath)
}

func rootOwnedDevboxManagerPolicyPath(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0 && info.Mode().Perm()&0o022 == 0
}

func readDevboxManagerPolicy(path string, trusted func(os.FileInfo) bool) *devboxManagerPolicy {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil
	}
	for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
		info, err := os.Lstat(dir)
		if err != nil || !info.IsDir() || !trusted(info) {
			return nil
		}
		if dir == filepath.Dir(dir) {
			break
		}
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || !trusted(before) || before.Size() > maxDevboxManagerPolicySize {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !trusted(after) || !os.SameFile(before, after) {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(file, maxDevboxManagerPolicySize+1))
	if err != nil {
		return nil
	}
	return parseDevboxManagerPolicy(data)
}
