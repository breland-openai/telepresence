//go:build !windows

package daemon

import (
	"context"
	"encoding/json/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/v2/pkg/filelocation"
)

func TestHostHeartbeatTouchesOnlyVerifiedInodeAfterPathReplacement(t *testing.T) {
	dir := t.TempDir()
	ctx := filelocation.WithAppUserCacheDir(context.Background(), dir)
	oldInfo := &Info{Name: "old", HostID: "physical-old", DaemonPort: 12345}
	require.NoError(t, NewUserInfoLoader(ctx).SaveInfo(oldInfo, InfoFileName))
	path := filepath.Join(dir, daemonsDirName, InfoFileName)
	past := time.Now().Add(-time.Minute)
	require.NoError(t, os.Chtimes(path, past, past))
	old, err := ReadHostInfo(ctx)
	require.NoError(t, err)
	replacement := filepath.Join(dir, "replacement.json")
	winner := &Info{Name: "winner", HostID: "physical-winner", DaemonPort: 12346}
	data, err := json.Marshal(winner)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(replacement, data, 0o600))
	future := time.Now().Add(time.Minute)
	require.NoError(t, os.Chtimes(replacement, future, future))
	winnerBefore, err := os.Stat(replacement)
	require.NoError(t, err)
	var verifiedOldTouched bool
	err = touchHostInfo(ctx, old, func(file hostFile) error {
		require.NoError(t, os.Rename(replacement, path))
		if touchErr := touchHostFile(file); touchErr != nil {
			return touchErr
		}
		oldAfter, statErr := file.Stat()
		if statErr == nil {
			verifiedOldTouched = os.SameFile(old.Stat, oldAfter) && oldAfter.ModTime().After(old.Stat.ModTime())
		}
		return statErr
	})
	require.NoError(t, err)
	require.True(t, verifiedOldTouched)
	winnerAfter, err := ReadHostInfo(ctx)
	require.NoError(t, err)
	require.True(t, os.SameFile(winnerBefore, winnerAfter.Stat))
	require.True(t, winnerBefore.ModTime().Equal(winnerAfter.Stat.ModTime()))
	require.Equal(t, *winner, *winnerAfter.Info)
	require.ErrorIs(t, touchHostInfo(ctx, old, touchHostFile), errHostOwnershipChanged)
}
