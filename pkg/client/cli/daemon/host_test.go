package daemon_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/filelocation"
)

func daemonHostTestSave(t *testing.T, ctx context.Context, name, id string) {
	t.Helper()
	require.NoError(t, daemon.NewUserInfoLoader(ctx).SaveInfo(&daemon.Info{
		Name: name, HostID: id, DaemonPort: 12345,
	}, daemon.InfoFileName))
}

func TestHostInfoAgeIsNonDestructiveAndOwnedCleanupDeletes(t *testing.T) {
	dir := t.TempDir()
	ctx := filelocation.WithAppUserCacheDir(context.Background(), dir)
	daemonHostTestSave(t, ctx, "old", "physical")
	path := filepath.Join(dir, "userd", daemon.InfoFileName)
	past := time.Now().Add(-time.Minute)
	require.NoError(t, os.Chtimes(path, past, past))
	host, err := daemon.ReadHostInfo(ctx)
	require.NoError(t, err)
	_, err = daemon.NewUserInfoLoader(ctx).LoadMatchingInfo(regexp.MustCompile("^old$"))
	require.NoError(t, err)
	loaded, err := daemon.NewUserInfoLoader(ctx).LoadInfo(daemon.InfoFileName)
	require.NoError(t, err)
	require.Equal(t, "old", loaded.Name)
	current, err := daemon.ReadHostInfo(ctx)
	require.NoError(t, err)
	require.True(t, host.Stat.ModTime().Equal(current.Stat.ModTime()))

	running, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- daemon.KeepHostInfoAlive(running, 12345, "physical", func() {}) }()
	require.Eventually(t, func() bool {
		newHost, readErr := daemon.ReadHostInfo(ctx)
		return readErr == nil && newHost.Stat.ModTime().After(host.Stat.ModTime())
	}, time.Second, 10*time.Millisecond)
	stop()
	require.NoError(t, <-done)
	_, err = daemon.ReadHostInfo(ctx)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestHostHeartbeatNeverTouchesOrDeletesChangedOwner(t *testing.T) {
	dir := t.TempDir()
	ctx := filelocation.WithAppUserCacheDir(context.Background(), dir)
	daemonHostTestSave(t, ctx, "old", "physical")
	path := filepath.Join(dir, "userd", daemon.InfoFileName)
	past := time.Now().Add(-time.Minute)
	require.NoError(t, os.Chtimes(path, past, past))
	running, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	replaced := make(chan struct{}, 1)
	go func() {
		done <- daemon.KeepHostInfoAlive(running, 12345, "physical", func() { replaced <- struct{}{} })
	}()
	require.Eventually(t, func() bool {
		host, err := daemon.ReadHostInfo(ctx)
		return err == nil && host.Stat.ModTime().After(past)
	}, time.Second, 10*time.Millisecond)
	daemonHostTestSave(t, ctx, "winner", "different")
	future := time.Now().Add(time.Minute)
	require.NoError(t, os.Chtimes(path, future, future))
	before, err := daemon.ReadHostInfo(ctx)
	require.NoError(t, err)
	select {
	case <-replaced:
	case <-time.After(4 * time.Second):
		t.Fatal("old process did not relinquish changed cache ownership")
	}
	require.NoError(t, <-done)
	after, err := daemon.ReadHostInfo(ctx)
	require.NoError(t, err)
	require.True(t, before.SameOwner(after))
	require.True(t, before.Stat.ModTime().Equal(after.Stat.ModTime()))
	require.Equal(t, "winner", after.Info.Name)
}

func TestHostHeartbeatPreservesMalformedCacheAndPhysicalProcess(t *testing.T) {
	dir := t.TempDir()
	ctx := filelocation.WithAppUserCacheDir(context.Background(), dir)
	daemonHostTestSave(t, ctx, "old", "physical")
	path := filepath.Join(dir, "userd", daemon.InfoFileName)
	past := time.Now().Add(-time.Minute)
	require.NoError(t, os.Chtimes(path, past, past))
	running, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	cancelled := make(chan struct{}, 1)
	go func() {
		done <- daemon.KeepHostInfoAlive(running, 12345, "physical", func() { cancelled <- struct{}{} })
	}()
	require.Eventually(t, func() bool {
		host, err := daemon.ReadHostInfo(ctx)
		return err == nil && host.Stat.ModTime().After(past)
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, os.WriteFile(path, []byte("{bad"), 0o600))
	select {
	case <-cancelled:
		t.Fatal("malformed cache canceled the physical daemon")
	case err := <-done:
		t.Fatalf("malformed cache stopped the physical heartbeat: %v", err)
	case <-time.After(2200 * time.Millisecond):
	}
	stop()
	require.NoError(t, <-done)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("{bad"), data)
}

func TestHostHeartbeatDistinguishesManualAndManagedAbsence(t *testing.T) {
	ctx := filelocation.WithAppUserCacheDir(context.Background(), t.TempDir())
	for _, id := range []string{"", "physical"} {
		cancelled := false
		require.NoError(t, daemon.KeepHostInfoAlive(ctx, 12345, id, func() { cancelled = true }))
		require.Equal(t, id != "", cancelled)
	}
}

func TestHostLockCanBeCancelledWithoutStealingOwnership(t *testing.T) {
	ctx := filelocation.WithAppUserCacheDir(context.Background(), t.TempDir())
	unlock, err := daemon.LockHost(ctx)
	require.NoError(t, err)
	wait, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	_, err = daemon.LockHost(wait)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	unlock()
	unlock, err = daemon.LockHost(ctx)
	require.NoError(t, err)
	unlock()
	_, err = daemon.NewUserInfoLoader(ctx).LoadInfos()
	require.NoError(t, err)
}
