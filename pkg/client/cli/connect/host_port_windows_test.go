package connect

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"syscall" //nolint:depguard // The synthetic Windows errno must not establish refusal.
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"

	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/daemon"
)

func TestWindowsHostRefusalFromClosedPortAndWrappedNativeErrors(t *testing.T) {
	dir := t.TempDir()
	ctx := hostTestContext(dir, "unused")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())
	hostTestSave(t, ctx, "old", port)
	path := filepath.Join(dir, "userd", daemon.InfoFileName)
	past := time.Now().Add(-time.Second)
	require.NoError(t, os.Chtimes(path, past, past))
	host, err := daemon.ReadHostInfo(ctx)
	require.NoError(t, err)

	probeErr := probeHostPort(ctx, port)
	require.ErrorIs(t, probeErr, windows.WSAECONNREFUSED, "physical closed port: %T %v", probeErr, probeErr)
	for _, initial := range []error{
		probeErr,
		windows.WSAECONNREFUSED,
		&net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connectex", windows.WSAECONNREFUSED)},
	} {
		stale, staleErr := hostDefinitelyStale(ctx, host, initial, 40*time.Millisecond)
		require.NoError(t, staleErr, "initial: %T %v", initial, initial)
		require.True(t, stale, "initial: %T %v", initial, initial)
	}
	for _, initial := range []error{
		syscall.ECONNREFUSED,
		windows.WSAETIMEDOUT,
		windows.WSAEHOSTUNREACH,
		windows.WSAEACCES,
		context.DeadlineExceeded,
		context.Canceled,
		os.ErrPermission,
		&net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connectex", windows.WSAECONNRESET)},
	} {
		stale, staleErr := hostDefinitelyStale(ctx, host, initial, 40*time.Millisecond)
		require.False(t, stale, "initial: %T %v", initial, initial)
		require.True(t, errors.Is(staleErr, initial), "initial: %T %v; result: %T %v", initial, initial, staleErr, staleErr)
	}
	current, err := daemon.ReadHostInfo(ctx)
	require.NoError(t, err)
	require.True(t, host.SameOwner(current))

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, probeHostPort(canceled, port), context.Canceled)
}
