//go:build !windows

package connect

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/telepresence/rpc/v2/connector"
	daemonRPC "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/ann"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/shellquote"
)

func TestHostProcess(t *testing.T) {
	separator := slices.Index(os.Args, "--")
	if separator < 0 || separator+1 >= len(os.Args) {
		return
	}
	args := os.Args[separator+1:]
	dir := os.Getenv("TP_HOST_TEST_DIR")
	wrapper := os.Getenv("TP_HOST_TEST_WRAPPER")
	require.NotEmpty(t, dir)
	require.NotEmpty(t, wrapper)
	if args[0] == "cli" {
		require.Len(t, args, 2)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "ready-"+args[1]), nil, 0o600))
		session, err := hostTestInit(hostTestContext(dir, wrapper), hostTestRequest(args[1], false, ""), ann.Required)
		require.NoError(t, err)
		defer session.Close()
		fmt.Println("HOST OWNER " + session.Info.ConnectionName)
		return
	}
	require.Equal(t, "userd", args[0])
	address := slices.Index(args, "--address")
	require.GreaterOrEqual(t, address, 0)
	require.Less(t, address+1, len(args))
	ln, err := net.Listen("tcp", "127.0.0.1"+args[address+1])
	require.NoError(t, err)
	marker, err := os.OpenFile(filepath.Join(dir, "launches"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = fmt.Fprintln(marker, os.Getpid())
	require.NoError(t, err)
	require.NoError(t, marker.Close())
	server := grpc.NewServer()
	service := &hostTestConnector{exe: wrapper, root: &daemonRPC.DaemonStatus{}, onConnect: hostProcessConnectGate(dir)}
	service.onQuit = func() {
		time.Sleep(30 * time.Millisecond)
		server.Stop()
	}
	connector.RegisterConnectorServer(server, service)
	require.NoError(t, server.Serve(ln))
}

func hostProcessConnectGate(dir string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = os.WriteFile(filepath.Join(dir, "entered"), nil, 0o600)
			deadline := time.Now().Add(20 * time.Second)
			for time.Now().Before(deadline) {
				if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

type hostProcessResult struct {
	output string
	err    error
}

func startHostCLIProcess(t *testing.T, ctx context.Context, dir, wrapper, name string) <-chan hostProcessResult {
	t.Helper()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHostProcess$", "--", "cli", name)
	cmd.Env = append(os.Environ(), "TP_HOST_TEST_DIR="+dir, "TP_HOST_TEST_WRAPPER="+wrapper)
	var output strings.Builder
	cmd.Stdout, cmd.Stderr = &output, &output
	require.NoError(t, cmd.Start())
	result := make(chan hostProcessResult, 1)
	go func() {
		err := cmd.Wait()
		result <- hostProcessResult{output: output.String(), err: err}
	}()
	return result
}

func hostProcessStop(dir string) {
	_ = os.WriteFile(filepath.Join(dir, "release"), nil, 0o600)
	ctx, cancel := context.WithTimeout(hostTestContext(dir, "unused"), 2*time.Second)
	defer cancel()
	if host, err := daemon.ReadHostInfo(ctx); err == nil {
		if conn, dialErr := daemon.DialHostInfo(ctx, host); dialErr == nil {
			defer conn.Close()
			if _, quitErr := connector.NewConnectorClient(conn).Quit(ctx, &emptypb.Empty{}); quitErr == nil {
				return
			}
		}
	}
	if data, err := os.ReadFile(filepath.Join(dir, "launches")); err == nil {
		for _, line := range strings.Fields(string(data)) {
			if pid, parseErr := strconv.Atoi(line); parseErr == nil {
				if process, findErr := os.FindProcess(pid); findErr == nil {
					_ = process.Kill()
				}
			}
		}
	}
}

func TestHostCompetingCLIProcesses(t *testing.T) {
	for _, cold := range []bool{false, true} {
		name := "inactive"
		if cold {
			name = "cold"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			wrapper := filepath.Join(dir, "connector")
			require.NoError(t, os.WriteFile(wrapper, []byte("#!/bin/sh\nexec "+shellquote.Unix(os.Args[0])+" -test.run='^TestHostProcess$' -- \"$@\"\n"), 0o700))
			ctx := hostTestContext(dir, wrapper)
			var physical *daemon.HostInfo
			if cold {
				t.Cleanup(func() { hostProcessStop(dir) })
			} else {
				service := &hostTestConnector{exe: wrapper, root: &daemonRPC.DaemonStatus{}, onConnect: hostProcessConnectGate(dir)}
				port := hostTestListen(t, service)
				physical = hostTestSave(t, ctx, "old", port)
				t.Cleanup(func() { _ = os.WriteFile(filepath.Join(dir, "release"), nil, 0o600) })
			}
			processCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			t.Cleanup(cancel)
			first := startHostCLIProcess(t, processCtx, dir, wrapper, "alpha")
			second := startHostCLIProcess(t, processCtx, dir, wrapper, "beta")
			require.Eventually(t, func() bool {
				for _, file := range []string{"ready-alpha", "ready-beta", "entered"} {
					if _, err := os.Stat(filepath.Join(dir, file)); err != nil {
						return false
					}
				}
				return true
			}, 12*time.Second, 10*time.Millisecond)
			for _, result := range []<-chan hostProcessResult{first, second} {
				select {
				case early := <-result:
					t.Fatalf("CLI exited before the first connection was released: %s (%v)", early.output, early.err)
				default:
				}
			}
			require.NoError(t, os.WriteFile(filepath.Join(dir, "release"), nil, 0o600))
			results := []hostProcessResult{<-first, <-second}
			host, err := daemon.ReadHostInfo(ctx)
			require.NoError(t, err)
			require.Contains(t, []string{"alpha", "beta"}, host.Info.Name)
			successes := 0
			for _, result := range results {
				if result.err == nil {
					successes++
					require.Contains(t, result.output, "HOST OWNER "+host.Info.Name)
				} else {
					require.Contains(t, result.output, `host connection "`+host.Info.Name+`" is already active`)
				}
			}
			require.Equal(t, 1, successes)
			infos, err := daemon.NewUserInfoLoader(ctx).LoadInfos()
			require.NoError(t, err)
			require.Len(t, infos, 1)
			if cold {
				data, readErr := os.ReadFile(filepath.Join(dir, "launches"))
				require.NoError(t, readErr)
				require.Len(t, strings.Fields(string(data)), 1)
				require.NotEmpty(t, host.Info.HostID)
			} else {
				require.True(t, physical.SameOwner(host))
				_, statErr := os.Stat(filepath.Join(dir, "launches"))
				require.ErrorIs(t, statErr, os.ErrNotExist)
			}
		})
	}
}
