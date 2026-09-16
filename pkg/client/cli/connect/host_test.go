package connect

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/telepresence/rpc/v2/common"
	"github.com/telepresenceio/telepresence/rpc/v2/connector"
	daemonRPC "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/ann"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/dos"
	"github.com/telepresenceio/telepresence/v2/pkg/filelocation"
)

type hostTestExe string

func (e hostTestExe) Executable() (string, error) { return string(e), nil }

type hostTestConnector struct {
	connector.UnimplementedConnectorServer
	mu         sync.Mutex
	active     *connector.ConnectInfo
	root       *daemonRPC.DaemonStatus
	statusErr  error
	connectErr error
	onConnect  func()
	onQuit     func()
	requests   []string
	exe        string
}

func (s *hostTestConnector) Version(ctx context.Context, _ *emptypb.Empty) (*common.VersionInfo, error) {
	return client.VersionInfo(dos.WithExe(ctx, hostTestExe(s.exe))), nil
}

func (s *hostTestConnector) RootDaemonVersion(ctx context.Context, e *emptypb.Empty) (*common.VersionInfo, error) {
	return s.Version(ctx, e)
}

func (s *hostTestConnector) Quit(context.Context, *emptypb.Empty) (*daemonRPC.QuitResponse, error) {
	if s.onQuit != nil {
		go s.onQuit()
	}
	return &daemonRPC.QuitResponse{}, nil
}

func (s *hostTestConnector) Status(_ context.Context, _ *emptypb.Empty) (*connector.ConnectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.statusErr != nil {
		return nil, s.statusErr
	}
	if s.active != nil {
		return proto.Clone(s.active).(*connector.ConnectInfo), nil
	}
	return &connector.ConnectInfo{DaemonStatus: s.root}, nil
}

func (s *hostTestConnector) Connect(_ context.Context, req *connector.ConnectRequest) (*connector.ConnectInfo, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req.GetName())
	onConnect := s.onConnect
	s.mu.Unlock()
	if onConnect != nil {
		onConnect()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connectErr != nil {
		return nil, s.connectErr
	}
	if s.active == nil {
		name := req.GetName()
		if name == "" {
			name = daemon.NewIdentifier("", req.GetKubeFlags()["context"], req.GetKubeFlags()["namespace"], false).Name
		}
		s.active = hostTestActive(name)
	}
	return proto.Clone(s.active).(*connector.ConnectInfo), nil
}

func (s *hostTestConnector) requestNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string{}, s.requests...)
}

func hostTestActive(name string) *connector.ConnectInfo {
	session := &manager.SessionInfo{SessionId: "session-" + name}
	return &connector.ConnectInfo{
		ConnectionName: name, ClusterContext: "test", Namespace: "ns",
		KubeFlags:   map[string]string{"context": "test", "namespace": "ns"},
		SessionInfo: session, ManagerVersion: &manager.VersionInfo2{Name: "OSS Traffic Manager", Version: "v2.31.2"},
		Version: client.VersionInfo(context.Background()), DaemonStatus: &daemonRPC.DaemonStatus{
			OutboundConfig: &daemonRPC.NetworkConfig{Session: session},
		},
	}
}

func hostTestContext(dir, exe string) context.Context {
	ctx := filelocation.WithAppUserCacheDir(context.Background(), dir)
	ctx = filelocation.WithAppUserConfigDir(ctx, filepath.Join(dir, "config"))
	ctx = filelocation.WithAppUserLogDir(ctx, filepath.Join(dir, "logs"))
	ctx = client.WithEnv(ctx, &client.Env{UserDaemonAddress: "root-is-supplied-by-this-test"})
	ctx = client.WithConfig(ctx, client.GetDefaultConfig())
	ctx = client.WithConfigFile(ctx, filepath.Join(dir, "config", "config.yml"))
	ctx = dos.WithExe(ctx, hostTestExe(exe))
	ctx = dos.WithStdout(ctx, io.Discard)
	return dos.WithStderr(ctx, io.Discard)
}

func hostTestRequest(name string, implicit bool, selector string) *daemon.Request {
	r := &daemon.Request{ConnectRequest: &connector.ConnectRequest{
		Name: name, KubeFlags: map[string]string{"context": "test", "namespace": "ns"},
	}, Implicit: implicit}
	if selector != "" {
		r.Use = regexp.MustCompile(selector)
	}
	return r
}

func hostTestInit(ctx context.Context, req *daemon.Request, annotation string) (*daemon.Session, error) {
	cmd := &cobra.Command{Use: "connect", Annotations: map[string]string{ann.Session: annotation}}
	cmd.SetContext(daemon.WithRequest(ctx, req))
	if err := InitCommand(cmd); err != nil {
		return nil, err
	}
	return daemon.GetSession(cmd.Context()), nil
}

func hostTestListen(t *testing.T, service *hostTestConnector) uint16 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	connector.RegisterConnectorServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(ln) }()
	t.Cleanup(grpcServer.Stop)
	return uint16(ln.Addr().(*net.TCPAddr).Port)
}

func hostTestSave(t *testing.T, ctx context.Context, name string, port uint16) *daemon.HostInfo {
	t.Helper()
	require.NoError(t, daemon.NewUserInfoLoader(ctx).SaveInfo(&daemon.Info{
		Name: name, KubeContext: "test", Namespace: "ns", DaemonPort: port, HostID: "physical-host",
	}, daemon.InfoFileName))
	host, err := daemon.ReadHostInfo(ctx)
	require.NoError(t, err)
	return host
}

func hostTestBytes(t *testing.T, dir string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "userd", daemon.InfoFileName))
	require.NoError(t, err)
	return data
}

func TestHostPreservesActualActiveName(t *testing.T) {
	dir := t.TempDir()
	ctx := hostTestContext(dir, "host-test-exe")
	service := &hostTestConnector{exe: "host-test-exe", active: hostTestActive("older.actual")}
	port := hostTestListen(t, service)
	hostTestSave(t, ctx, "cached-old", port)
	before := hostTestBytes(t, dir)
	_, err := hostTestInit(ctx, hostTestRequest("new", false, ""), ann.Required)
	require.ErrorContains(t, err, `host connection "older.actual" is already active`)
	require.ErrorContains(t, err, `older\.actual$`)
	require.Equal(t, before, hostTestBytes(t, dir))
	require.Empty(t, service.requestNames())
	require.NoError(t, probeHostPort(ctx, port))
}

func TestHostReusesInactivePhysicalConnector(t *testing.T) {
	dir := t.TempDir()
	ctx := hostTestContext(dir, "host-test-exe")
	service := &hostTestConnector{exe: "host-test-exe", root: &daemonRPC.DaemonStatus{}}
	port := hostTestListen(t, service)
	before := hostTestSave(t, ctx, "old", port)
	session, err := hostTestInit(ctx, hostTestRequest("new", false, ""), ann.Required)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	after, err := daemon.ReadHostInfo(ctx)
	require.NoError(t, err)
	require.True(t, before.SameOwner(after))
	require.Equal(t, "new", after.Info.Name)
	require.Equal(t, port, after.Info.DaemonPort)
	require.Equal(t, []string{"new"}, service.requestNames())
	require.Equal(t, "new", session.Info.ConnectionName)
	_, err = daemon.NewUserInfoLoader(ctx).LoadMatchingInfo(regexp.MustCompile("^new$"))
	require.NoError(t, err)
}

func TestHostPreservesUncertainOrSharedRoot(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   error
		root     *daemonRPC.DaemonStatus
		implicit bool
	}{
		{name: "reconnecting", status: status.Error(codes.FailedPrecondition, "root daemon is reconnecting")},
		{name: "connecting", status: status.Error(codes.FailedPrecondition, "connection in progress")},
		{name: "unavailable", status: status.Error(codes.Unavailable, "daemon unavailable")},
		{name: "timeout", status: status.Error(codes.DeadlineExceeded, "timeout")},
		{name: "permission", status: status.Error(codes.PermissionDenied, "permission")},
		{name: "cancelled", status: status.Error(codes.Canceled, "cancelled")},
		{name: "malformed"},
		{name: "root-active", root: hostTestActive("orphan").DaemonStatus},
		{name: "root-unconfirmed", root: &daemonRPC.DaemonStatus{OutboundConfig: &daemonRPC.NetworkConfig{}}},
		{name: "implicit-root-active", root: hostTestActive("orphan").DaemonStatus, implicit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := hostTestContext(dir, "host-test-exe")
			service := &hostTestConnector{exe: "host-test-exe", statusErr: tc.status, root: tc.root}
			port := hostTestListen(t, service)
			hostTestSave(t, ctx, "old", port)
			before := hostTestBytes(t, dir)
			request := hostTestRequest("new", tc.implicit, "")
			_, err := hostTestInit(ctx, request, ann.Required)
			require.ErrorContains(t, err, "preserved")
			require.Equal(t, "new", request.Name)
			require.Equal(t, "test", request.KubeFlags["context"])
			require.Equal(t, before, hostTestBytes(t, dir))
			require.Empty(t, service.requestNames())
			require.NoError(t, probeHostPort(ctx, port))
		})
	}
}

func TestHostExplicitSelectorKeepsSelection(t *testing.T) {
	dir := t.TempDir()
	ctx := hostTestContext(dir, "host-test-exe")
	service := &hostTestConnector{exe: "host-test-exe", active: hostTestActive("old")}
	port := hostTestListen(t, service)
	hostTestSave(t, ctx, "old", port)
	before := hostTestBytes(t, dir)
	for _, tc := range []struct {
		name       string
		implicit   bool
		annotation string
		optional   bool
	}{
		{name: "explicit", annotation: ann.Required},
		{name: "required-implicit", implicit: true, annotation: ann.Required},
		{name: "optional-implicit", implicit: true, annotation: ann.Optional, optional: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session, err := hostTestInit(ctx, hostTestRequest("new", tc.implicit, "^missing$"), tc.annotation)
			if tc.optional {
				require.NoError(t, err)
				require.Nil(t, session)
			} else {
				require.ErrorContains(t, err, `--use "^missing$"`)
			}
			require.Equal(t, before, hostTestBytes(t, dir))
		})
	}
	session, err := hostTestInit(ctx, hostTestRequest("new", false, "^old$"), ann.Required)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	require.Equal(t, "old", session.Info.ConnectionName)
	require.Equal(t, []string{"old"}, service.requestNames())
	require.Equal(t, before, hostTestBytes(t, dir))
	require.NoError(t, daemon.NewUserInfoLoader(ctx).SaveInfo(&daemon.Info{
		Name: "older-docker", DaemonPort: port, ContainerPID: 1,
	}, "older-docker.json"))
	_, err = hostTestInit(ctx, hostTestRequest("new", true, "^old"), ann.Required)
	require.ErrorContains(t, err, "multiple daemons")
	require.Equal(t, []string{"old"}, service.requestNames())
	require.Equal(t, before, hostTestBytes(t, dir))
}

func TestHostImplicitReuseAndIdleSelectorHonorTheirIdentity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		request  *daemon.Request
		expected string
		stored   string
	}{
		{name: "implicit-current-default", request: hostTestRequest("", true, ""), expected: "test-ns", stored: "test-ns"},
		{name: "idle-explicit-selector", request: hostTestRequest("new", false, "^old$"), expected: "old", stored: "old"},
		{name: "idle-implicit-selector", request: hostTestRequest("", true, "^old$"), expected: "old", stored: "old"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := hostTestContext(dir, "host-test-exe")
			service := &hostTestConnector{exe: "host-test-exe", root: &daemonRPC.DaemonStatus{}}
			port := hostTestListen(t, service)
			before := hostTestSave(t, ctx, "old", port)
			session, err := hostTestInit(ctx, tc.request, ann.Required)
			require.NoError(t, err)
			t.Cleanup(func() { _ = session.Close() })
			require.Equal(t, tc.expected, session.Info.GetConnectionName())
			after, err := daemon.ReadHostInfo(ctx)
			require.NoError(t, err)
			require.True(t, before.SameOwner(after))
			require.Equal(t, tc.stored, after.Info.Name)
		})
	}
}

func TestHostIsNotUsedForAnExplicitDockerRequest(t *testing.T) {
	dir := t.TempDir()
	ctx := hostTestContext(dir, "host-test-exe")
	service := &hostTestConnector{exe: "host-test-exe", active: hostTestActive("host")}
	port := hostTestListen(t, service)
	hostTestSave(t, ctx, "host", port)
	before := hostTestBytes(t, dir)
	request := hostTestRequest("docker", false, "")
	request.Docker = true
	id := daemon.NewIdentifier("docker", "test", "ns", true)
	_, launched, err := findOrLaunchConnectorDaemon(daemon.WithRequest(ctx, request), id, "unused", false)
	require.ErrorIs(t, err, daemon.ErrNoUserDaemon)
	require.False(t, launched)
	request.Use = regexp.MustCompile("^missing-docker$")
	_, launched, err = findOrLaunchConnectorDaemon(daemon.WithRequest(ctx, request), id, "unused", true)
	require.ErrorContains(t, err, "--use")
	require.False(t, launched)
	require.Empty(t, service.requestNames())
	require.Equal(t, before, hostTestBytes(t, dir))
}

func TestHostRequiredContenderWaitsForCallerCancellationOrTheActualOwner(t *testing.T) {
	dir := t.TempDir()
	ctx := hostTestContext(dir, "host-test-exe")
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	releaseOwner := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseOwner()
	service := &hostTestConnector{
		exe: "host-test-exe", root: &daemonRPC.DaemonStatus{},
		onConnect: func() { enterOnce.Do(func() { close(entered); <-release }) },
	}
	port := hostTestListen(t, service)
	physical := hostTestSave(t, ctx, "old", port)
	type result struct {
		session *daemon.Session
		err     error
	}
	start := func(callCtx context.Context, implicit bool, requirement string) (<-chan result, <-chan struct{}) {
		ready := make(chan struct{})
		done := make(chan result, 1)
		go func() {
			close(ready)
			session, err := hostTestInit(callCtx, hostTestRequest("same", implicit, ""), requirement)
			done <- result{session: session, err: err}
		}()
		return done, ready
	}
	owner, _ := start(ctx, false, ann.Required)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first CLI did not enter the native Connect RPC")
	}
	service.mu.Lock()
	service.statusErr = status.Error(codes.FailedPrecondition, "connection in progress")
	service.mu.Unlock()
	contender, started := start(ctx, false, ann.Required)
	<-started
	const formerDeadline = 45 * time.Millisecond
	formerClock := time.NewTimer(formerDeadline)
	defer formerClock.Stop()
	select {
	case early := <-contender:
		t.Fatalf("required CLI stopped before the simulated prior deadline: %v", early.err)
	case <-formerClock.C:
	}
	statusCtx, cancelStatus := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelStatus()
	optional, _ := start(statusCtx, true, ann.Optional)
	select {
	case observed := <-optional:
		require.ErrorContains(t, observed.err, "connection in progress")
		require.Nil(t, observed.session)
	case <-statusCtx.Done():
		t.Fatal("optional status waited for the host lock")
	}
	canceledCtx, cancelContender := context.WithCancel(ctx)
	defer cancelContender()
	canceled, started := start(canceledCtx, false, ann.Required)
	<-started
	continuedClock := time.NewTimer(formerDeadline)
	defer continuedClock.Stop()
	select {
	case early := <-contender:
		t.Fatalf("required CLI stopped after the simulated prior deadline while the owner was still connecting: %v", early.err)
	case early := <-canceled:
		t.Fatalf("third CLI stopped before its own caller canceled: %v", early.err)
	case <-continuedClock.C:
	}
	cancelContender()
	select {
	case observed := <-canceled:
		require.ErrorIs(t, observed.err, context.Canceled)
		require.Nil(t, observed.session)
	case <-time.After(time.Second):
		t.Fatal("canceled caller did not leave the wait promptly")
	}
	service.mu.Lock()
	service.statusErr = nil
	service.mu.Unlock()
	releaseOwner()
	for _, finished := range []<-chan result{owner, contender} {
		select {
		case observed := <-finished:
			require.NoError(t, observed.err)
			require.Equal(t, "same", observed.session.Info.GetConnectionName())
			t.Cleanup(func() { _ = observed.session.Close() })
		case <-time.After(2 * time.Second):
			t.Fatal("required CLI did not observe the owner after its native Connect completed")
		}
	}
	after, err := daemon.ReadHostInfo(ctx)
	require.NoError(t, err)
	require.True(t, physical.SameOwner(after))
	require.Equal(t, "same", after.Info.Name)
	require.Equal(t, []string{"same", "same"}, service.requestNames())
}

func TestHostConnectFailurePreservesPhysicalOwner(t *testing.T) {
	dir := t.TempDir()
	ctx := hostTestContext(dir, "host-test-exe")
	service := &hostTestConnector{
		exe: "host-test-exe", root: &daemonRPC.DaemonStatus{},
		connectErr: status.Error(codes.InvalidArgument, "bad configuration"),
	}
	port := hostTestListen(t, service)
	hostTestSave(t, ctx, "old", port)
	before := hostTestBytes(t, dir)
	_, err := hostTestInit(ctx, hostTestRequest("new", false, ""), ann.Required)
	require.ErrorContains(t, err, "bad configuration")
	require.Equal(t, before, hostTestBytes(t, dir))
	require.NoError(t, probeHostPort(ctx, port))
	service.mu.Lock()
	service.connectErr = nil
	service.mu.Unlock()
	session, err := hostTestInit(ctx, hostTestRequest("new", false, ""), ann.Required)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
}

func TestHostCannotOverwriteOrDeleteChangedOwner(t *testing.T) {
	dir := t.TempDir()
	ctx := hostTestContext(dir, "host-test-exe")
	service := &hostTestConnector{exe: "host-test-exe", root: &daemonRPC.DaemonStatus{}}
	port := hostTestListen(t, service)
	old := hostTestSave(t, ctx, "old", port)
	service.onConnect = func() {
		_ = daemon.NewUserInfoLoader(ctx).SaveInfo(&daemon.Info{Name: "winner", HostID: "different-host", DaemonPort: port}, daemon.InfoFileName)
	}
	_, err := hostTestInit(ctx, hostTestRequest("new", false, ""), ann.Required)
	require.ErrorContains(t, err, "ownership changed")
	after, err := daemon.ReadHostInfo(ctx)
	require.NoError(t, err)
	require.Equal(t, "winner", after.Info.Name)
	require.ErrorContains(t, old.Delete(ctx), "ownership changed")
	require.Equal(t, "winner", after.Info.Name)
}

func TestHostStaleRequiresHardRefusalAndUnchangedHeartbeat(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fresh     bool
		advance   bool
		initial   error
		wantStale bool
	}{
		{name: "dead", wantStale: true},
		{name: "fresh-start", fresh: true},
		{name: "timeout", initial: context.DeadlineExceeded},
		{name: "permission", initial: os.ErrPermission},
		{name: "heartbeat-advanced", advance: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := hostTestContext(dir, "unused")
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			port := uint16(ln.Addr().(*net.TCPAddr).Port)
			require.NoError(t, ln.Close())
			hostTestSave(t, ctx, "old", port)
			path := filepath.Join(dir, "userd", daemon.InfoFileName)
			if !tc.fresh {
				past := time.Now().Add(-time.Second)
				require.NoError(t, os.Chtimes(path, past, past))
			}
			host, err := daemon.ReadHostInfo(ctx)
			require.NoError(t, err)
			if tc.advance {
				go func() {
					time.Sleep(20 * time.Millisecond)
					now := time.Now()
					_ = os.Chtimes(path, now, now)
				}()
			}
			initial := tc.initial
			if initial == nil {
				initial = probeHostPort(ctx, port)
				require.Error(t, initial)
			}
			stale, staleErr := hostDefinitelyStale(ctx, host, initial, 80*time.Millisecond)
			require.Equal(t, tc.wantStale, stale)
			if tc.wantStale {
				require.NoError(t, staleErr)
			} else {
				require.Error(t, staleErr)
			}
			_, err = daemon.ReadHostInfo(ctx)
			require.NoError(t, err)
		})
	}
}

func TestHostCachePeekAndSlowLivePortNeverDelete(t *testing.T) {
	t.Run("malformed", func(t *testing.T) {
		dir := t.TempDir()
		ctx := hostTestContext(dir, "unused")
		hostTestSave(t, ctx, "old", 12000)
		path := filepath.Join(dir, "userd", daemon.InfoFileName)
		require.NoError(t, os.WriteFile(path, []byte("{bad"), 0o600))
		past := time.Now().Add(-time.Minute)
		require.NoError(t, os.Chtimes(path, past, past))
		_, err := daemon.NewUserInfoLoader(ctx).LoadMatchingInfo(regexp.MustCompile("^new$"))
		require.Error(t, err)
		require.Equal(t, []byte("{bad"), hostTestBytes(t, dir))
	})
	t.Run("slow-live", func(t *testing.T) {
		dir := t.TempDir()
		ctx := hostTestContext(dir, "unused")
		ctx, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
		defer cancel()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = ln.Close() })
		port := uint16(ln.Addr().(*net.TCPAddr).Port)
		hostTestSave(t, ctx, "old", port)
		before := hostTestBytes(t, dir)
		_, err = hostTestInit(ctx, hostTestRequest("new", false, ""), ann.Required)
		require.ErrorContains(t, err, "preserved")
		require.Equal(t, before, hostTestBytes(t, dir))
	})
	t.Run("genuinely-absent", func(t *testing.T) {
		ctx := hostTestContext(t.TempDir(), "unused")
		id := daemon.NewIdentifier("new", "test", "ns", false)
		_, missing, err := hostConnectorOrMissing(ctx, id, hostTestRequest("new", false, ""))
		require.NoError(t, err)
		require.True(t, missing)
		_, err = daemon.ReadHostInfo(ctx)
		require.True(t, errors.Is(err, os.ErrNotExist))
	})
}
