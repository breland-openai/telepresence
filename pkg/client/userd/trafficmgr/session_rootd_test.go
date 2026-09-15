package trafficmgr

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	connectorRpc "github.com/telepresenceio/telepresence/rpc/v2/connector"
	rootdRpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	cliDaemon "github.com/telepresenceio/telepresence/v2/pkg/client/cli/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
	"github.com/telepresenceio/telepresence/v2/pkg/client/remotefs"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func TestSessionStatusUsesManagerInstallIDWithoutKubernetes(t *testing.T) {
	server := &rootDaemonReconnectTestServer{}
	conn, cleanup, err := dialTestRootDaemon(server)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	s := rootStatusTestSession(t.Context(), conn, "manager-namespace-id")
	info, err := s.Status(t.Context())
	require.NoError(t, err)
	require.Equal(t, "manager-namespace-id", info.ManagerInstallId)
}

func TestSessionStatusForwardsCallerDeadlineToRootDaemon(t *testing.T) {
	server := &rootDaemonStalledStatusTestServer{started: make(chan struct{}), canceled: make(chan struct{})}
	conn, cleanup, err := dialTestRootDaemon(server)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	s := rootStatusTestSession(t.Context(), conn, "manager-namespace-id")
	requestCtx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, callErr := s.Status(requestCtx)
		result <- callErr
	}()
	select {
	case <-server.started:
	case <-time.After(2 * time.Second):
		t.Fatal("root daemon status was not called")
	}
	select {
	case err = <-result:
		require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	case <-time.After(2 * time.Second):
		t.Fatal("root daemon status did not respect the request deadline")
	}
	select {
	case <-server.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("root daemon status handler did not observe request cancellation")
	}
	require.NoError(t, s.Err())
}

func TestSessionStatusFallbackKubernetesHonorsCallerCancellation(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	t.Cleanup(api.Close)
	ki, err := kubernetes.NewForConfig(&rest.Config{Host: api.URL})
	require.NoError(t, err)
	conn, cleanup, err := dialTestRootDaemon(&rootDaemonReconnectTestServer{})
	require.NoError(t, err)
	t.Cleanup(cleanup)
	s := rootStatusTestSession(k8sapi.WithK8sInterface(t.Context(), ki), conn, "")
	requestCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, callErr := s.Status(requestCtx)
		result <- callErr
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the older session did not request its manager namespace ID")
	}
	cancel()
	select {
	case err = <-result:
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(2 * time.Second):
		t.Fatal("the namespace lookup did not respect request cancellation")
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("Kubernetes handler did not observe request cancellation")
	}
	require.NoError(t, s.Err())
}

func rootStatusTestSession(ctx context.Context, conn *grpc.ClientConn, managerInstallID string) *session {
	ctx = client.WithConfig(ctx, client.GetDefaultConfig())
	return &session{
		Cluster: &k8s.Cluster{Kubeconfig: &k8s.Kubeconfig{
			Context: ctx, Namespace: "default", KubeContext: "test", Server: "https://cluster.example",
		}},
		daemonID:       cliDaemon.NewIdentifier("test", "test", "default", false),
		sessionInfo:    &manager.SessionInfo{SessionId: "test-session", ManagerInstallId: managerInstallID},
		rootDaemon:     rootdRpc.NewDaemonClient(conn),
		currentIngests: xsync.NewMap[ingestKey, *ingest](),
	}
}

type rootDaemonStalledStatusTestServer struct {
	rootdRpc.UnimplementedDaemonServer
	started, canceled chan struct{}
}

func (s *rootDaemonStalledStatusTestServer) Status(ctx context.Context, _ *emptypb.Empty) (*rootdRpc.DaemonStatus, error) {
	close(s.started)
	<-ctx.Done()
	close(s.canceled)
	return nil, ctx.Err()
}

func TestRootDaemonActivityWatcherReconnectsRootDaemon(t *testing.T) {
	const sessionID = "test-session"

	ctx, cancel := context.WithCancel(client.WithConfig(context.Background(), client.GetDefaultConfig()))
	defer cancel()

	first := &rootDaemonReconnectTestServer{
		name:        "first",
		sessionID:   sessionID,
		activityErr: status.Error(codes.Unavailable, "rootd exited"),
	}
	second := &rootDaemonReconnectTestServer{
		name:      "second",
		sessionID: sessionID,
	}
	servers := []*rootDaemonReconnectTestServer{first, second}

	var dialCount atomic.Int32
	var cleanupLock sync.Mutex
	var cleanups []func()
	dialRootDaemon := func(_ context.Context, _ bool) (*grpc.ClientConn, error) {
		idx := int(dialCount.Add(1)) - 1
		if idx >= len(servers) {
			return nil, fmt.Errorf("unexpected root daemon dial %d", idx+1)
		}
		conn, cleanup, err := dialTestRootDaemon(servers[idx])
		if err != nil {
			return nil, err
		}
		cleanupLock.Lock()
		cleanups = append(cleanups, cleanup)
		cleanupLock.Unlock()
		return conn, nil
	}
	t.Cleanup(func() {
		cleanupLock.Lock()
		defer cleanupLock.Unlock()
		for _, cleanup := range cleanups {
			cleanup()
		}
	})

	s := &session{
		Cluster: &k8s.Cluster{
			Kubeconfig: &k8s.Kubeconfig{
				Context:     ctx,
				Namespace:   "default",
				KubeContext: "test",
				Server:      "https://cluster.example",
			},
		},
		service:        rootDaemonReconnectTestService{},
		sessionInfo:    &manager.SessionInfo{SessionId: sessionID},
		dialRootDaemon: dialRootDaemon,
	}
	nc := &rootdRpc.NetworkConfig{
		Namespace: "default",
		Session:   s.sessionInfo,
	}

	require.NoError(t, s.connectRootDaemon(ctx, nc, nil, false))
	require.Eventually(t, func() bool {
		return second.connectCount.Load() == 1
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(2), dialCount.Load())
	require.Equal(t, int32(1), first.connectCount.Load())
	require.Equal(t, int32(1), second.connectCount.Load())

	err := s.WithRootClient(ctx, func(ctx context.Context, rd rootdRpc.DaemonClient) error {
		st, err := rd.Status(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.Equal(t, "second", st.GetOutboundConfig().GetManagerNamespace())
		return nil
	})
	require.NoError(t, err)
}

type rootDaemonReconnectTestService struct{}

func (rootDaemonReconnectTestService) ListenerAddress() netip.AddrPort {
	return netip.AddrPort{}
}

func (rootDaemonReconnectTestService) SetListenerAddress(netip.AddrPort) {}

func (rootDaemonReconnectTestService) Server() *grpc.Server {
	return nil
}

func (rootDaemonReconnectTestService) ConnectorServer() connectorRpc.ConnectorServer {
	return nil
}

func (rootDaemonReconnectTestService) FuseFTPMgr() remotefs.FuseFTPManager {
	return nil
}

func (rootDaemonReconnectTestService) RootSessionInProcess() bool {
	return false
}

func (rootDaemonReconnectTestService) TeleroutePort() uint16 {
	return 0
}

func (rootDaemonReconnectTestService) LinkedFTP() bool {
	return false
}

func (rootDaemonReconnectTestService) InitFTPServer(context.Context) error {
	return nil
}

type rootDaemonReconnectTestServer struct {
	rootdRpc.UnimplementedDaemonServer

	name        string
	sessionID   string
	activityErr error

	connectCount atomic.Int32
}

func (s *rootDaemonReconnectTestServer) Connect(_ context.Context, nc *rootdRpc.NetworkConfig) (*rootdRpc.DaemonStatus, error) {
	s.connectCount.Add(1)
	if nc.GetSession().GetSessionId() != s.sessionID {
		return nil, status.Errorf(codes.InvalidArgument, "session id = %q, want %q", nc.GetSession().GetSessionId(), s.sessionID)
	}
	return s.daemonStatus(), nil
}

func (s *rootDaemonReconnectTestServer) Status(context.Context, *emptypb.Empty) (*rootdRpc.DaemonStatus, error) {
	return s.daemonStatus(), nil
}

func (s *rootDaemonReconnectTestServer) WaitForNetwork(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *rootDaemonReconnectTestServer) SetDNSTopLevelDomains(context.Context, *rootdRpc.Domains) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *rootDaemonReconnectTestServer) ActivityWatcher(_ *emptypb.Empty, stream grpc.ServerStreamingServer[rootdRpc.Activity]) error {
	if s.activityErr != nil {
		return s.activityErr
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *rootDaemonReconnectTestServer) daemonStatus() *rootdRpc.DaemonStatus {
	return &rootdRpc.DaemonStatus{
		OutboundConfig: &rootdRpc.NetworkConfig{
			ManagerNamespace: s.name,
			Session:          &manager.SessionInfo{SessionId: s.sessionID},
		},
	}
}

func dialTestRootDaemon(server rootdRpc.DaemonServer) (*grpc.ClientConn, func(), error) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	rootdRpc.RegisterDaemonServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	cleanup := func() {
		if conn != nil {
			conn.Close()
		}
		grpcServer.Stop()
		listener.Close()
	}
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return conn, cleanup, nil
}
