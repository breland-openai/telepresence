package trafficmgr

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	connectorRpc "github.com/telepresenceio/telepresence/rpc/v2/connector"
	rootdRpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
	"github.com/telepresenceio/telepresence/v2/pkg/client/remotefs"
)

func TestRootDaemonActivityWatcherReconnectsRootDaemon(t *testing.T) {
	const sessionID = "test-session"

	ctx, cancel := context.WithCancel(client.WithConfig(context.Background(), client.GetDefaultConfig()))
	defer cancel()

	first := &rootDaemonReconnectTestServer{
		name:             "first",
		sessionID:        sessionID,
		activityErr:      status.Error(codes.Unavailable, "rootd exited"),
		supportsCallback: true,
	}
	second := &rootDaemonReconnectTestServer{
		name:             "second",
		sessionID:        sessionID,
		supportsCallback: true,
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
		Namespace:            "default",
		Session:              s.sessionInfo,
		ManagerTokenCallback: &rootdRpc.ManagerTokenCallback{Address: "127.0.0.1:1234", Capability: bytes.Repeat([]byte{'x'}, 32)},
	}

	require.NoError(t, s.connectRootDaemon(ctx, nc, nil, false))
	require.Eventually(t, func() bool {
		return second.connectCount.Load() == 1
	}, time.Second, 10*time.Millisecond)

	require.Equal(t, int32(2), dialCount.Load())
	require.Equal(t, int32(1), first.connectCount.Load())
	require.Equal(t, int32(1), second.connectCount.Load())
	require.True(t, proto.Equal(nc.ManagerTokenCallback, first.callback.Load()))
	require.True(t, proto.Equal(nc.ManagerTokenCallback, second.callback.Load()), "root daemon replacement must retain the same per-user connection callback")
	nc.ManagerTokenCallback.Capability[0] = 'y'
	require.NotEqual(t, nc.ManagerTokenCallback.Capability, second.callback.Load().Capability, "cached IPC must be copied independently")

	err := s.WithRootClient(ctx, func(ctx context.Context, rd rootdRpc.DaemonClient) error {
		st, err := rd.Status(ctx, &emptypb.Empty{})
		require.NoError(t, err)
		require.Equal(t, "second", st.GetOutboundConfig().GetManagerNamespace())
		return nil
	})
	require.NoError(t, err)
}

func TestRootDaemonManagerTokenRejectsOlderDaemonAndReplacesLegacySession(t *testing.T) {
	for _, tc := range []struct {
		name               string
		supports           bool
		negotiated         bool
		supportsNegotiated bool
		legacySession      bool
		previousCredential bool
	}{
		{name: "older daemon"},
		{name: "daemon only supports explicit token", supports: true, negotiated: true},
		{name: "current daemon with existing same-id legacy session", supports: true, legacySession: true},
		{name: "current daemon with same-id previous user's callback", supports: true, legacySession: true, previousCredential: true},
		{name: "current daemon with negotiated credential", supports: true, negotiated: true, supportsNegotiated: true, legacySession: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(client.WithConfig(t.Context(), client.GetDefaultConfig()))
			defer cancel()
			server := &rootDaemonReconnectTestServer{sessionID: "same-session", supportsCallback: tc.supports, supportsNegotiated: tc.supportsNegotiated}
			server.legacySession.Store(tc.legacySession)
			if tc.previousCredential {
				previousID := sha256.Sum256(bytes.Repeat([]byte{'y'}, 32))
				server.callbackID.Store(&previousID)
				server.activeCallback.Store(true)
			}
			conn, cleanup, err := dialTestRootDaemon(server)
			require.NoError(t, err)
			defer func() { cancel(); cleanup() }()
			s := &session{
				Cluster: &k8s.Cluster{Kubeconfig: &k8s.Kubeconfig{Context: ctx, Namespace: "default"}},
				service: rootDaemonReconnectTestService{}, sessionInfo: &manager.SessionInfo{SessionId: "same-session"},
				dialRootDaemon: func(context.Context, bool) (*grpc.ClientConn, error) { return conn, nil },
			}
			nc := &rootdRpc.NetworkConfig{Session: s.sessionInfo, ManagerTokenCallback: &rootdRpc.ManagerTokenCallback{
				Address: "127.0.0.1:1234", Capability: bytes.Repeat([]byte{'x'}, 32), NegotiatedDevboxProxy: tc.negotiated,
			}}
			err = s.connectRootDaemon(ctx, nc, nil, false)
			if !tc.supports || (tc.negotiated && !tc.supportsNegotiated) {
				require.ErrorContains(t, err, "root daemon does not support")
				if tc.negotiated {
					require.ErrorContains(t, err, "negotiated Devbox")
				}
				require.Zero(t, server.connectCount.Load(), "an older daemon must never receive Connect with a required credential")
				require.Zero(t, server.disconnectCount.Load())
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 2, server.connectCount.Load())
				require.EqualValues(t, 1, server.disconnectCount.Load(), "same-ID legacy session must be replaced before use")
				require.True(t, server.activeCallback.Load())
			}
		})
	}
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

	name               string
	sessionID          string
	activityErr        error
	supportsCallback   bool
	supportsNegotiated bool

	connectCount    atomic.Int32
	disconnectCount atomic.Int32
	legacySession   atomic.Bool
	activeCallback  atomic.Bool
	callback        atomic.Pointer[rootdRpc.ManagerTokenCallback]
	callbackID      atomic.Pointer[[sha256.Size]byte]
}

func (s *rootDaemonReconnectTestServer) Connect(_ context.Context, nc *rootdRpc.NetworkConfig) (*rootdRpc.DaemonStatus, error) {
	s.callback.Store(nc.GetManagerTokenCallback())
	s.connectCount.Add(1)
	if nc.GetSession().GetSessionId() != s.sessionID {
		return nil, status.Errorf(codes.InvalidArgument, "session id = %q, want %q", nc.GetSession().GetSessionId(), s.sessionID)
	}
	if !s.legacySession.Load() && nc.GetManagerTokenCallback() != nil {
		id := sha256.Sum256(nc.GetManagerTokenCallback().GetCapability())
		s.callbackID.Store(&id)
		s.activeCallback.Store(true)
	}
	return s.daemonStatus(), nil
}

func (s *rootDaemonReconnectTestServer) Disconnect(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	s.disconnectCount.Add(1)
	s.legacySession.Store(false)
	s.activeCallback.Store(false)
	s.callbackID.Store(nil)
	return &emptypb.Empty{}, nil
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
	var callbackID []byte
	if id := s.callbackID.Load(); id != nil {
		callbackID = id[:]
	}
	return &rootdRpc.DaemonStatus{
		SupportsManagerTokenCallback:  s.supportsCallback,
		SupportsNegotiatedDevboxProxy: s.supportsNegotiated,
		ManagerTokenCallbackActive:    s.activeCallback.Load(),
		ManagerTokenCallbackId:        callbackID,
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
