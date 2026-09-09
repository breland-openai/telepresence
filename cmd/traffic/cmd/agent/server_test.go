package agent

// This file lives in package agent (not agent_test) so the test can reach
// s.(*state).dialWatchers directly: driving the displacement policy end to end requires
// pushing a value through the map's channel and observing which fake server receives it.

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/clog/testutil"
	agentrpc "github.com/telepresenceio/telepresence/rpc/v2/agent"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/sessiontoken"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

type metricsManagerClient struct {
	rpc.ManagerClient
	reports chan *rpc.TunnelMetrics
}

func TestAwaitingForwardAbandonedBeforePeerReturns(t *testing.T) {
	requestCtx, cancel := context.WithCancel(t.Context())
	aw := &awaitingForward{streamCh: make(chan tunnel.Stream), doneCh: requestCtx.Done()}
	cancel()
	done := make(chan error, 1)
	go func() { done <- aw.serve(t.Context(), nil) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("late Tunnel handler blocked sending to the abandoned waiter")
	}
}

func TestAwaitingForwardPeerCanCancelBeforeHandoff(t *testing.T) {
	peerCtx, cancel := context.WithCancel(t.Context())
	aw := &awaitingForward{streamCh: make(chan tunnel.Stream), doneCh: t.Context().Done()}
	done := make(chan error, 1)
	go func() { done <- aw.serve(peerCtx, nil) }()
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled Tunnel handler blocked sending its stream")
	}
}

func TestAwaitingForwardEstablishedTunnelLastsUntilConnectionCloses(t *testing.T) {
	requestCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	aw := &awaitingForward{streamCh: make(chan tunnel.Stream), doneCh: requestCtx.Done()}
	done := make(chan error, 1)
	go func() { done <- aw.serve(t.Context(), nil) }()
	select {
	case <-aw.streamCh:
	case <-time.After(time.Second):
		t.Fatal("Tunnel handler did not deliver its stream")
	}
	select {
	case err := <-done:
		t.Fatalf("Tunnel handler stopped immediately after its stream was accepted: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Tunnel handler remained active after its connection closed")
	}
}

func TestAwaitingForwardLegacyTombstoneExpiresOrCanBeReplaced(t *testing.T) {
	const timeout = 25 * time.Millisecond
	t.Run("unused entry expires", func(t *testing.T) {
		aw := &awaitingForward{}
		removed := make(chan struct{})
		aw.keepForLateResponse(timeout, func() { close(removed) })
		select {
		case <-removed:
		case <-time.After(time.Second):
			t.Fatal("legacy dial response tombstone did not expire")
		}
	})
	t.Run("replacement prevents old cleanup", func(t *testing.T) {
		aw := &awaitingForward{}
		removed := make(chan struct{}, 2)
		aw.keepForLateResponse(timeout, func() { removed <- struct{}{} })
		aw.stopCleanup()
		aw.keepForLateResponse(timeout, func() { removed <- struct{}{} })
		select {
		case <-removed:
			t.Fatal("a replaced legacy tombstone attempted to remove its successor")
		case <-time.After(3 * timeout):
		}
	})
}

func TestCreateClientStreamRejectsConcurrentConnIDAndReplacesAbandonedWaiter(t *testing.T) {
	ctx := testutil.NewContext(t, false)
	cfg := &fakeConfig{sidecar: &agentconfig.Sidecar{}, podIP: netip.MustParseAddr("127.0.0.1")}
	st, err := NewState(ctx, cfg)
	require.NoError(t, err)
	s := st.(*state)
	sid := tunnel.SessionID("developer")
	id := tunnel.NewConnID(types.ProtoTCP, netip.MustParseAddrPort("192.0.2.1:1234"), netip.MustParseAddrPort("127.0.0.1:8080"))
	dials := make(chan *rpc.DialRequest, 2)
	s.dialWatchers.Store(sid, dials)

	firstCtx, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	firstDone := make(chan error, 1)
	go func() {
		_, err := s.CreateClientStream(firstCtx, tunnel.AgentToClient, sid, id, 0, 0)
		firstDone <- err
	}()
	select {
	case <-dials:
	case <-time.After(time.Second):
		t.Fatal("first request did not send a dial")
	}
	awaiting, ok := s.awaitingForwards.Load(sid)
	require.True(t, ok)
	first, ok := awaiting.Load(id)
	require.True(t, ok)
	_, err = s.CreateClientStream(ctx, tunnel.AgentToClient, sid, id, 0, 0)
	require.ErrorContains(t, err, "is already pending")
	select {
	case <-dials:
		t.Fatal("duplicate connection ID sent a second dial request")
	default:
	}
	cancelFirst()
	require.ErrorIs(t, <-firstDone, context.Canceled)
	retained, ok := awaiting.Load(id)
	require.True(t, ok)
	require.Same(t, first, retained, "abandoned sent requests must retain a temporary legacy tombstone")

	secondCtx, cancelSecond := context.WithCancel(ctx)
	defer cancelSecond()
	secondDone := make(chan error, 1)
	go func() {
		_, err := s.CreateClientStream(secondCtx, tunnel.AgentToClient, sid, id, 0, 0)
		secondDone <- err
	}()
	select {
	case <-dials:
	case <-time.After(time.Second):
		t.Fatal("retry did not replace abandoned legacy tombstone")
	}
	second, ok := awaiting.Load(id)
	require.True(t, ok)
	require.NotSame(t, first, second)
	cancelSecond()
	require.ErrorIs(t, <-secondDone, context.Canceled)
	second.stopCleanup()
	awaiting.LoadAndDelete(id)
}

type fakeAgentTunnelServer struct {
	grpc.ServerStream
	ctx      context.Context
	incoming chan *rpc.TunnelMessage
	outgoing chan *rpc.TunnelMessage
}

func (s *fakeAgentTunnelServer) Context() context.Context { return s.ctx }

func (s *fakeAgentTunnelServer) Recv() (*rpc.TunnelMessage, error) {
	select {
	case msg := <-s.incoming:
		return msg, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *fakeAgentTunnelServer) Send(msg *rpc.TunnelMessage) error {
	select {
	case s.outgoing <- msg:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func TestTunnelDropsMarkedAbandonedReplyButPermitsUnmarkedClientDial(t *testing.T) {
	for _, tt := range []struct {
		name   string
		marked bool
	}{
		{name: "marked abandoned response is dropped", marked: true},
		{name: "ordinary legacy client can still dial cluster"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(testutil.NewContext(t, false))
			defer cancel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			accepted := make(chan bool, 1)
			go func() {
				conn, err := listener.Accept()
				accepted <- err == nil
				if err == nil {
					_ = conn.Close()
				}
			}()
			cfg := &fakeConfig{sidecar: &agentconfig.Sidecar{}, podIP: netip.MustParseAddr("127.0.0.1")}
			st, err := NewState(ctx, cfg)
			require.NoError(t, err)
			s := st.(*state)
			server := &fakeAgentTunnelServer{ctx: ctx, incoming: make(chan *rpc.TunnelMessage, 1), outgoing: make(chan *rpc.TunnelMessage, 10)}
			id := tunnel.NewConnID(types.ProtoTCP, netip.MustParseAddrPort("192.0.2.1:1234"), listener.Addr().(*net.TCPAddr).AddrPort())
			msg := tunnel.StreamInfoMessage(id, "developer", 0, time.Second).TunnelMessage()
			if tt.marked {
				// The additive StreamInfo flags byte marks this as a dial-response tunnel.
				msg.Payload = append(msg.Payload, 1)
			}
			server.incoming <- msg
			done := make(chan error, 1)
			go func() { done <- s.Tunnel(server) }()
			if tt.marked {
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("agent did not promptly drop an abandoned marked response")
				}
				_ = listener.Close()
				require.False(t, <-accepted, "an abandoned response must never be used to dial a cluster address")
			} else {
				select {
				case ok := <-accepted:
					require.True(t, ok, "an ordinary client-initiated tunnel must continue to dial the cluster")
				case <-time.After(time.Second):
					t.Fatal("agent failed to dial the cluster for an ordinary legacy client tunnel")
				}
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("ordinary tunnel did not stop after its client disconnected")
				}
			}
		})
	}
}

func (m *metricsManagerClient) ReportMetrics(
	_ context.Context,
	metrics *rpc.TunnelMetrics,
	_ ...grpc.CallOption,
) (*emptypb.Empty, error) {
	m.reports <- metrics
	return &emptypb.Empty{}, nil
}

func TestReportMetricsBeforeManagerConnection(t *testing.T) {
	s := &state{}
	s.ReportMetrics(context.Background(), &rpc.TunnelMetrics{ClientSessionId: "early-session"})
	s.RefreshQuicAgentListener(context.Background(), context.Background())
}

func TestReportMetricsFollowsManagerReconnects(t *testing.T) {
	s := &state{}
	first := &metricsManagerClient{reports: make(chan *rpc.TunnelMetrics, 1)}
	second := &metricsManagerClient{reports: make(chan *rpc.TunnelMetrics, 1)}

	s.SetManager(&rpc.SessionInfo{SessionId: "first-manager"}, first, semver.Version{})
	firstMetrics := &rpc.TunnelMetrics{ClientSessionId: "first-client"}
	s.ReportMetrics(context.Background(), firstMetrics)
	select {
	case reported := <-first.reports:
		require.Same(t, firstMetrics, reported)
	case <-time.After(time.Second):
		t.Fatal("first manager never received metrics")
	}

	s.SetManager(&rpc.SessionInfo{SessionId: "second-manager"}, second, semver.Version{})
	secondMetrics := &rpc.TunnelMetrics{ClientSessionId: "second-client"}
	s.ReportMetrics(context.Background(), secondMetrics)
	select {
	case reported := <-second.reports:
		require.Same(t, secondMetrics, reported)
	case <-time.After(time.Second):
		t.Fatal("second manager never received metrics")
	}
}

func TestManagerStateConcurrentReconnects(t *testing.T) {
	const iterations = 100
	s := &state{}
	manager := &metricsManagerClient{reports: make(chan *rpc.TunnelMetrics, iterations)}
	ctx := context.Background()
	var workers sync.WaitGroup
	workers.Add(2)

	go func() {
		defer workers.Done()
		for range iterations {
			s.SetManager(&rpc.SessionInfo{SessionId: "manager-session"}, manager, semver.Version{})
		}
	}()

	go func() {
		defer workers.Done()
		for range iterations {
			_ = s.ManagerClient()
			_ = s.ManagerVersion()
			_ = s.SessionInfo()
			s.ReportMetrics(ctx, &rpc.TunnelMetrics{ClientSessionId: "client-session"})
		}
	}()

	workers.Wait()
}

// fakeWatchDialServer is a minimal agentrpc.Agent_WatchDialServer: WatchDial only calls
// Context and Send, so embedding a nil grpc.ServerStream satisfies the rest of the
// interface without it ever being invoked.
type fakeWatchDialServer struct {
	grpc.ServerStream
	ctx  context.Context
	sent chan *rpc.DialRequest
}

var _ agentrpc.Agent_WatchDialServer = (*fakeWatchDialServer)(nil)

func newFakeWatchDialServer(ctx context.Context) *fakeWatchDialServer {
	return &fakeWatchDialServer{ctx: ctx, sent: make(chan *rpc.DialRequest, 4)}
}

func (f *fakeWatchDialServer) Context() context.Context { return f.ctx }

func (f *fakeWatchDialServer) Send(dr *rpc.DialRequest) error {
	select {
	case f.sent <- dr:
		return nil
	case <-f.ctx.Done():
		return f.ctx.Err()
	}
}

// TestWatchDial_Displacement drives the real (*state).WatchDial through the
// displacement policy described in "Agent gRPC: tunnel and dial-watcher calls" in
// docs/reference/authentication.md: an unverified caller may register only
// when no watcher is live for the session; a verified caller always displaces whatever
// was there; and a displaced handler's own exit must not remove its successor's
// registration.
func TestWatchDial_Displacement(t *testing.T) {
	ctx := testutil.NewContext(t, false)
	cfg := &fakeConfig{sidecar: &agentconfig.Sidecar{}, podIP: netip.MustParseAddr("127.0.0.1")}
	st, err := NewState(ctx, cfg)
	require.NoError(t, err)
	s := st.(*state)

	key := generateTestKey(t)
	s.fileShareAuth.set(&key.PublicKey, "permissive")
	validToken, err := sessiontoken.Mint(key, "session-1", time.Now().Add(time.Hour))
	require.NoError(t, err)

	session := &rpc.SessionInfo{SessionId: "session-1"}
	sid := tunnel.SessionID(session.SessionId)

	// First call: unverified (no token in its context), registers because nothing is
	// live yet.
	ctx1, cancel1 := context.WithCancel(ctx)
	server1 := newFakeWatchDialServer(ctx1)
	done1 := make(chan error, 1)
	go func() { done1 <- s.WatchDial(session, server1) }()
	require.Eventually(t, func() bool {
		_, ok := s.dialWatchers.Load(sid)
		return ok
	}, time.Second, time.Millisecond, "first watcher never registered")

	// Second call: also unverified, refused because the first watcher is still live.
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	err = s.WatchDial(session, newFakeWatchDialServer(ctx2))
	require.Equal(t, codes.AlreadyExists, status.Code(err))

	// The first watcher is still live: a dial pushed through the map reaches it.
	ch1, ok := s.dialWatchers.Load(sid)
	require.True(t, ok)
	probe1 := &rpc.DialRequest{ConnId: []byte("probe-1")}
	ch1 <- probe1
	select {
	case got := <-server1.sent:
		require.Same(t, probe1, got)
	case <-time.After(time.Second):
		t.Fatal("first watcher never received the probe")
	}

	// Third call: verified (presents a valid token for this session), always displaces
	// whatever is registered.
	mdCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(sessiontoken.MetadataKey, validToken))
	ctx3, cancel3 := context.WithCancel(mdCtx)
	defer cancel3()
	server3 := newFakeWatchDialServer(ctx3)
	done3 := make(chan error, 1)
	go func() { done3 <- s.WatchDial(session, server3) }()

	var ch3 chan *rpc.DialRequest
	require.Eventually(t, func() bool {
		cur, ok := s.dialWatchers.Load(sid)
		if !ok || cur == ch1 {
			return false
		}
		ch3 = cur
		return true
	}, time.Second, time.Millisecond, "verified call never displaced the existing watcher")

	probe2 := &rpc.DialRequest{ConnId: []byte("probe-2")}
	ch3 <- probe2
	select {
	case got := <-server3.sent:
		require.Same(t, probe2, got)
	case <-time.After(time.Second):
		t.Fatal("displaced-in watcher never received the probe")
	}

	// The first handler's exit must not remove the second's registration: cancel its
	// context, wait for its WatchDial call to return, and confirm the map still
	// resolves the displaced-in channel.
	cancel1()
	select {
	case err := <-done1:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("first WatchDial call never returned after its context was cancelled")
	}

	cur, ok := s.dialWatchers.Load(sid)
	require.True(t, ok)
	require.True(t, cur == ch3, "the displaced-in watcher's registration must survive the replaced handler's exit")

	cancel3()
	select {
	case err := <-done3:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("third WatchDial call never returned after its context was cancelled")
	}
}
