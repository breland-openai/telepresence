package rootd

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/telepresence/rpc/v2/agent"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/agentpf"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

type serviceFallbackClients struct {
	agentpf.Clients
	mu        sync.Mutex
	provider  tunnel.Provider
	generic   []netip.Addr
	workloads []string
}

func (c *serviceFallbackClients) GetClient(ip netip.Addr) tunnel.Provider {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generic = append(c.generic, ip)
	return c.provider
}

func (c *serviceFallbackClients) GetWorkloadClient(workload string) tunnel.Provider {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workloads = append(c.workloads, workload)
	return c.provider
}

func (c *serviceFallbackClients) selected() ([]netip.Addr, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]netip.Addr(nil), c.generic...), append([]string(nil), c.workloads...)
}

type observedServiceAgent struct {
	tunnel.Provider
	mu          sync.Mutex
	contexts    []context.Context
	creationErr []error
}

type terminalBeforeServiceSend struct {
	tunnel.Provider
	sent chan error
}

func (p terminalBeforeServiceSend) Tunnel(ctx context.Context, options ...grpc.CallOption) (tunnel.GRPCClientStream, error) {
	stream, err := p.Provider.Tunnel(ctx, options...)
	if err != nil {
		return nil, err
	}
	return terminalBeforeServiceSendStream{GRPCClientStream: stream, sent: p.sent}, nil
}

type terminalBeforeServiceSendStream struct {
	tunnel.GRPCClientStream
	sent chan error
}

type eofAfterServiceNativeSend struct{ tunnel.Provider }

func (p eofAfterServiceNativeSend) Tunnel(ctx context.Context, options ...grpc.CallOption) (tunnel.GRPCClientStream, error) {
	stream, err := p.Provider.Tunnel(ctx, options...)
	if err != nil {
		return nil, err
	}
	return eofAfterServiceNativeSendStream{stream}, nil
}

type eofAfterServiceNativeSendStream struct{ tunnel.GRPCClientStream }

func (s eofAfterServiceNativeSendStream) Send(message *manager.TunnelMessage) error {
	if err := s.GRPCClientStream.Send(message); err != nil {
		return err
	}
	return io.EOF
}

func (s terminalBeforeServiceSendStream) Send(message *manager.TunnelMessage) error {
	_, _ = s.GRPCClientStream.(grpc.ClientStream).Header()
	err := s.GRPCClientStream.Send(message)
	s.sent <- err
	return err
}

func (p *observedServiceAgent) Tunnel(ctx context.Context, options ...grpc.CallOption) (tunnel.GRPCClientStream, error) {
	p.mu.Lock()
	p.contexts = append(p.contexts, ctx)
	p.mu.Unlock()
	stream, err := p.Provider.Tunnel(ctx, options...)
	p.mu.Lock()
	p.creationErr = append(p.creationErr, err)
	p.mu.Unlock()
	return stream, err
}

func (p *observedServiceAgent) first() (context.Context, []error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.contexts) == 0 {
		return nil, append([]error(nil), p.creationErr...)
	}
	return p.contexts[0], append([]error(nil), p.creationErr...)
}

type serviceNativeOpen struct {
	mu               sync.Mutex
	agent            *observedServiceAgent
	beforeHandshake  func(grpc.BidiStreamingServer[manager.TunnelMessage, manager.TunnelMessage]) error
	requests         []tunnel.ConnID
	sessions         []tunnel.SessionID
	deadlines        []time.Time
	oldAlreadyEnded  []bool
	applicationBytes [][]byte
	failAfterPayload bool
}

func (n *serviceNativeOpen) open(raw grpc.BidiStreamingServer[manager.TunnelMessage, manager.TunnelMessage]) error {
	if n.beforeHandshake != nil {
		return n.beforeHandshake(raw)
	}
	stream, err := tunnel.NewServerStream(raw.Context(), tunnel.ClientToManager, raw)
	if err != nil {
		return err
	}
	deadline, _ := raw.Context().Deadline()
	oldAlreadyEnded := false
	if n.agent != nil {
		oldCtx, _ := n.agent.first()
		oldAlreadyEnded = oldCtx != nil && oldCtx.Err() != nil
	}
	n.mu.Lock()
	n.requests = append(n.requests, stream.ID())
	n.sessions = append(n.sessions, stream.SessionID())
	n.deadlines = append(n.deadlines, deadline)
	n.oldAlreadyEnded = append(n.oldAlreadyEnded, oldAlreadyEnded)
	n.mu.Unlock()
	for {
		message, receiveErr := stream.Receive(raw.Context())
		if receiveErr != nil {
			if errors.Is(receiveErr, io.EOF) || status.Code(receiveErr) == codes.Canceled {
				return nil
			}
			return receiveErr
		}
		if message.Code() == tunnel.Normal {
			n.mu.Lock()
			n.applicationBytes = append(n.applicationBytes, append([]byte(nil), message.Payload()...))
			n.mu.Unlock()
			if n.failAfterPayload {
				return status.Error(codes.Unavailable, "physical agent transport failed after receiving application bytes")
			}
			if err := stream.Send(raw.Context(), tunnel.NewMessage(tunnel.Normal, message.Payload())); err != nil {
				return err
			}
		}
	}
}

func (n *serviceNativeOpen) snapshot() ([]tunnel.ConnID, []tunnel.SessionID, []time.Time, []bool, [][]byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]tunnel.ConnID(nil), n.requests...), append([]tunnel.SessionID(nil), n.sessions...),
		append([]time.Time(nil), n.deadlines...), append([]bool(nil), n.oldAlreadyEnded...), append([][]byte(nil), n.applicationBytes...)
}

type serviceNativeAgent struct {
	agent.UnimplementedAgentServer
	open *serviceNativeOpen
}

func (s *serviceNativeAgent) Version(context.Context, *emptypb.Empty) (*manager.VersionInfo2, error) {
	return &manager.VersionInfo2{}, nil
}

func (s *serviceNativeAgent) Tunnel(raw grpc.BidiStreamingServer[manager.TunnelMessage, manager.TunnelMessage]) error {
	return s.open.open(raw)
}

type serviceNativeManager struct {
	manager.UnimplementedManagerServer
	open *serviceNativeOpen
}

func (s *serviceNativeManager) Tunnel(raw grpc.BidiStreamingServer[manager.TunnelMessage, manager.TunnelMessage]) error {
	return s.open.open(raw)
}

func serviceNativeConn(t *testing.T, name string, register func(*grpc.Server)) (*grpc.ClientConn, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///"+name, grpc.WithContextDialer(
		func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	stop := sync.OnceFunc(func() {
		server.Stop()
		require.NoError(t, listener.Close())
	})
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		stop()
	})
	return conn, stop
}

func newServiceNativeSession(t *testing.T, ag, mgr *serviceNativeOpen) (*session, *serviceFallbackClients, *observedServiceAgent, *grpc.ClientConn, func()) {
	t.Helper()
	ctx := client.WithConfig(t.Context(), client.GetDefaultConfig())
	s := newTestStreamSession(ctx)
	s.serviceSubnets = []netip.Prefix{netip.MustParsePrefix("10.96.0.0/12"), netip.MustParsePrefix("fd00:96::/64")}
	s.podSubnets = []netip.Prefix{netip.MustParsePrefix("10.244.0.0/16"), netip.MustParsePrefix("fd00:244::/64")}
	s.managerConn, _ = serviceNativeConn(t, "service-manager", func(server *grpc.Server) {
		manager.RegisterManagerServer(server, &serviceNativeManager{open: mgr})
	})
	conn, stop := serviceNativeConn(t, "physical-service-agent", func(server *grpc.Server) {
		agent.RegisterAgentServer(server, &serviceNativeAgent{open: ag})
	})
	_, err := agent.NewAgentClient(conn).Version(ctx, &emptypb.Empty{})
	require.NoError(t, err, "genuine bufconn physical agent must be connected initially")
	observed := &observedServiceAgent{Provider: tunnel.AgentProvider(agent.NewAgentClient(conn))}
	mgr.agent = observed
	clients := &serviceFallbackClients{provider: observed}
	s.agentClients = clients
	return s, clients, observed, conn, stop
}

func serviceFallbackID(destination string) tunnel.ConnID {
	return tunnel.NewConnID(types.ProtoTCP, netip.MustParseAddrPort("192.0.2.1:42310"), netip.MustParseAddrPort(destination))
}

func serviceFallbackAgentEarlyError(code codes.Code) *serviceNativeOpen {
	return &serviceNativeOpen{beforeHandshake: func(raw grpc.BidiStreamingServer[manager.TunnelMessage, manager.TunnelMessage]) error {
		if _, err := raw.Recv(); err != nil {
			return err
		}
		return status.Error(code, "physical agent failed before acknowledging StreamInfo")
	}}
}

func requireServiceManagerRoundtrip(t *testing.T, s *session, ctx context.Context, input, effective tunnel.ConnID, manager *serviceNativeOpen) {
	t.Helper()
	stream, err := s.streamCreator()(ctx, input)
	require.NoError(t, err)
	require.Equal(t, effective, stream.ID())
	request := []byte("GET /ordinary-service HTTP/1.1\r\nHost: controlled-service\r\n\r\n")
	require.NoError(t, stream.Send(ctx, tunnel.NewMessage(tunnel.Normal, request)))
	response, err := stream.Receive(ctx)
	require.NoError(t, err)
	require.Equal(t, request, response.Payload())
	ids, sessions, deadlines, stopped, payloads := manager.snapshot()
	require.Equal(t, []tunnel.ConnID{effective}, ids)
	require.Equal(t, []tunnel.SessionID{"test-session"}, sessions)
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.Len(t, deadlines, 1)
	require.WithinDuration(t, deadline, deadlines[0], 100*time.Millisecond)
	require.Equal(t, []bool{true}, stopped, "failed physical stream context must be canceled before opening the manager stream")
	require.Equal(t, [][]byte{request}, payloads)
	require.EqualValues(t, 1, s.outboundTunnels.Load())
	require.Zero(t, s.outboundTunnelErrors.Load())
}

func TestOrdinaryServiceFallsBackWhenPhysicalAgentStreamCannotOpen(t *testing.T) {
	ag, mgr := &serviceNativeOpen{}, &serviceNativeOpen{}
	s, clients, observed, conn, stop := newServiceNativeSession(t, ag, mgr)
	stop()
	ctx, cancel := context.WithTimeout(s, 5*time.Second)
	defer cancel()
	conn.Connect()
	for state := conn.GetState(); state != connectivity.TransientFailure; state = conn.GetState() {
		if state == connectivity.Idle {
			conn.Connect()
		}
		require.True(t, conn.WaitForStateChange(ctx, state), "the retired physical agent must be unavailable before opening a native stream")
	}
	input := serviceFallbackID("246.246.0.4:8080")
	effective := serviceFallbackID("10.96.67.127:8080")
	s.virtualIPs = xsync.NewMap[netip.Addr, agentVIP]()
	s.virtualIPs.Store(input.DestinationAddr(), agentVIP{destinationIP: effective.DestinationAddr()})
	requireServiceManagerRoundtrip(t, s, ctx, input, effective, mgr)
	agentCtx, creationErr := observed.first()
	require.ErrorIs(t, agentCtx.Err(), context.Canceled)
	require.Len(t, creationErr, 1)
	require.Equal(t, codes.Unavailable, status.Code(creationErr[0]), "real generated client failed while opening the gRPC stream")
	generic, workloads := clients.selected()
	require.Equal(t, []netip.Addr{effective.DestinationAddr()}, generic)
	require.Empty(t, workloads)
}

func TestOrdinaryServiceFallsBackWhenRetiringPhysicalAgentDiesBeforeStreamOK(t *testing.T) {
	entered := make(chan struct{})
	ag := &serviceNativeOpen{beforeHandshake: func(raw grpc.BidiStreamingServer[manager.TunnelMessage, manager.TunnelMessage]) error {
		if _, err := raw.Recv(); err != nil {
			return err
		}
		close(entered)
		<-raw.Context().Done()
		return raw.Context().Err()
	}}
	mgr := &serviceNativeOpen{}
	s, _, observed, _, stop := newServiceNativeSession(t, ag, mgr)
	ctx, cancel := context.WithTimeout(s, 5*time.Second)
	defer cancel()
	go func() {
		select {
		case <-entered:
			stop()
		case <-ctx.Done():
		}
	}()
	id := serviceFallbackID("[fd00:96::127]:8080")
	requireServiceManagerRoundtrip(t, s, ctx, id, id, mgr)
	agentCtx, creationErr := observed.first()
	require.ErrorIs(t, agentCtx.Err(), context.Canceled)
	require.Equal(t, []error{nil}, creationErr, "real generated client opened the gRPC stream before the network disappeared during its protocol handshake")
}

func TestOrdinaryServiceEarlyEOFKeepsNativeAgentStatus(t *testing.T) {
	t.Run("clean native EOF before StreamOK recovers", func(t *testing.T) {
		ag := &serviceNativeOpen{beforeHandshake: func(raw grpc.BidiStreamingServer[manager.TunnelMessage, manager.TunnelMessage]) error {
			_, err := raw.Recv()
			return err
		}}
		mgr := &serviceNativeOpen{}
		s, _, _, _, _ := newServiceNativeSession(t, ag, mgr)
		ctx, cancel := context.WithTimeout(s, 5*time.Second)
		defer cancel()
		id := serviceFallbackID("10.96.67.127:8080")
		requireServiceManagerRoundtrip(t, s, ctx, id, id, mgr)
	})

	t.Run("native Send EOF cannot hide typed authorization denial", func(t *testing.T) {
		ag := &serviceNativeOpen{beforeHandshake: func(grpc.BidiStreamingServer[manager.TunnelMessage, manager.TunnelMessage]) error {
			return status.Error(codes.PermissionDenied, "authoritative real agent refusal")
		}}
		mgr := &serviceNativeOpen{}
		s, _, observed, _, _ := newServiceNativeSession(t, ag, mgr)
		sent := make(chan error, 1)
		observed.Provider = terminalBeforeServiceSend{Provider: observed.Provider, sent: sent}
		ctx, cancel := context.WithTimeout(s, 5*time.Second)
		defer cancel()
		stream, err := s.streamCreator()(ctx, serviceFallbackID("10.96.67.127:8080"))
		require.Nil(t, stream)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.ErrorIs(t, <-sent, io.EOF, "genuine generated gRPC send returned only EOF")
		agentCtx, creationErr := observed.first()
		require.ErrorIs(t, agentCtx.Err(), context.Canceled)
		require.Equal(t, []error{nil}, creationErr)
		managerIDs, _, _, _, managerPayload := mgr.snapshot()
		require.Empty(t, managerIDs)
		require.Empty(t, managerPayload)
	})

	t.Run("a genuine agent acknowledgment behind Send EOF never retries", func(t *testing.T) {
		ag := &serviceNativeOpen{}
		mgr := &serviceNativeOpen{}
		s, _, observed, _, _ := newServiceNativeSession(t, ag, mgr)
		observed.Provider = eofAfterServiceNativeSend{observed.Provider}
		ctx, cancel := context.WithTimeout(s, 5*time.Second)
		defer cancel()
		id := serviceFallbackID("10.96.67.127:8080")
		stream, err := s.streamCreator()(ctx, id)
		require.Nil(t, stream)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Eventually(t, func() bool {
			ids, _, _, _, _ := ag.snapshot()
			return len(ids) == 1
		}, time.Second, 10*time.Millisecond)
		agentIDs, sessions, _, _, agentPayload := ag.snapshot()
		require.Equal(t, []tunnel.ConnID{id}, agentIDs, "the real bufconn agent read and confirmed the original stream")
		require.Equal(t, []tunnel.SessionID{"test-session"}, sessions)
		require.Empty(t, agentPayload)
		managerIDs, _, _, _, managerPayload := mgr.snapshot()
		require.Empty(t, managerIDs)
		require.Empty(t, managerPayload)
	})
}

func TestOrdinaryServiceTunnelFallbackPreservesMandatoryRoutesAndErrors(t *testing.T) {
	for _, test := range []struct {
		name          string
		destination   string
		code          codes.Code
		namedWorkload bool
		overlap       bool
	}{
		{name: "direct and headless Pod IP", destination: "10.244.0.114:8080", code: codes.Unavailable},
		{name: "IPv6 headless Pod IP", destination: "[fd00:244::114]:8080", code: codes.Unavailable},
		{name: "external optional agent is unchanged", destination: "203.0.113.8:8080", code: codes.Unavailable},
		{name: "mandatory named proxy agent for Service", destination: "10.96.67.127:8080", code: codes.Unavailable, namedWorkload: true},
		{name: "ambiguous Service subnet containing a known Pod", destination: "10.244.0.114:8080", code: codes.Unavailable, overlap: true},
		{name: "authoritative authentication failure", destination: "10.96.67.127:8080", code: codes.Unauthenticated},
		{name: "authoritative authorization failure", destination: "10.96.67.127:8080", code: codes.PermissionDenied},
		{name: "endpoint logical error", destination: "10.96.67.127:8080", code: codes.InvalidArgument},
	} {
		t.Run(test.name, func(t *testing.T) {
			mgr := &serviceNativeOpen{}
			s, clients, _, _, _ := newServiceNativeSession(t, serviceFallbackAgentEarlyError(test.code), mgr)
			if test.overlap {
				s.serviceSubnets = append(s.serviceSubnets, netip.MustParsePrefix("10.244.0.0/16"))
			}
			id := serviceFallbackID(test.destination)
			if test.namedWorkload {
				original := id
				id = serviceFallbackID("246.246.0.4:8080")
				s.virtualIPs = xsync.NewMap[netip.Addr, agentVIP]()
				s.virtualIPs.Store(id.DestinationAddr(), agentVIP{workload: "mandatory-proxy", destinationIP: original.DestinationAddr()})
			}
			ctx, cancel := context.WithTimeout(s, 5*time.Second)
			defer cancel()
			stream, err := s.streamCreator()(ctx, id)
			require.Nil(t, stream)
			require.Equal(t, test.code, status.Code(err))
			ids, _, _, _, _ := mgr.snapshot()
			require.Empty(t, ids)
			generic, workloads := clients.selected()
			if test.namedWorkload {
				require.Empty(t, generic)
				require.Equal(t, []string{"mandatory-proxy"}, workloads)
			} else {
				require.Equal(t, []netip.Addr{id.DestinationAddr()}, generic)
				require.Empty(t, workloads)
			}
			require.Zero(t, s.outboundTunnels.Load())
			require.EqualValues(t, 1, s.outboundTunnelErrors.Load())
		})
	}
}

func TestOrdinaryServiceTunnelKeepsExplicitManagerAndDoesNotReplayEstablishedAgent(t *testing.T) {
	t.Run("also-proxy uses manager without choosing an agent", func(t *testing.T) {
		mgr := &serviceNativeOpen{}
		s, clients, _, _, _ := newServiceNativeSession(t, serviceFallbackAgentEarlyError(codes.Unavailable), mgr)
		s.alsoProxySubnets = []netip.Prefix{netip.MustParsePrefix("10.96.0.0/12")}
		ctx, cancel := context.WithTimeout(s, 5*time.Second)
		defer cancel()
		id := serviceFallbackID("10.96.67.127:8080")
		stream, err := s.streamCreator()(ctx, id)
		require.NoError(t, err)
		require.NoError(t, stream.Send(ctx, tunnel.NewMessage(tunnel.Normal, []byte("manager"))))
		_, err = stream.Receive(ctx)
		require.NoError(t, err)
		ids, _, _, _, payloads := mgr.snapshot()
		require.Equal(t, []tunnel.ConnID{id}, ids)
		require.Equal(t, [][]byte{[]byte("manager")}, payloads)
		generic, workloads := clients.selected()
		require.Empty(t, generic)
		require.Empty(t, workloads)
	})

	t.Run("confirmed stream never retries even before application bytes", func(t *testing.T) {
		ag := &serviceNativeOpen{beforeHandshake: func(raw grpc.BidiStreamingServer[manager.TunnelMessage, manager.TunnelMessage]) error {
			if _, err := tunnel.NewServerStream(raw.Context(), tunnel.ClientToAgent, raw); err != nil {
				return err
			}
			return status.Error(codes.Unavailable, "physical agent ended after confirming its protocol stream")
		}}
		mgr := &serviceNativeOpen{}
		s, _, _, _, _ := newServiceNativeSession(t, ag, mgr)
		ctx, cancel := context.WithTimeout(s, 5*time.Second)
		defer cancel()
		id := serviceFallbackID("10.96.67.127:8080")
		stream, err := s.streamCreator()(ctx, id)
		require.NoError(t, err)
		_, err = stream.Receive(ctx)
		require.Equal(t, codes.Unavailable, status.Code(err))
		managerIDs, _, _, _, managerPayload := mgr.snapshot()
		require.Empty(t, managerIDs)
		require.Empty(t, managerPayload)
	})

	t.Run("confirmed stream never replays application bytes", func(t *testing.T) {
		ag, mgr := &serviceNativeOpen{failAfterPayload: true}, &serviceNativeOpen{}
		s, _, _, _, _ := newServiceNativeSession(t, ag, mgr)
		ctx, cancel := context.WithTimeout(s, 5*time.Second)
		defer cancel()
		id := serviceFallbackID("10.96.67.127:8080")
		stream, err := s.streamCreator()(ctx, id)
		require.NoError(t, err)
		payload := []byte("POST /must-never-replay HTTP/1.1\r\n\r\n")
		require.NoError(t, stream.Send(ctx, tunnel.NewMessage(tunnel.Normal, payload)))
		_, err = stream.Receive(ctx)
		require.Equal(t, codes.Unavailable, status.Code(err))
		agentIDs, _, _, _, sent := ag.snapshot()
		require.Equal(t, []tunnel.ConnID{id}, agentIDs)
		require.Equal(t, [][]byte{payload}, sent)
		managerIDs, _, _, _, managerPayload := mgr.snapshot()
		require.Empty(t, managerIDs)
		require.Empty(t, managerPayload)
	})
}

func TestOrdinaryServiceDoesNotFallbackWhenCallerCancelsEarlyAgentStream(t *testing.T) {
	entered := make(chan struct{})
	ag := &serviceNativeOpen{beforeHandshake: func(raw grpc.BidiStreamingServer[manager.TunnelMessage, manager.TunnelMessage]) error {
		if _, err := raw.Recv(); err != nil {
			return err
		}
		close(entered)
		<-raw.Context().Done()
		return status.Error(codes.Unavailable, "agent ended after caller cancellation")
	}}
	mgr := &serviceNativeOpen{}
	s, _, _, _, _ := newServiceNativeSession(t, ag, mgr)
	ctx, cancel := context.WithTimeout(s, 5*time.Second)
	defer cancel()
	go func() {
		select {
		case <-entered:
			cancel()
		case <-ctx.Done():
		}
	}()
	stream, err := s.streamCreator()(ctx, serviceFallbackID("10.96.67.127:8080"))
	require.Nil(t, stream)
	require.Equal(t, codes.Canceled, status.Code(err))
	managerIDs, _, _, _, managerPayload := mgr.snapshot()
	require.Empty(t, managerIDs)
	require.Empty(t, managerPayload)
}
