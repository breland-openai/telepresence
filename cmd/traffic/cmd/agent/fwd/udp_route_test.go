package fwd

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/forwarder"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

type udpRouteListener struct {
	net.ListenConfig
	bindings atomic.Int32
}

func (l *udpRouteListener) ListenPacket(ctx context.Context, network, address string) (net.PacketConn, error) {
	conn, err := l.ListenConfig.ListenPacket(ctx, network, address)
	if err == nil {
		l.bindings.Add(1)
	}
	return conn, err
}

type udpRouteProvider struct {
	created  atomic.Int32
	received chan string
}

func (p *udpRouteProvider) CreateClientStream(_ context.Context, _ tunnel.Tag, session tunnel.SessionID, id tunnel.ConnID, _, _ time.Duration) (tunnel.Stream, error) {
	p.created.Add(1)
	return &udpRouteStream{provider: p, session: session, id: id, replies: make(chan tunnel.Message, 32)}, nil
}

func (*udpRouteProvider) ReportMetrics(context.Context, *manager.TunnelMetrics) {}
func (*udpRouteProvider) MetricsEnabled() bool                                  { return false }

type udpRouteStream struct {
	provider *udpRouteProvider
	session  tunnel.SessionID
	id       tunnel.ConnID
	replies  chan tunnel.Message
}

func (*udpRouteStream) Tag() tunnel.Tag                 { return tunnel.AgentToClient }
func (s *udpRouteStream) ID() tunnel.ConnID             { return s.id }
func (*udpRouteStream) PeerVersion() uint16             { return tunnel.Version }
func (s *udpRouteStream) SessionID() tunnel.SessionID   { return s.session }
func (*udpRouteStream) DialTimeout() time.Duration      { return time.Second }
func (*udpRouteStream) RoundtripLatency() time.Duration { return 0 }
func (*udpRouteStream) SetTag(tunnel.Tag)               {}
func (*udpRouteStream) CloseSend(context.Context) error { return nil }

func (s *udpRouteStream) Receive(ctx context.Context) (tunnel.Message, error) {
	select {
	case message := <-s.replies:
		return message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *udpRouteStream) Send(ctx context.Context, message tunnel.Message) error {
	if message.Code() != tunnel.Normal {
		return nil
	}
	payload := string(message.Payload())
	select {
	case s.provider.received <- payload:
	case <-ctx.Done():
		return ctx.Err()
	}
	prefix := "client:"
	if s.session == "second-session" {
		prefix = "second-session:"
	} else if s.id.Destination().Port() == 18325 {
		prefix = "second-target:"
	}
	select {
	case s.replies <- tunnel.NewMessage(tunnel.Normal, []byte(prefix+payload)):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func udpRouteIntercept() *manager.InterceptInfo {
	return &manager.InterceptInfo{
		Id:            "udp-intercept",
		ClientSession: &manager.SessionInfo{SessionId: "udp-client"},
		Spec:          &manager.InterceptSpec{Name: "udp", Client: "test-client", TargetHost: "127.0.0.1", TargetPort: 18324},
	}
}

func startUDPRouteAgent(t *testing.T, target netip.AddrPort) (*udp, *udpRouteListener, *udpRouteProvider, netip.AddrPort) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	listener := &udpRouteListener{}
	provider := &udpRouteProvider{received: make(chan string, 32)}
	f := newUDP(ctx, types.PortAndProto{Proto: types.ProtoUDP}, tunnel.AgentToClient, target,
		forwarder.WithListener(listener), forwarder.WithListenAddr(netip.MustParseAddr("127.0.0.1"))).(*udp)
	f.SetStreamProvider(provider)
	ready, done := make(chan netip.AddrPort, 1), make(chan error, 1)
	go func() { done <- f.Serve(ctx, ready) }()
	var address netip.AddrPort
	select {
	case address = <-ready:
		require.True(t, address.IsValid())
	case err := <-done:
		t.Fatalf("UDP interceptor exited before opening its listener: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("UDP interceptor did not open its listener")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Error("UDP interceptor did not stop")
		}
	})
	return f, listener, provider, address
}

func newUDPRouteSocket(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func udpRouteExchange(conn *net.UDPConn, target netip.AddrPort, payload string) (string, error) {
	if _, err := conn.WriteToUDPAddrPort([]byte(payload), target); err != nil {
		return "", err
	}
	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		return "", err
	}
	buf := make([]byte, 128)
	n, _, err := conn.ReadFromUDP(buf)
	return string(buf[:n]), err
}

func requireUDPRouteResponse(t *testing.T, conn *net.UDPConn, target netip.AddrPort, payload, expected string) {
	t.Helper()
	require.Eventually(t, func() bool {
		response, err := udpRouteExchange(conn, target, payload)
		return err == nil && response == expected
	}, 3*time.Second, 20*time.Millisecond)
}

func drainUDPRouteRecords(source chan string) []string {
	var values []string
	for {
		select {
		case value := <-source:
			values = append(values, value)
		default:
			return values
		}
	}
}

func TestUDPRouteChangesAfterListenerStarts(t *testing.T) {
	app := newUDPRouteSocket(t)
	appReceived := make(chan string, 32)
	go func() {
		buf := make([]byte, 128)
		for {
			n, addr, err := app.ReadFromUDP(buf)
			if err != nil {
				return
			}
			payload := string(buf[:n])
			appReceived <- payload
			_, _ = app.WriteToUDP([]byte("app:"+payload), addr)
		}
	}()
	f, listener, provider, address := startUDPRouteAgent(t, app.LocalAddr().(*net.UDPAddr).AddrPort())
	client := newUDPRouteSocket(t)

	requireUDPRouteResponse(t, client, address, "initial", "app:initial")
	require.EqualValues(t, 1, listener.bindings.Load())
	require.Zero(t, provider.created.Load())
	f.SetIntercepting(nil)
	requireUDPRouteResponse(t, client, address, "unchanged-fallback", "app:unchanged-fallback")
	require.EqualValues(t, 1, listener.bindings.Load())

	f.SetIntercepting([]*manager.InterceptInfo{udpRouteIntercept()})
	require.Eventually(t, func() bool { return listener.bindings.Load() == 2 }, 3*time.Second, 10*time.Millisecond)
	requireUDPRouteResponse(t, client, address, "selected", "client:selected")
	require.EqualValues(t, 1, provider.created.Load())

	// A new manager snapshot of the same logical intercept does not disrupt
	// either the UDP listener or this client's existing stream.
	unchanged := udpRouteIntercept()
	unchanged.PodName = "reconnected-pod"
	f.SetIntercepting([]*manager.InterceptInfo{unchanged})
	requireUDPRouteResponse(t, client, address, "unchanged-selected", "client:unchanged-selected")
	require.Never(t, func() bool { return listener.bindings.Load() != 2 }, 150*time.Millisecond, 10*time.Millisecond)
	require.EqualValues(t, 1, provider.created.Load())

	changedTarget := udpRouteIntercept()
	changedTarget.Spec.TargetPort = 18325
	f.SetIntercepting([]*manager.InterceptInfo{changedTarget})
	require.Eventually(t, func() bool { return listener.bindings.Load() == 3 }, 3*time.Second, 10*time.Millisecond)
	requireUDPRouteResponse(t, client, address, "changed-target", "second-target:changed-target")
	require.EqualValues(t, 2, provider.created.Load())

	changedSession := udpRouteIntercept()
	changedSession.Spec.TargetPort = 18325
	changedSession.ClientSession.SessionId = "second-session"
	f.SetIntercepting([]*manager.InterceptInfo{changedSession})
	require.Eventually(t, func() bool { return listener.bindings.Load() == 4 }, 3*time.Second, 10*time.Millisecond)
	requireUDPRouteResponse(t, client, address, "changed-session", "second-session:changed-session")
	require.EqualValues(t, 3, provider.created.Load())

	f.SetIntercepting(nil)
	require.Eventually(t, func() bool { return listener.bindings.Load() == 5 }, 3*time.Second, 10*time.Millisecond)
	requireUDPRouteResponse(t, client, address, "restored", "app:restored")
	require.EqualValues(t, 3, provider.created.Load())

	appMessages := drainUDPRouteRecords(appReceived)
	require.Contains(t, appMessages, "initial")
	require.Contains(t, appMessages, "unchanged-fallback")
	require.Contains(t, appMessages, "restored")
	require.NotContains(t, appMessages, "selected")
	require.NotContains(t, appMessages, "unchanged-selected")
	require.NotContains(t, appMessages, "changed-target")
	require.NotContains(t, appMessages, "changed-session")
	clientMessages := drainUDPRouteRecords(provider.received)
	require.Contains(t, clientMessages, "selected")
	require.Contains(t, clientMessages, "unchanged-selected")
	require.Contains(t, clientMessages, "changed-target")
	require.Contains(t, clientMessages, "changed-session")
	require.NotContains(t, clientMessages, "initial")
	require.NotContains(t, clientMessages, "restored")
}

func TestUDPReplacementWaitsWithoutForwardingOrReopening(t *testing.T) {
	f, listener, provider, address := startUDPRouteAgent(t, netip.AddrPort{})
	client := newUDPRouteSocket(t)

	_, err := udpRouteExchange(client, address, "ignored-before")
	require.Error(t, err)
	require.True(t, tunnel.IsTimeout(err))
	f.SetIntercepting(nil)
	require.Never(t, func() bool { return listener.bindings.Load() != 1 }, 150*time.Millisecond, 10*time.Millisecond)
	require.Zero(t, provider.created.Load())

	f.SetIntercepting([]*manager.InterceptInfo{udpRouteIntercept()})
	require.Eventually(t, func() bool { return listener.bindings.Load() == 2 }, 3*time.Second, 10*time.Millisecond)
	requireUDPRouteResponse(t, client, address, "selected", "client:selected")

	f.SetIntercepting(nil)
	require.Eventually(t, func() bool { return listener.bindings.Load() == 3 }, 3*time.Second, 10*time.Millisecond)
	_, err = udpRouteExchange(client, address, "ignored-after")
	require.Error(t, err)
	require.True(t, tunnel.IsTimeout(err))
	require.Never(t, func() bool { return listener.bindings.Load() != 3 }, 150*time.Millisecond, 10*time.Millisecond)
	require.EqualValues(t, 1, provider.created.Load())
	clientMessages := drainUDPRouteRecords(provider.received)
	require.Contains(t, clientMessages, "selected")
	require.NotContains(t, clientMessages, "ignored-before")
	require.NotContains(t, clientMessages, "ignored-after")
}
