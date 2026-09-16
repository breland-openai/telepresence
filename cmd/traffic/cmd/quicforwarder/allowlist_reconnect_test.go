package quicforwarder

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	empty "google.golang.org/protobuf/types/known/emptypb"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
)

// Successful gRPC DNS resolution waits for ResolveNow instead of polling DNS TTLs.
// This builder gives each channel the current headless-Service address and changes
// it only on a new channel or an explicit re-resolution request from gRPC.
type changeableManagerResolver struct {
	scheme      string
	address     atomic.Pointer[string]
	builds      atomic.Int32
	resolveNows atomic.Int32
}

func (r *changeableManagerResolver) set(address string) { r.address.Store(&address) }

func (r *changeableManagerResolver) Scheme() string { return r.scheme }

func (r *changeableManagerResolver) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	r.builds.Add(1)
	watch := &channelManagerResolver{builder: r, cc: cc}
	watch.update()
	return watch, nil
}

type channelManagerResolver struct {
	builder *changeableManagerResolver
	cc      resolver.ClientConn
}

func (r *channelManagerResolver) update() {
	_ = r.cc.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: *r.builder.address.Load()}}})
}

func (r *channelManagerResolver) ResolveNow(resolver.ResolveNowOptions) {
	r.builder.resolveNows.Add(1)
	r.update()
}

func (*channelManagerResolver) Close() {}

type countingManagerListener struct {
	net.Listener
	accepted atomic.Int32
}

func (l *countingManagerListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return conn, err
}

func startMeshWatchManager(t *testing.T, manager rpc.ManagerServer) *countingManagerListener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	counted := &countingManagerListener{Listener: lis}
	server := grpc.NewServer()
	rpc.RegisterManagerServer(server, manager)
	go func() { _ = server.Serve(counted) }()
	t.Cleanup(server.Stop)
	return counted
}

// This real local HTTP/2 server models a healthy transparent mesh connection
// synthesizing Unavailable when its old upstream manager Pod has disappeared.
type meshUnavailableManager struct {
	rpc.UnimplementedManagerServer
	initial *rpc.QuicBackendSnapshot
	fail    chan struct{}
	failed  chan struct{}
	sent    atomic.Bool
	calls   atomic.Int32
}

func (m *meshUnavailableManager) WatchQuicBackends(_ *empty.Empty, stream grpc.ServerStreamingServer[rpc.QuicBackendSnapshot]) error {
	m.calls.Add(1)
	if m.initial != nil && m.sent.CompareAndSwap(false, true) {
		if err := stream.Send(m.initial); err != nil {
			return err
		}
	}
	select {
	case <-stream.Context().Done():
		return nil
	case <-m.fail:
		select {
		case m.failed <- struct{}{}:
		default:
		}
		return status.Error(codes.Unavailable, "transparent mesh cannot reach the previous manager Pod")
	}
}

type replacementQuicManager struct {
	rpc.UnimplementedManagerServer
	snapshot *rpc.QuicBackendSnapshot
	release  chan struct{}
	calls    atomic.Int32
}

func (m *replacementQuicManager) WatchQuicBackends(_ *empty.Empty, stream grpc.ServerStreamingServer[rpc.QuicBackendSnapshot]) error {
	m.calls.Add(1)
	select {
	case <-stream.Context().Done():
		return nil
	case <-m.release:
		if err := stream.Send(m.snapshot); err != nil {
			return err
		}
		<-stream.Context().Done()
		return nil
	}
}

func TestWatchAllowlist_ReresolvesAfterMeshRPCUnavailable(t *testing.T) {
	const timeout = allowlistWatchRetryInterval + 3*time.Second
	oldIP := netip.MustParseAddr("10.1.2.3")
	newIP := netip.MustParseAddr("10.4.5.6")
	managerSnapshot := func(ip netip.Addr) *rpc.QuicBackendSnapshot {
		return &rpc.QuicBackendSnapshot{Backends: []*rpc.QuicBackend{{Ip: ip.AsSlice(), Kind: "manager", Port: 7778}}}
	}
	for _, test := range []struct {
		name    string
		initial bool
	}{
		{name: "initially-unready"},
		{name: "retains-last-snapshot", initial: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			old := &meshUnavailableManager{fail: make(chan struct{}), failed: make(chan struct{}, 1)}
			if test.initial {
				old.initial = managerSnapshot(oldIP)
			}
			oldListener := startMeshWatchManager(t, old)
			replacement := &replacementQuicManager{snapshot: managerSnapshot(newIP), release: make(chan struct{})}
			newListener := startMeshWatchManager(t, replacement)
			address := &changeableManagerResolver{scheme: "forwarder-mesh-" + test.name}
			address.set(oldListener.Addr().String())
			resolver.Register(address)

			allowlist := NewAllowlist(0)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- WatchAllowlist(ctx, address.Scheme()+":///manager", allowlist) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					assert.NoError(t, err)
				case <-time.After(2 * time.Second):
					t.Error("allowlist watch did not shut down")
				}
			})

			require.Eventually(t, func() bool { return old.calls.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
			if test.initial {
				require.Eventually(t, func() bool { return allowlist.Contains(oldIP) }, 2*time.Second, 5*time.Millisecond)
			}
			require.Equal(t, test.initial, allowlist.Ready())
			require.Equal(t, int32(1), oldListener.accepted.Load())

			close(old.fail)
			select {
			case <-old.failed:
			case <-time.After(2 * time.Second):
				t.Fatal("mesh did not synthesize its RPC Unavailable status")
			}
			address.set(newListener.Addr().String())
			if !assert.Eventually(t, func() bool { return replacement.calls.Load() > 0 }, timeout, 5*time.Millisecond) {
				t.Fatalf("replacement manager was never dialed: old RPCs=%d, old TCP sockets=%d, new TCP sockets=%d, resolver builds=%d, transport ResolveNow=%d",
					old.calls.Load(), oldListener.accepted.Load(), newListener.accepted.Load(), address.builds.Load(), address.resolveNows.Load())
			}

			// A successful new connection alone is not a replacement snapshot.
			require.Equal(t, test.initial, allowlist.Ready())
			require.Equal(t, test.initial, allowlist.Contains(oldIP))
			require.False(t, allowlist.Contains(newIP))
			require.Equal(t, int32(1), old.calls.Load())
			require.Equal(t, int32(1), oldListener.accepted.Load())
			require.Equal(t, int32(1), newListener.accepted.Load())
			require.GreaterOrEqual(t, address.builds.Load(), int32(2))

			close(replacement.release)
			require.Eventually(t, func() bool { return allowlist.Contains(newIP) }, 2*time.Second, 5*time.Millisecond)
			require.True(t, allowlist.Ready())
			require.False(t, allowlist.Contains(oldIP))
		})
	}
}
