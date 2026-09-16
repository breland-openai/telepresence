package agentpf

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/telepresenceio/telepresence/rpc/v2/agent"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	tpClient "github.com/telepresenceio/telepresence/v2/pkg/client"
)

func statefulPod(namespace, id, address string, port int32) *manager.AgentPodInfo {
	return &manager.AgentPodInfo{
		PodName:      "database-0",
		Namespace:    namespace,
		PodId:        id,
		PodIp:        netip.MustParseAddr(address).AsSlice(),
		ApiPort:      port,
		QuicSni:      id,
		WorkloadName: "database",
	}
}

func cacheAgent(c *client, a agent.AgentClient) {
	c.Lock()
	c.cli = a
	atomic.StoreInt64(&c.lastActive, time.Now().UnixNano())
	c.Unlock()
}

func TestStatefulAgentSnapshotRetainsBothPhysicalPodsInEitherOrder(t *testing.T) {
	old := statefulPod("alpha", "old-pod", "10.0.0.68", 37689)
	next := statefulPod("alpha", "replacement-pod", "10.0.0.69", 32937)
	for _, tt := range []struct {
		name string
		pods []*manager.AgentPodInfo
	}{
		{name: "replacement last", pods: []*manager.AgentPodInfo{old, next}},
		{name: "old pod last", pods: []*manager.AgentPodInfo{next, old}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestClients(t.Context(), "alpha")
			require.NoError(t, s.updateClients(tt.pods))
			require.Len(t, s.snapshot, 2)
			for _, want := range []*manager.AgentPodInfo{old, next} {
				ip := netip.MustParseAddr(net.IP(want.PodIp).String())
				require.Same(t, want, s.snapshotInfoForIP("alpha", ip))
				provider, ok := s.GetClient(ip).(*client)
				require.True(t, ok)
				require.Same(t, want, provider.info)
				require.Equal(t, want.ApiPort, provider.info.ApiPort)
				require.Equal(t, want.QuicSni, provider.info.QuicSni)
			}
		})
	}
}

func TestStatefulAgentWaitAndRoutingNeverReuseAnotherPhysicalConnection(t *testing.T) {
	s := newTestClients(t.Context(), "alpha")
	old := statefulPod("alpha", "old-pod", "10.0.0.68", 37689)
	next := statefulPod("alpha", "replacement-pod", "10.0.0.69", 32937)
	oldAgent, nextAgent := &workloadLookupAgent{}, &workloadLookupAgent{}
	oldClient := s.loadOrAddClient(old)
	cacheAgent(oldClient, oldAgent)
	nextClient := s.loadOrAddClient(next)
	require.NotSame(t, oldClient, nextClient, "a cached connection belongs to one physical agent")
	cacheAgent(nextClient, nextAgent)
	require.NoError(t, s.updateClients([]*manager.AgentPodInfo{next, old}))

	oldIP, nextIP := netip.MustParseAddr("10.0.0.68"), netip.MustParseAddr("10.0.0.69")
	require.NoError(t, s.WaitForIP(t.Context(), time.Second, "alpha", nextIP))
	require.NoError(t, s.WaitForIP(t.Context(), time.Second, "alpha", oldIP))
	require.Same(t, oldClient, s.GetClient(oldIP))
	require.Same(t, nextClient, s.GetClient(nextIP))
	selected, err := s.GetClient(nextIP).(*client).ensureConnect(t.Context())
	require.NoError(t, err)
	require.Same(t, nextAgent, selected)
	selected, err = s.GetClient(oldIP).(*client).ensureConnect(t.Context())
	require.NoError(t, err)
	require.Same(t, oldAgent, selected)
	require.NoError(t, s.updateClients([]*manager.AgentPodInfo{next}))
	require.Nil(t, s.snapshotInfoForIP("alpha", oldIP))
	require.Same(t, nextClient, s.GetClient(nextIP))
	require.NoError(t, s.WaitForIP(t.Context(), time.Second, "alpha", nextIP))
	selected, err = nextClient.ensureConnect(t.Context())
	require.NoError(t, err)
	require.Same(t, nextAgent, selected)
}

func TestStatefulAgentDeltaRemovalAndResetRetainOnlyCurrentPhysicalPods(t *testing.T) {
	for _, removed := range []string{"agent:old-pod", "agent:replacement-pod"} {
		t.Run(removed, func(t *testing.T) {
			s := newTestClients(t.Context(), "alpha")
			old := statefulPod("alpha", "old-pod", "10.0.0.68", 37689)
			next := statefulPod("alpha", "replacement-pod", "10.0.0.69", 32937)
			other := statefulPod("beta", "other-namespace-pod", "10.0.0.70", 33434)
			entries := map[string]*manager.AgentPodInfo{"agent:old-pod": old, "agent:replacement-pod": next, "agent:other": other}
			require.NoError(t, s.ApplyPodsDelta(true, entries, nil))
			require.Len(t, s.snapshot, 3)
			require.Len(t, s.deltaSnapshot, 3)
			require.Equal(t, 3, s.clients.Size())

			require.NoError(t, s.ApplyPodsDelta(false, nil, []string{removed}))
			for key, pod := range entries {
				ip := netip.MustParseAddr(net.IP(pod.PodIp).String())
				if key == removed {
					require.Nil(t, s.snapshotInfoForIP(pod.Namespace, ip))
					require.Nil(t, s.GetClient(ip))
				} else {
					require.Same(t, pod, s.snapshotInfoForIP(pod.Namespace, ip))
					require.Same(t, pod, s.GetClient(ip).(*client).info)
				}
			}
			require.Nil(t, s.snapshotInfoForIP("alpha", netip.MustParseAddr("10.0.0.70")))
			require.NoError(t, s.ApplyPodsDelta(true, map[string]*manager.AgentPodInfo{"agent:other": other}, nil))
			require.Len(t, s.snapshot, 1)
			require.Equal(t, 1, s.clients.Size())
			require.Same(t, other, s.snapshotInfoForIP("beta", netip.MustParseAddr("10.0.0.70")))
		})
	}
}

func TestStatefulAgentStaleDialRemovalDoesNotDeleteReaddedSameAgent(t *testing.T) {
	s := newTestClients(t.Context(), "alpha")
	pod := statefulPod("alpha", "same-physical-pod", "10.0.0.69", 32937)
	stale := s.loadOrAddClient(pod)
	stale.remove()
	current := s.loadOrAddClient(pod)
	require.NotSame(t, stale, current)
	stale.remove()
	require.NotNil(t, s.GetClient(netip.MustParseAddr("10.0.0.69")), "a callback from the previous dial must leave the replacement client in the map")
	require.Same(t, current, s.GetClient(netip.MustParseAddr("10.0.0.69")))
	current.remove()
	require.Nil(t, s.GetClient(netip.MustParseAddr("10.0.0.69")))
}

func TestStatefulAgentOldManagerIdentityUsesNamespaceAndNormalizedPodIP(t *testing.T) {
	s := newTestClients(t.Context(), "alpha")
	old := statefulPod("alpha", "", "10.0.0.68", 37689)
	next := statefulPod("alpha", "", "10.0.0.69", 32937)
	other := statefulPod("beta", "", "10.0.0.69", 32937)
	oldClient := s.loadOrAddClient(old)
	nextClient := s.loadOrAddClient(next)
	otherClient := s.loadOrAddClient(other)
	require.NotSame(t, oldClient, nextClient)
	require.NotSame(t, nextClient, otherClient)
	mapped := statefulPod("alpha", "", "::ffff:10.0.0.69", 32937)
	require.Same(t, nextClient, s.loadOrAddClient(mapped), "the old-manager fallback must use the normalized address")
	require.NoError(t, s.updateClients([]*manager.AgentPodInfo{old, mapped}))
	ip := netip.MustParseAddr("10.0.0.69")
	require.Same(t, mapped, s.snapshotInfoForIP("alpha", ip))
	require.Same(t, nextClient, s.GetClient(ip))
	wl, ns, ok := s.WorkloadForIP(netip.MustParseAddr("::ffff:10.0.0.68"))
	require.True(t, ok)
	require.Equal(t, "database", wl)
	require.Equal(t, "alpha", ns)
	cacheAgent(nextClient, &workloadLookupAgent{})
	require.NoError(t, s.WaitForIP(t.Context(), time.Second, "alpha", ip))
	known := statefulPod("alpha", "10.0.0.69", "10.0.0.69", 32937)
	require.NotSame(t, nextClient, s.loadOrAddClient(known), "a reported UID is distinct from the old-manager IP fallback")
}

func TestStatefulAgentRoutingConcurrentWithSamePhysicalRefresh(t *testing.T) {
	s := newTestClients(t.Context(), "alpha")
	pod := statefulPod("alpha", "physical-pod", "10.0.0.69", 32937)
	c := s.loadOrAddClient(pod)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			c.refresh(statefulPod("alpha", "physical-pod", "10.0.0.69", 32937))
		}
	}()
	for range 1000 {
		require.Same(t, c, s.GetClient(netip.MustParseAddr("10.0.0.69")))
	}
	<-done
}

func TestStatefulAgentTransportStatusKeepsPublicWorkloadAndPodNames(t *testing.T) {
	s := newTestClients(t.Context(), "alpha")
	old := statefulPod("alpha", "old-pod", "10.0.0.68", 37689)
	next := statefulPod("alpha", "replacement-pod", "10.0.0.69", 32937)
	oldClient, nextClient := s.loadOrAddClient(old), s.loadOrAddClient(next)
	oldClient.transport.Store("grpc")
	nextClient.transport.Store("quic")
	require.ElementsMatch(t, []AgentTransport{
		{Workload: "database", Pod: "database-0", Transport: "grpc"},
		{Workload: "database", Pod: "database-0", Transport: "quic"},
	}, s.Transports())
}

type statefulSnapshotManager struct {
	manager.UnimplementedManagerServer
	pods []*manager.AgentPodInfo
}

func (s *statefulSnapshotManager) WatchAgentPods(_ *manager.SessionInfo, stream grpc.ServerStreamingServer[manager.AgentPodInfoSnapshot]) error {
	if err := stream.Send(&manager.AgentPodInfoSnapshot{Agents: s.pods}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestStatefulAgentLegacyFullWatchPreservesUIDAndOldManagerIPFallback(t *testing.T) {
	for _, tt := range []struct {
		name string
		ids  []string
	}{
		{name: "pod UID", ids: []string{"old-pod", "replacement-pod"}},
		{name: "older manager without UID", ids: []string{"", ""}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			old := statefulPod("alpha", tt.ids[0], "10.0.0.68", 37689)
			next := statefulPod("alpha", tt.ids[1], "10.0.0.69", 32937)
			other := statefulPod("beta", tt.ids[1], "10.0.0.69", 32937)
			listener := bufconn.Listen(1024 * 1024)
			server := grpc.NewServer()
			manager.RegisterManagerServer(server, &statefulSnapshotManager{pods: []*manager.AgentPodInfo{next, old, other}})
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			conn, err := grpc.NewClient("passthrough:///manager", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return listener.DialContext(ctx)
			}), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })

			ctx := tpClient.WithConfig(t.Context(), tpClient.GetDefaultConfig())
			errDone := errors.New("full snapshot captured")
			var resets int
			var narrowed []string
			err = WatchPods(ctx, manager.NewManagerClient(conn), &manager.SessionInfo{SessionId: "client"}, []string{"alpha", "beta"}, "alpha",
				func(upserts map[string]*manager.AgentPodInfo, removals []string) error {
					assert.Len(t, upserts, 3)
					assert.Empty(t, removals)
					s := newTestClients(t.Context(), "alpha")
					require.NoError(t, s.ApplyPodsDelta(true, upserts, removals))
					assert.Equal(t, old.PodIp, s.snapshotInfoForIP("alpha", netip.MustParseAddr("10.0.0.68")).GetPodIp())
					assert.Equal(t, next.PodIp, s.snapshotInfoForIP("alpha", netip.MustParseAddr("10.0.0.69")).GetPodIp())
					assert.Equal(t, other.PodIp, s.snapshotInfoForIP("beta", netip.MustParseAddr("10.0.0.69")).GetPodIp())
					return errDone
				}, func() error {
					resets++
					return nil
				}, func(namespaces []string) { narrowed = namespaces })
			require.ErrorIs(t, err, errDone)
			require.Equal(t, 3, resets)
			require.Equal(t, []string{"alpha"}, narrowed)
		})
	}
}
