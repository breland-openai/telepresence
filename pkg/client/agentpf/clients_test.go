package agentpf

import (
	"context"
	"fmt"
	"net/netip"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/telepresenceio/telepresence/rpc/v2/agent"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	tpClient "github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

type workloadReadyTestClient struct{ agent.AgentClient }

func TestWaitForWorkloadFirstCallerWaitsForRealAgentDelta(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newTestClients(tpClient.WithConfig(t.Context(), tpClient.GetDefaultConfig()), "alpha")
		first, second := make(chan error, 1), make(chan error, 1)
		go func() { first <- cs.WaitForWorkload(t.Context(), time.Minute, "missing") }()
		synctest.Wait()
		waiter, ok := cs.wlWaiters.Load("missing")
		require.True(t, ok)
		require.NotNil(t, waiter)
		select {
		case err := <-first:
			t.Fatalf("first wait for an absent workload returned before its agent delta: %v", err)
		default:
		}
		go func() { second <- cs.WaitForWorkload(t.Context(), time.Minute, "missing") }()
		synctest.Wait()
		select {
		case err := <-second:
			t.Fatalf("second wait for an absent workload returned before its agent delta: %v", err)
		default:
		}
		info := podInfo("missing", "alpha", "10.0.0.1")
		cs.clients.Store("missing.alpha", &client{Cluster: cs.Cluster, info: info, cli: &workloadReadyTestClient{}})
		require.NoError(t, cs.ApplyPodsDelta(false, map[string]*manager.AgentPodInfo{"missing.alpha": info}, nil))
		synctest.Wait()
		require.NoError(t, <-first)
		require.NoError(t, <-second)
		require.NoError(t, cs.WaitForWorkload(t.Context(), time.Minute, "missing"))
	})
}

func TestWaitForWorkloadCancellationDoesNotCancelOtherCallersOrSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		persistent, stopSession := context.WithCancel(tpClient.WithConfig(t.Context(), tpClient.GetDefaultConfig()))
		defer stopSession()
		cs := newTestClients(persistent, "alpha")
		firstCtx, stopFirst := context.WithCancel(t.Context())
		defer stopFirst()
		secondCtx, stopSecond := context.WithCancel(t.Context())
		defer stopSecond()
		first, second := make(chan error, 1), make(chan error, 1)
		go func() { first <- cs.WaitForWorkload(firstCtx, time.Minute, "missing") }()
		go func() { second <- cs.WaitForWorkload(secondCtx, time.Minute, "missing") }()
		synctest.Wait()
		stopFirst()
		synctest.Wait()
		require.ErrorIs(t, <-first, context.Canceled)
		require.NoError(t, persistent.Err())
		select {
		case err := <-second:
			t.Fatalf("another caller's cancellation ended the workload waiter: %v", err)
		default:
		}
		stopSecond()
		synctest.Wait()
		require.ErrorIs(t, <-second, context.Canceled)
		require.NoError(t, persistent.Err())
	})
}

func TestWaitForWorkloadFirstCallerUsesConfiguredDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newTestClients(tpClient.WithConfig(t.Context(), tpClient.GetDefaultConfig()), "alpha")
		start := time.Now()
		err := cs.WaitForWorkload(t.Context(), 20*time.Millisecond, "missing")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 20*time.Millisecond, time.Since(start))
	})
}

func TestWaitForWorkloadSameNameInOtherMappedNamespaceCannotRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cs := newTestClients(tpClient.WithConfig(t.Context(), tpClient.GetDefaultConfig()), "alpha")
		cs.setNamespaces([]string{"alpha", "other"})
		cs.SetProxyVia("same")
		result := make(chan error, 1)
		go func() { result <- cs.WaitForWorkload(t.Context(), time.Minute, "same") }()
		synctest.Wait()
		otherInfo := podInfo("same", "other", "10.0.0.1")
		cs.clients.Store("same.other", &client{Cluster: cs.Cluster, info: otherInfo, cli: &workloadReadyTestClient{}})
		require.NoError(t, cs.ApplyPodsDelta(false, map[string]*manager.AgentPodInfo{"same.other": otherInfo}, nil))
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("connected matching workload in a different namespace released primary readiness: %v", err)
		default:
		}
		_, waiterStillStored := cs.wlWaiters.Load("same")
		require.True(t, waiterStillStored)
		require.False(t, cs.isProxyVIA(otherInfo))
		require.False(t, cs.hasWaiterFor(otherInfo))
		primaryInfo := podInfo("same", "alpha", "10.0.0.2")
		cs.clients.Store("same.alpha", &client{Cluster: cs.Cluster, info: primaryInfo, cli: &workloadReadyTestClient{}})
		require.NoError(t, cs.ApplyPodsDelta(false, map[string]*manager.AgentPodInfo{"same.alpha": primaryInfo}, nil))
		synctest.Wait()
		require.NoError(t, <-result)
		require.True(t, cs.isProxyVIA(primaryInfo))
		require.NotNil(t, cs.GetWorkloadClient("same"))
	})
}

func TestWaitForWorkloadRetriesFailedNativeDialWithoutAnotherAgentDelta(t *testing.T) {
	config := tpClient.GetDefaultConfig()
	config.Timeouts().PrivateTrafficAgentConnect = 30 * time.Millisecond
	ctx := k8sapi.WithK8sInterface(tpClient.WithConfig(t.Context(), config), fake.NewClientset())
	cs := newTestClients(ctx, "alpha")
	cs.SetProxyVia("same")
	info := podInfo("same", "alpha", "10.0.0.1")
	info.PodId, info.ApiPort = "native-dial", 9900
	require.NoError(t, cs.ApplyPodsDelta(false, map[string]*manager.AgentPodInfo{"same.alpha": info}, nil))
	_, exists := cs.clients.Load("same.alpha")
	require.True(t, exists)
	result := make(chan error, 1)
	go func() { result <- cs.WaitForWorkload(ctx, time.Second, "same") }()
	require.Eventually(t, func() bool {
		_, stillPresent := cs.clients.Load("same.alpha")
		return !stillPresent
	}, time.Second, 5*time.Millisecond, "native gRPC route failure must remove its failed live client")
	select {
	case err := <-result:
		t.Fatalf("a registered agent with a failed native route marked workload readiness: %v", err)
	default:
	}
	_, _, snapshotStillPresent := cs.WorkloadForIP(netip.MustParseAddr("10.0.0.1"))
	require.True(t, snapshotStillPresent, "the only manager delta stays authoritative after the dial failure")
	cs.clients.Store("same.alpha", &client{
		Cluster: cs.Cluster, owner: cs, info: info, cli: &workloadReadyTestClient{}, remove: func() { cs.clients.Delete("same.alpha") },
	})
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("the periodic workload retry did not use the same manager snapshot once the agent became reachable")
	}
	require.NoError(t, ctx.Err())
}

func TestWaitForWorkloadRegisteredButUnreachableNativeAgentExhaustsDeadline(t *testing.T) {
	config := tpClient.GetDefaultConfig()
	config.Timeouts().PrivateTrafficAgentConnect = 20 * time.Millisecond
	ctx := k8sapi.WithK8sInterface(tpClient.WithConfig(t.Context(), config), fake.NewClientset())
	cs := newTestClients(ctx, "alpha")
	info := podInfo("same", "alpha", "10.0.0.1")
	info.PodId, info.ApiPort = "native-dial", 9900
	require.NoError(t, cs.ApplyPodsDelta(false, map[string]*manager.AgentPodInfo{"same.alpha": info}, nil))
	start := time.Now()
	err := cs.WaitForWorkload(ctx, 100*time.Millisecond, "same")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
	require.NoError(t, ctx.Err())
}

func TestWaitForWorkloadDisabledByAgentMetadataCannotReportReady(t *testing.T) {
	ctx := tpClient.WithConfig(t.Context(), tpClient.GetDefaultConfig())
	cs := newTestClients(ctx, "alpha")
	old := &manager.AgentPodInfo{Namespace: "alpha", WorkloadName: "same"}
	require.NoError(t, cs.ApplyPodsDelta(false, map[string]*manager.AgentPodInfo{"same.alpha": old}, nil))
	err := cs.WaitForWorkload(ctx, time.Second, "same")
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, "agent port-forwards are unavailable in namespace alpha")
}

func TestWaitForWorkloadHealthyReplicaDoesNotWaitBehindUnreachableReplica(t *testing.T) {
	config := tpClient.GetDefaultConfig()
	config.Timeouts().PrivateTrafficAgentConnect = 500 * time.Millisecond
	ctx := k8sapi.WithK8sInterface(tpClient.WithConfig(t.Context(), config), fake.NewClientset())
	cs := newTestClients(ctx, "alpha")
	cs.SetProxyVia("same")
	stale := podInfo("a-stale", "alpha", "10.0.0.1")
	stale.PodId, stale.ApiPort, stale.WorkloadName = "stale-native-dial", 9900, "same"
	healthy := podInfo("z-healthy", "alpha", "10.0.0.2")
	healthy.PodId, healthy.ApiPort, healthy.WorkloadName = "healthy-native-dial", 9900, "same"
	healthyLive := &client{
		Cluster: cs.Cluster, owner: cs, info: healthy, cli: &workloadReadyTestClient{}, remove: func() { cs.clients.Delete("z-healthy.alpha") },
	}
	cs.clients.Store("z-healthy.alpha", healthyLive)
	require.NoError(t, cs.ApplyPodsDelta(false, map[string]*manager.AgentPodInfo{"a-stale.alpha": stale, "z-healthy.alpha": healthy}, nil))
	start := time.Now()
	require.NoError(t, cs.WaitForWorkload(ctx, 250*time.Millisecond, "same"))
	require.Less(t, time.Since(start), 250*time.Millisecond)
	for range 10 {
		require.Same(t, healthyLive, cs.GetWorkloadClient("same"), "the real outbound proxy must select the replica startup proved reachable")
	}
}

func TestGetWorkloadClientPrefersReachablePrimaryReplicaAndExcludesNodeAgents(t *testing.T) {
	cs := newTestClients(t.Context(), "alpha")
	addWorkloadLookupClient(t, cs, "first-unreachable", "alpha", "target", false, false)
	addWorkloadLookupClient(t, cs, "first-outside", "other", "target", false, true)
	addWorkloadLookupClient(t, cs, "first-node", "alpha", "target", true, true)
	later, _ := addWorkloadLookupClient(t, cs, "later", "alpha", "target", false, true)
	last, _ := addWorkloadLookupClient(t, cs, "last", "alpha", "target", false, true)
	for range 10 {
		require.Same(t, last, cs.GetWorkloadClient("target"))
	}
	cs.clients.Delete("last.alpha")
	require.Same(t, later, cs.GetWorkloadClient("target"))
	cs.clients.Delete("later.alpha")
	unreachable, exists := cs.clients.Load("first-unreachable.alpha")
	require.True(t, exists)
	require.Same(t, unreachable, cs.GetWorkloadClient("target"))
	cs.clients.Delete("first-unreachable.alpha")
	require.Nil(t, cs.GetWorkloadClient("target"))
}

func TestWaitForIPUnavailableForUnwatchedNamespace(t *testing.T) {
	cl := &k8s.Cluster{
		Kubeconfig: &k8s.Kubeconfig{
			Context:   context.Background(),
			Namespace: "alpha",
		},
	}
	cs := NewClients(cl, &manager.SessionInfo{SessionId: "session"}, []string{"alpha"}, nil)

	err := cs.WaitForIP(context.Background(), time.Millisecond, "beta", netip.MustParseAddr("10.0.0.1"))
	require.Equal(t, codes.Unavailable, status.Code(err))
}

// TestClients_PreferredQuicAddr proves the default (no callback installed, or a callback
// that hasn't got a winner yet) is "" -- quicEndpointFor's documented fallback to the
// descriptor's own host/port -- and that SetPreferredQuicAddr's callback is consulted on
// every call, not just cached from installation time.
func TestClients_PreferredQuicAddr(t *testing.T) {
	cl := &k8s.Cluster{
		Kubeconfig: &k8s.Kubeconfig{
			Context:   context.Background(),
			Namespace: "alpha",
		},
	}
	cs := NewClients(cl, &manager.SessionInfo{SessionId: "session"}, []string{"alpha"}, nil)
	css, ok := cs.(*clients)
	require.True(t, ok)

	require.Empty(t, css.preferredQuicAddr(), "no callback installed yet")

	winner := ""
	css.SetPreferredQuicAddr(func() string { return winner })
	require.Empty(t, css.preferredQuicAddr(), "callback installed but has no winner yet")

	winner = "198.51.100.5:31778"
	require.Equal(t, winner, css.preferredQuicAddr())

	css.SetPreferredQuicAddr(nil)
	require.Empty(t, css.preferredQuicAddr(), "nil callback reverts to the descriptor fallback")
}

// TestTransportsConcurrentWithRefresh pins the locking contract between Transports
// (which snapshots ac.info under ac.RLock) and refresh (which replaces ac.info under
// ac.Lock). The two race in production -- a status RPC polling Transports while the
// agent watch delivers updates -- so this must be run with -race to catch a
// reintroduction: without the race detector it passes even with the locks removed.
func TestTransportsConcurrentWithRefresh(t *testing.T) {
	cl := &k8s.Cluster{
		Kubeconfig: &k8s.Kubeconfig{
			Context:   context.Background(),
			Namespace: "alpha",
		},
	}
	session := &manager.SessionInfo{SessionId: "session"}
	cs := NewClients(cl, session, []string{"alpha"}, nil)
	css, ok := cs.(*clients)
	require.True(t, ok)

	ac := &client{
		Cluster: cl,
		session: session,
		owner:   css,
		info: &manager.AgentPodInfo{
			PodName:      "echo-1",
			Namespace:    "alpha",
			WorkloadName: "echo",
		},
	}
	ac.transport.Store("quic")
	css.clients.Store("echo-1.alpha", ac)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 1000 {
			ac.refresh(&manager.AgentPodInfo{
				PodName:      fmt.Sprintf("echo-%d", i),
				Namespace:    "alpha",
				WorkloadName: "echo",
			})
		}
	}()
	for range 1000 {
		require.Len(t, cs.Transports(), 1)
	}
	<-done
}

func TestGetRandomAgentSkipsNodeAgent(t *testing.T) {
	cl := &k8s.Cluster{
		Kubeconfig: &k8s.Kubeconfig{
			Context:   context.Background(),
			Namespace: "alpha",
		},
	}
	session := &manager.SessionInfo{SessionId: "session"}
	cs := NewClients(cl, session, []string{"alpha"}, nil)
	css, ok := cs.(*clients)
	require.True(t, ok)

	ai := &manager.AgentPodInfo{
		PodName:   "agent-node",
		Namespace: "ambassador",
		NodeAgent: true,
	}
	css.clients.Store("agent-node.ambassador", &client{
		Cluster: cl,
		session: session,
		owner:   css,
		info:    ai,
	})

	require.Nil(t, cs.GetRandomAgent(context.Background()))
}

// TestGetClientRelayRequiresConnection asserts that an unconnected agent is offered for
// its own pod address but not as a relay for anything else. Relaying is interchangeable
// with the traffic-manager tunnel the caller falls back to, so an agent that has not been
// reached must not be preferred over it.
func TestGetClientRelayRequiresConnection(t *testing.T) {
	cl := &k8s.Cluster{
		Kubeconfig: &k8s.Kubeconfig{
			Context:   context.Background(),
			Namespace: "alpha",
		},
	}
	session := &manager.SessionInfo{SessionId: "session"}
	cs := NewClients(cl, session, []string{"alpha"}, nil)
	css, ok := cs.(*clients)
	require.True(t, ok)

	podIP := netip.MustParseAddr("10.244.0.7")
	css.clients.Store("agent-alpha.alpha", &client{
		Cluster: cl,
		session: session,
		owner:   css,
		remove:  func() { css.clients.Delete("agent-alpha.alpha") },
		info: &manager.AgentPodInfo{
			PodName:   "agent-alpha",
			Namespace: "alpha",
			PodIp:     podIP.AsSlice(),
		},
	})

	// The agent's own pod address still selects it, connected or not.
	require.NotNil(t, cs.GetClient(podIP))

	// A service ClusterIP matches no pod, so the agent would only be a relay.
	require.Nil(t, cs.GetClient(netip.MustParseAddr("10.96.230.123")))
}

// TestGetRandomAgentDoesNotDial asserts that an agent nothing has connected yet is
// skipped rather than dialled. The caller is a name lookup whose deadline is shorter
// than a dial's own timeout, so dialling here would spend the lookup's entire budget
// before it could fall back to the traffic-manager.
func TestGetRandomAgentDoesNotDial(t *testing.T) {
	ctx := tpClient.WithConfig(t.Context(), tpClient.GetDefaultConfig())
	ctx = k8sapi.WithK8sInterface(ctx, fake.NewClientset())
	cl := &k8s.Cluster{
		Kubeconfig: &k8s.Kubeconfig{
			Context:   ctx,
			Namespace: "alpha",
		},
	}
	session := &manager.SessionInfo{SessionId: "session"}
	cs := NewClients(cl, session, []string{"alpha"}, nil)
	css, ok := cs.(*clients)
	require.True(t, ok)

	ac := &client{
		Cluster: cl,
		session: session,
		owner:   css,
		remove:  func() { css.clients.Delete("agent-alpha.alpha") },
		info: &manager.AgentPodInfo{
			PodName:   "agent-alpha",
			Namespace: "alpha",
		},
	}
	css.clients.Store("agent-alpha.alpha", ac)

	require.Nil(t, cs.GetRandomAgent(ctx))

	// A successful dial sets cli; a failed dial removes a non-intercepted client.
	ac.RLock()
	defer ac.RUnlock()
	require.Nil(t, ac.cli)
	stored, exists := css.clients.Load("agent-alpha.alpha")
	require.True(t, exists)
	require.Same(t, ac, stored)
}

type workloadLookupAgent struct {
	agent.AgentClient
}

func addWorkloadLookupClient(
	t *testing.T, cs *clients, pod, namespace, workload string, nodeAgent, connected bool,
) (*client, agent.AgentClient) {
	t.Helper()
	key := pod + "." + namespace
	var lookupAgent agent.AgentClient
	if connected {
		lookupAgent = &workloadLookupAgent{}
	}
	ac := &client{
		Cluster: cs.Cluster,
		session: cs.session,
		owner:   cs,
		remove:  func() { cs.clients.Delete(key) },
		cli:     lookupAgent,
		info: &manager.AgentPodInfo{
			PodName:      pod,
			PodId:        pod + "-id",
			Namespace:    namespace,
			WorkloadName: workload,
			NodeAgent:    nodeAgent,
			ApiPort:      9900,
		},
	}
	cs.clients.Store(key, ac)
	return ac, lookupAgent
}

func TestGetAgentForWorkloadSelectsRequestedWorkload(t *testing.T) {
	cs := newTestClients(t.Context(), "alpha")
	addWorkloadLookupClient(t, cs, "frontend-0", "alpha", "frontend", false, true)
	target, targetAgent := addWorkloadLookupClient(t, cs, "target-0", "alpha", "target", false, true)

	require.Same(t, targetAgent, cs.GetAgentForWorkload(t.Context(), "target"))
	require.Positive(t, atomic.LoadInt64(&target.lastActive))
}

func TestGetAgentForWorkloadSelectsDeterministicPod(t *testing.T) {
	cs := newTestClients(t.Context(), "alpha")
	addWorkloadLookupClient(t, cs, "target-2", "alpha", "target", false, true)
	_, firstAgent := addWorkloadLookupClient(t, cs, "target-0", "alpha", "target", false, true)
	addWorkloadLookupClient(t, cs, "target-1", "alpha", "target", false, true)

	for range 20 {
		require.Same(t, firstAgent, cs.GetAgentForWorkload(t.Context(), "target"))
	}
}

func TestGetAgentForWorkloadPrefersConnectedPod(t *testing.T) {
	cs := newTestClients(t.Context(), "alpha")
	addWorkloadLookupClient(t, cs, "target-0", "alpha", "target", false, false)
	_, connectedAgent := addWorkloadLookupClient(t, cs, "target-1", "alpha", "target", false, true)

	require.Same(t, connectedAgent, cs.GetAgentForWorkload(t.Context(), "target"))
}

func TestGetAgentForWorkloadRejectsIneligibleAgents(t *testing.T) {
	tests := []struct {
		name        string
		workload    string
		pod         string
		namespace   string
		podWorkload string
		nodeAgent   bool
		disabled    bool
	}{
		{
			name:        "empty workload",
			pod:         "target-0",
			namespace:   "alpha",
			podWorkload: "target",
		},
		{
			name:        "missing workload",
			workload:    "target",
			pod:         "frontend-0",
			namespace:   "alpha",
			podWorkload: "frontend",
		},
		{
			name:        "different namespace",
			workload:    "target",
			pod:         "target-0",
			namespace:   "beta",
			podWorkload: "target",
		},
		{
			name:        "node agent",
			workload:    "target",
			pod:         "target-0",
			namespace:   "alpha",
			podWorkload: "target",
			nodeAgent:   true,
		},
		{
			name:        "disabled clients",
			workload:    "target",
			pod:         "target-0",
			namespace:   "alpha",
			podWorkload: "target",
			disabled:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := newTestClients(t.Context(), "alpha")
			addWorkloadLookupClient(t, cs, tt.pod, tt.namespace, tt.podWorkload, tt.nodeAgent, true)
			cs.disabled.Store(tt.disabled)

			require.Nil(t, cs.GetAgentForWorkload(t.Context(), tt.workload))
		})
	}
}

func TestGetAgentForWorkloadReturnsNilWhenConnectionFails(t *testing.T) {
	ctx := tpClient.WithConfig(t.Context(), tpClient.GetDefaultConfig())
	ctx = k8sapi.WithK8sInterface(ctx, fake.NewClientset())
	cs := newTestClients(ctx, "alpha")
	addWorkloadLookupClient(t, cs, "target-0", "alpha", "target", false, false)
	lookupContext, cancel := context.WithCancel(ctx)
	cancel()

	require.Nil(t, cs.GetAgentForWorkload(lookupContext, "target"))
	_, exists := cs.clients.Load("target-0.alpha")
	require.False(t, exists)
}

func TestGetAgentForWorkloadConcurrentWithRefresh(t *testing.T) {
	cs := newTestClients(t.Context(), "alpha")
	target, targetAgent := addWorkloadLookupClient(t, cs, "target-0", "alpha", "target", false, true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 1000 {
			target.refresh(&manager.AgentPodInfo{
				PodName:      fmt.Sprintf("target-%d", i),
				Namespace:    "alpha",
				WorkloadName: "target",
			})
		}
	}()

	for range 1000 {
		require.Same(t, targetAgent, cs.GetAgentForWorkload(t.Context(), "target"))
	}
	<-done
}
