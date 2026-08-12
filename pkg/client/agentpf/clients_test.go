package agentpf

import (
	"context"
	"fmt"
	"net/netip"
	"sync/atomic"
	"testing"
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
