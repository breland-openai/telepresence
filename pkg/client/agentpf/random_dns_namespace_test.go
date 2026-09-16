package agentpf

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/telepresenceio/telepresence/rpc/v2/agent"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
)

type namespaceDNSAgent struct {
	agent.UnimplementedAgentServer
	namespace string
}

func (a *namespaceDNSAgent) Lookup(_ context.Context, req *manager.LookupRequest) (*manager.LookupResponse, error) {
	name := strings.TrimSuffix(req.Name, ".")
	if !strings.Contains(name, ".") {
		name += "." + a.namespace + ".svc.cluster.local"
	}
	addresses := map[string]string{
		"database.alpha.svc.cluster.local":      "10.0.0.11",
		"database.beta.svc.cluster.local":       "10.0.0.12",
		"database.gamma.svc.cluster.local":      "10.0.0.13",
		"database.ambassador.svc.cluster.local": "10.0.0.14",
	}
	response := &manager.LookupResponse{}
	if address, ok := addresses[name]; ok {
		response.Ips = [][]byte{netip.MustParseAddr(address).AsSlice()}
	}
	return response, nil
}

func realNamespaceDNSAgent(t *testing.T, namespace string) agent.AgentClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	agent.RegisterAgentServer(server, &namespaceDNSAgent{namespace: namespace})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///namespace-agent", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return agent.NewAgentClient(conn)
}

func namespaceDNSAnswer(t *testing.T, candidate agent.AgentClient, name string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	response, err := candidate.Lookup(ctx, &manager.LookupRequest{Name: name})
	require.NoError(t, err)
	require.Len(t, response.Ips, 1)
	address, ok := netip.AddrFromSlice(response.Ips[0])
	require.True(t, ok)
	return address.Unmap().String()
}

func namespaceDNSPod(name, namespace string, node bool) *manager.AgentPodInfo {
	return &manager.AgentPodInfo{
		PodName: name + "-0", PodId: name + "-id", Namespace: namespace,
		WorkloadName: "database", NodeAgent: node,
	}
}

func namespaceDNSClients(t *testing.T) *clients {
	t.Helper()
	cs := newTestClients(t.Context(), "alpha")
	cs.setNamespaces([]string{"alpha", "beta"})
	return cs
}

func TestGetRandomDNSAgentOnlyWrongMappedResolverLeavesOrdinaryNameToManager(t *testing.T) {
	cs := namespaceDNSClients(t)
	beta := realNamespaceDNSAgent(t, "beta")
	require.Equal(t, "10.0.0.12", namespaceDNSAnswer(t, beta, "database."))
	wrong := seedWorkloadTestClient(cs, namespaceDNSPod("beta", "beta", false), beta)
	last := time.Now().Add(-time.Second).UnixNano()
	atomic.StoreInt64(&wrong.lastActive, last)
	unconnected := cs.loadOrAddClient(namespaceDNSPod("alpha", "alpha", false))

	if selected := cs.GetRandomAgent(t.Context()); selected != nil {
		t.Errorf("ordinary name for connected namespace alpha was delegated to a real agent returning %s", namespaceDNSAnswer(t, selected, "database."))
	}
	require.Equal(t, last, atomic.LoadInt64(&wrong.lastActive), "ordinary DNS must not keep another namespace's resolver alive")
	require.False(t, unconnected.connected(), "ordinary DNS must not dial a new agent")
}

func TestGetRandomDNSAgentUsesConnectedNamespaceAndStillResolvesExplicitOtherNamespace(t *testing.T) {
	cs := namespaceDNSClients(t)
	alpha := realNamespaceDNSAgent(t, "alpha")
	beta := realNamespaceDNSAgent(t, "beta")
	require.Equal(t, "10.0.0.11", namespaceDNSAnswer(t, alpha, "database."))
	require.Equal(t, "10.0.0.12", namespaceDNSAnswer(t, beta, "database."))
	seedWorkloadTestClient(cs, namespaceDNSPod("beta", "beta", false), beta)
	correct := seedWorkloadTestClient(cs, namespaceDNSPod("alpha", "alpha", false), alpha)
	last := time.Now().Add(-time.Second).UnixNano()
	atomic.StoreInt64(&correct.lastActive, last)

	for range 20 {
		selected := cs.GetRandomAgent(t.Context())
		require.NotNil(t, selected)
		require.Equal(t, "10.0.0.11", namespaceDNSAnswer(t, selected, "database."))
		require.Equal(t, "10.0.0.12", namespaceDNSAnswer(t, selected, "database.beta.svc.cluster.local."))
	}
	require.Greater(t, atomic.LoadInt64(&correct.lastActive), last)
}

func TestGetRandomDNSAgentRejectsConnectedNodeUnmappedAndUnknownNamespaceResolvers(t *testing.T) {
	for _, item := range []struct {
		name, physicalResolver, advertisedNamespace, expected string
		node                                                  bool
	}{
		{name: "node", physicalResolver: "ambassador", advertisedNamespace: "alpha", expected: "10.0.0.14", node: true},
		{name: "unmapped", physicalResolver: "gamma", advertisedNamespace: "gamma", expected: "10.0.0.13"},
		{name: "missing-namespace", physicalResolver: "beta", expected: "10.0.0.12"},
	} {
		t.Run(item.name, func(t *testing.T) {
			cs := namespaceDNSClients(t)
			candidate := realNamespaceDNSAgent(t, item.physicalResolver)
			require.Equal(t, item.expected, namespaceDNSAnswer(t, candidate, "database."))
			seedWorkloadTestClient(cs, namespaceDNSPod(item.name, item.advertisedNamespace, item.node), candidate)
			require.Nil(t, cs.GetRandomAgent(t.Context()))
		})
	}
}

func TestGetRandomDNSAgentLegacyWithoutPodUIDKeepsVerifiedConnectedNamespace(t *testing.T) {
	cs := namespaceDNSClients(t)
	alpha := realNamespaceDNSAgent(t, "alpha")
	require.Equal(t, "10.0.0.11", namespaceDNSAnswer(t, alpha, "database."))
	pod := namespaceDNSPod("legacy", "alpha", false)
	pod.PodId = ""
	pod.PodIp = netip.MustParseAddr("10.0.0.51").AsSlice()
	seedWorkloadTestClient(cs, pod, alpha)
	selected := cs.GetRandomAgent(t.Context())
	require.NotNil(t, selected)
	require.Equal(t, "10.0.0.11", namespaceDNSAnswer(t, selected, "database."))
}
