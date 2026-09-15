package rootd

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"testing"

	dns2 "github.com/miekg/dns"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/telepresenceio/telepresence/rpc/v2/agent"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/agentpf"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
	"github.com/telepresenceio/telepresence/v2/pkg/client/rootd/dns"
	"github.com/telepresenceio/telepresence/v2/pkg/client/rootd/vip"
	"github.com/telepresenceio/telepresence/v2/pkg/dnsproxy"
	"github.com/telepresenceio/telepresence/v2/pkg/vif"
)

type workloadLookupManager struct {
	manager.UnimplementedManagerServer
	mu       sync.Mutex
	calls    int
	response *manager.DNSResponse
	err      error
}

func (m *workloadLookupManager) LookupDNS(_ context.Context, _ *manager.DNSRequest) (*manager.DNSResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return m.response, m.err
}

func (m *workloadLookupManager) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

type workloadLookupAgent struct {
	agent.UnimplementedAgentServer
	mu       sync.Mutex
	calls    int
	response *manager.LookupResponse
	err      error
}

func (a *workloadLookupAgent) Lookup(_ context.Context, _ *manager.LookupRequest) (*manager.LookupResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	return a.response, a.err
}

func (a *workloadLookupAgent) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

type workloadLookupClients struct {
	agentpf.Clients
	mu       sync.Mutex
	selected []string
	agents   map[string]agent.AgentClient
	random   agent.AgentClient
}

func (c *workloadLookupClients) GetAgentForWorkload(_ context.Context, workload string) agent.AgentClient {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.selected = append(c.selected, workload)
	return c.agents[workload]
}

func (c *workloadLookupClients) GetRandomAgent(context.Context) agent.AgentClient {
	return c.random
}

func (c *workloadLookupClients) selectedWorkloads() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.selected...)
}

type workloadLookupFixture struct {
	session *session
	manager *workloadLookupManager
	clients *workloadLookupClients
	agents  map[string]*workloadLookupAgent
}

type workloadLookupResponse struct {
	ips []string
	err error
}

func newWorkloadLookupFixture(
	t *testing.T,
	via []*rpc.SubnetViaWorkload,
	subnets []agentSubnet,
	agentResponses map[string]workloadLookupResponse,
) *workloadLookupFixture {
	t.Helper()
	ctx := context.Background()
	config := client.GetDefaultConfig()
	config.DNS().UseComplexLookup = true
	config.DNS().IncludeSuffixes = []string{".mesh.example"}
	ctx = client.WithConfig(ctx, config)

	fallbackResponse, err := dnsproxy.ToRPC(dnsproxy.RRs{&dns2.A{
		Hdr: rrHeader("service.mesh.example.", dns2.TypeA),
		A:   net.ParseIP("192.0.2.55").To4(),
	}}, dns2.RcodeSuccess)
	require.NoError(t, err)
	managerServer := &workloadLookupManager{response: fallbackResponse}
	managerConnection := newWorkloadLookupConnection(t, func(server *grpc.Server) {
		manager.RegisterManagerServer(server, managerServer)
	})

	clients := &workloadLookupClients{agents: make(map[string]agent.AgentClient)}
	agentServers := make(map[string]*workloadLookupAgent, len(agentResponses))
	for workload, response := range agentResponses {
		server := &workloadLookupAgent{err: response.err}
		if response.ips != nil {
			server.response = &manager.LookupResponse{}
			for _, value := range response.ips {
				addr := netip.MustParseAddr(value)
				encoded, marshalErr := addr.MarshalBinary()
				require.NoError(t, marshalErr)
				server.response.Ips = append(server.response.Ips, encoded)
			}
		}
		connection := newWorkloadLookupConnection(t, func(grpcServer *grpc.Server) {
			agent.RegisterAgentServer(grpcServer, server)
		})
		clients.agents[workload] = agent.NewAgentClient(connection)
		agentServers[workload] = server
	}

	vipGenerator := vip.NewGenerators(client.DefaultVirtualSubnet(), vif.TelepresenceULA6)
	for _, subnet := range subnets {
		vipGenerator.EnsureFamily(subnet.Addr())
	}
	s := &session{
		Cluster: &k8s.Cluster{
			Kubeconfig: &k8s.Kubeconfig{Context: ctx, Namespace: "default"},
		},
		managerConn:             managerConnection,
		agentClients:            clients,
		session:                 &manager.SessionInfo{SessionId: "workload-lookup-test"},
		subnetViaWorkloads:      via,
		localTranslationSubnets: subnets,
		localTranslationTable:   xsync.NewMap[netip.Addr, netip.Addr](),
		virtualIPs:              xsync.NewMap[netip.Addr, agentVIP](),
		vipGenerator:            vipGenerator,
	}
	s.dnsServer = dns.NewServer(config.DNS(), "default", s.clusterLookup)
	return &workloadLookupFixture{session: s, manager: managerServer, clients: clients, agents: agentServers}
}

func newWorkloadLookupConnection(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient("passthrough:///workload-lookup", grpc.WithContextDialer(
		func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, connection.Close())
		server.Stop()
		require.NoError(t, listener.Close())
	})
	return connection
}

func lookupQuestion(name string, qType uint16) *dns2.Question {
	return &dns2.Question{Name: name, Qtype: qType, Qclass: dns2.ClassINET}
}

func requireLookupAnswer(t *testing.T, records dnsproxy.RRs, expected string) {
	t.Helper()
	for _, record := range records {
		switch answer := record.(type) {
		case *dns2.A:
			if answer.A.String() == expected {
				return
			}
		case *dns2.AAAA:
			if answer.AAAA.String() == expected {
				return
			}
		}
	}
	t.Fatalf("DNS response %v does not contain %s", records, expected)
}

func TestClusterLookupComplexUsesManagerWithoutProxyViaWorkload(t *testing.T) {
	fixture := newWorkloadLookupFixture(t, nil, nil, map[string]workloadLookupResponse{
		"frontend": {ips: []string{"198.18.0.77"}},
	})
	fixture.clients.random = fixture.clients.agents["frontend"]

	records, rCode, err := fixture.session.clusterLookup(context.Background(),
		lookupQuestion("service.mesh.example.", dns2.TypeA))

	require.NoError(t, err)
	require.Equal(t, dns2.RcodeSuccess, rCode)
	requireLookupAnswer(t, records, "192.0.2.55")
	require.Equal(t, 1, fixture.manager.callCount())
	require.Zero(t, fixture.agents["frontend"].callCount())
}

func TestClusterLookupUsesSelectedProxyViaWorkload(t *testing.T) {
	via := []*rpc.SubnetViaWorkload{{Subnet: "198.18.0.0/16", Workload: "target"}}
	subnets := []agentSubnet{{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"}}
	fixture := newWorkloadLookupFixture(t, via, subnets, map[string]workloadLookupResponse{
		"target":   {ips: []string{"198.18.0.77", "203.0.113.44"}},
		"frontend": {ips: []string{"198.18.0.88"}},
	})
	fixture.clients.random = fixture.clients.agents["frontend"]

	records, rCode, err := fixture.session.clusterLookup(context.Background(),
		lookupQuestion("service.mesh.example.", dns2.TypeA))

	require.NoError(t, err)
	require.Equal(t, dns2.RcodeSuccess, rCode)
	requireLookupAnswer(t, records, "246.246.0.1")
	require.Len(t, records, 2)
	require.Equal(t, []string{"target"}, fixture.clients.selectedWorkloads())
	require.Equal(t, 1, fixture.agents["target"].callCount())
	require.Zero(t, fixture.agents["frontend"].callCount())
	require.Zero(t, fixture.manager.callCount())

	virtual, found := fixture.session.virtualIPs.Load(netip.MustParseAddr("246.246.0.1"))
	require.True(t, found)
	require.Equal(t, "target", virtual.workload)
	require.Equal(t, netip.MustParseAddr("198.18.0.77"), virtual.destinationIP)
}

func TestClusterLookupUsesConfiguredProxyViaWorkloadOrder(t *testing.T) {
	via := []*rpc.SubnetViaWorkload{
		{Subnet: "198.18.0.0/16", Workload: "target"},
		{Subnet: "198.19.0.0/16", Workload: "target"},
		{Subnet: "203.0.113.0/24", Workload: "frontend"},
	}
	subnets := []agentSubnet{
		{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"},
		{Prefix: netip.MustParsePrefix("198.19.0.0/16"), workload: "target"},
		{Prefix: netip.MustParsePrefix("203.0.113.0/24"), workload: "frontend"},
	}
	fixture := newWorkloadLookupFixture(t, via, subnets, map[string]workloadLookupResponse{
		"target":   {ips: []string{"203.0.113.7"}},
		"frontend": {ips: []string{"203.0.113.8"}},
	})

	records, rCode, err := fixture.session.clusterLookup(context.Background(),
		lookupQuestion("service.mesh.example.", dns2.TypeA))

	require.NoError(t, err)
	require.Equal(t, dns2.RcodeSuccess, rCode)
	requireLookupAnswer(t, records, "246.246.0.1")
	require.Equal(t, []string{"target", "frontend"}, fixture.clients.selectedWorkloads())
	require.Equal(t, 1, fixture.agents["target"].callCount())
	require.Equal(t, 1, fixture.agents["frontend"].callCount())
	require.Zero(t, fixture.manager.callCount())
	virtual, found := fixture.session.virtualIPs.Load(netip.MustParseAddr("246.246.0.1"))
	require.True(t, found)
	require.Equal(t, "frontend", virtual.workload)
}

func TestClusterLookupOverlappingRoutesUseMostSpecificWorkload(t *testing.T) {
	tests := []struct {
		name         string
		subnets      []agentSubnet
		wantWorkload string
		wantFallback bool
	}{
		{
			name: "narrow target overrides earlier broad local route",
			subnets: []agentSubnet{
				{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: ""},
				{Prefix: netip.MustParsePrefix("198.18.0.77/32"), workload: "target"},
			},
			wantWorkload: "target",
		},
		{
			name: "narrow target overrides later broad local route",
			subnets: []agentSubnet{
				{Prefix: netip.MustParsePrefix("198.18.0.77/32"), workload: "target"},
				{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: ""},
			},
			wantWorkload: "target",
		},
		{
			name: "narrow local route overrides broad target route",
			subnets: []agentSubnet{
				{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"},
				{Prefix: netip.MustParsePrefix("198.18.0.77/32"), workload: ""},
			},
			wantFallback: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			via := []*rpc.SubnetViaWorkload{
				{Subnet: "service", Workload: "local"},
				{Subnet: "198.18.0.77/32", Workload: "target"},
			}
			fixture := newWorkloadLookupFixture(t, via, tt.subnets, map[string]workloadLookupResponse{
				"target": {ips: []string{"198.18.0.77"}},
			})

			records, rCode, err := fixture.session.clusterLookup(context.Background(),
				lookupQuestion("service.mesh.example.", dns2.TypeA))

			require.NoError(t, err)
			require.Equal(t, dns2.RcodeSuccess, rCode)
			if tt.wantFallback {
				requireLookupAnswer(t, records, "192.0.2.55")
				require.Equal(t, 1, fixture.manager.callCount())
				require.Equal(t, 1, fixture.agents["target"].callCount())
				require.Zero(t, fixture.session.virtualIPs.Size())
				return
			}
			requireLookupAnswer(t, records, "246.246.0.1")
			require.Zero(t, fixture.manager.callCount())
			virtual, found := fixture.session.virtualIPs.Load(netip.MustParseAddr("246.246.0.1"))
			require.True(t, found)
			require.Equal(t, tt.wantWorkload, virtual.workload)
			require.Equal(t, netip.MustParseAddr("198.18.0.77"), virtual.destinationIP)
		})
	}
}

func TestClusterLookupUsesManagerProvidedIncludeSuffix(t *testing.T) {
	via := []*rpc.SubnetViaWorkload{{Subnet: "198.18.0.0/16", Workload: "target"}}
	subnets := []agentSubnet{{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"}}
	fixture := newWorkloadLookupFixture(t, via, subnets, map[string]workloadLookupResponse{
		"target": {ips: []string{"198.18.0.77"}},
	})
	fixture.session.dnsServer.Lock()
	fixture.session.dnsServer.IncludeSuffixes = nil
	fixture.session.dnsServer.Unlock()
	fixture.session.dnsServer.SetClusterDNS(&manager.DNS{
		IncludeSuffixes: []string{".MESH.EXAMPLE"},
		ClusterDomain:   "cluster.local.",
	}, netip.AddrPort{})

	records, rCode, err := fixture.session.clusterLookup(context.Background(),
		lookupQuestion("service.mesh.example.", dns2.TypeA))

	require.NoError(t, err)
	require.Equal(t, dns2.RcodeSuccess, rCode)
	requireLookupAnswer(t, records, "246.246.0.1")
	require.Zero(t, fixture.manager.callCount())
	require.Equal(t, 1, fixture.agents["target"].callCount())
}

func TestClusterLookupFallsBackToManager(t *testing.T) {
	tests := []struct {
		name          string
		responses     map[string]workloadLookupResponse
		subnets       []agentSubnet
		wantAgentCall int
	}{
		{
			name:          "agent unavailable",
			subnets:       []agentSubnet{{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"}},
			wantAgentCall: 0,
		},
		{
			name:          "agent returns no addresses",
			responses:     map[string]workloadLookupResponse{"target": {ips: []string{}}},
			subnets:       []agentSubnet{{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"}},
			wantAgentCall: 1,
		},
		{
			name:          "agent lookup fails",
			responses:     map[string]workloadLookupResponse{"target": {err: status.Error(codes.Unavailable, "unavailable")}},
			subnets:       []agentSubnet{{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"}},
			wantAgentCall: 1,
		},
		{
			name:          "agent address is outside workload subnet",
			responses:     map[string]workloadLookupResponse{"target": {ips: []string{"203.0.113.7"}}},
			subnets:       []agentSubnet{{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"}},
			wantAgentCall: 1,
		},
		{
			name:          "workload has no concrete subnet",
			responses:     map[string]workloadLookupResponse{"target": {ips: []string{"198.18.0.7"}}},
			wantAgentCall: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newWorkloadLookupFixture(t,
				[]*rpc.SubnetViaWorkload{{Subnet: "198.18.0.0/16", Workload: "target"}},
				tt.subnets, tt.responses)

			records, rCode, err := fixture.session.clusterLookup(context.Background(),
				lookupQuestion("service.mesh.example.", dns2.TypeA))

			require.NoError(t, err)
			require.Equal(t, dns2.RcodeSuccess, rCode)
			requireLookupAnswer(t, records, "192.0.2.55")
			require.Equal(t, 1, fixture.manager.callCount())
			if target := fixture.agents["target"]; target != nil {
				require.Equal(t, tt.wantAgentCall, target.callCount())
			}
		})
	}
}

func TestClusterLookupWorkloadScope(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		qType         uint16
		include       []string
		clusterDomain string
		via           []*rpc.SubnetViaWorkload
		wantAgent     bool
	}{
		{
			name:      "included suffix matches case and trailing dot",
			query:     "Service.Mesh.Example.",
			qType:     dns2.TypeA,
			include:   []string{".MESH.EXAMPLE."},
			wantAgent: true,
		},
		{
			name:      "included suffix matches whole domain",
			query:     "mesh.example.",
			qType:     dns2.TypeA,
			include:   []string{".mesh.example"},
			wantAgent: true,
		},
		{
			name:    "suffix requires a label boundary",
			query:   "service.notmesh.example.",
			qType:   dns2.TypeA,
			include: []string{"mesh.example"},
		},
		{
			name:    "ordinary name remains on manager",
			query:   "service.other.example.",
			qType:   dns2.TypeA,
			include: []string{"mesh.example"},
		},
		{
			name:    "CNAME remains on manager",
			query:   "service.mesh.example.",
			qType:   dns2.TypeCNAME,
			include: []string{"mesh.example"},
		},
		{
			name:    "PTR remains on manager",
			query:   "service.mesh.example.",
			qType:   dns2.TypePTR,
			include: []string{"mesh.example"},
		},
		{
			name:    "cluster service remains on manager",
			query:   "frontend.default.svc.cluster.local.",
			qType:   dns2.TypeA,
			include: []string{"cluster.local"},
		},
		{
			name:          "custom cluster domain remains on manager",
			query:         "frontend.default.svc.internal.test.",
			qType:         dns2.TypeA,
			include:       []string{"internal.test"},
			clusterDomain: "INTERNAL.TEST.",
		},
		{
			name:    "short cluster service remains on manager",
			query:   "frontend.default.svc.",
			qType:   dns2.TypeA,
			include: []string{"svc"},
		},
		{
			name:    "local-only virtual routing remains on manager",
			query:   "service.mesh.example.",
			qType:   dns2.TypeA,
			include: []string{"mesh.example"},
			via:     []*rpc.SubnetViaWorkload{{Subnet: "198.18.0.0/16", Workload: "local"}},
		},
		{
			name:    "no included suffix remains on manager",
			query:   "service.mesh.example.",
			qType:   dns2.TypeA,
			include: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			via := tt.via
			if via == nil {
				via = []*rpc.SubnetViaWorkload{{Subnet: "198.18.0.0/16", Workload: "target"}}
			}
			subnets := []agentSubnet{{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"}}
			fixture := newWorkloadLookupFixture(t, via, subnets, map[string]workloadLookupResponse{
				"target": {ips: []string{"198.18.0.77"}},
			})
			fixture.session.dnsServer.Lock()
			fixture.session.dnsServer.IncludeSuffixes = tt.include
			fixture.session.dnsServer.Unlock()
			if tt.clusterDomain != "" {
				fixture.session.dnsServer.SetClusterDNS(&manager.DNS{ClusterDomain: tt.clusterDomain}, netip.AddrPort{})
			}

			records, rCode, err := fixture.session.clusterLookup(context.Background(),
				lookupQuestion(tt.query, tt.qType))

			require.NoError(t, err)
			require.Equal(t, dns2.RcodeSuccess, rCode)
			if tt.wantAgent {
				requireLookupAnswer(t, records, "246.246.0.1")
				require.Equal(t, 1, fixture.agents["target"].callCount())
				require.Zero(t, fixture.manager.callCount())
			} else {
				requireLookupAnswer(t, records, "192.0.2.55")
				require.Zero(t, fixture.agents["target"].callCount())
				require.Equal(t, 1, fixture.manager.callCount())
			}
		})
	}
}

func TestClusterLookupWorkloadIPv6(t *testing.T) {
	via := []*rpc.SubnetViaWorkload{{Subnet: "2001:db8::/64", Workload: "target"}}
	subnets := []agentSubnet{{Prefix: netip.MustParsePrefix("2001:db8::/64"), workload: "target"}}
	fixture := newWorkloadLookupFixture(t, via, subnets, map[string]workloadLookupResponse{
		"target": {ips: []string{"2001:db8::77"}},
	})

	records, rCode, err := fixture.session.clusterLookup(context.Background(),
		lookupQuestion("service.mesh.example.", dns2.TypeAAAA))

	require.NoError(t, err)
	require.Equal(t, dns2.RcodeSuccess, rCode)
	requireLookupAnswer(t, records, "fd00:0:0:246::1")
	require.Len(t, records, 2)
	require.Zero(t, fixture.manager.callCount())
	virtual, found := fixture.session.virtualIPs.Load(netip.MustParseAddr("fd00:0:0:246::1"))
	require.True(t, found)
	require.Equal(t, "target", virtual.workload)
	require.Equal(t, netip.MustParseAddr("2001:db8::77"), virtual.destinationIP)
}

func TestClusterLookupWorkloadOppositeFamilyDoesNotFallBack(t *testing.T) {
	via := []*rpc.SubnetViaWorkload{{Subnet: "198.18.0.0/16", Workload: "target"}}
	subnets := []agentSubnet{{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"}}
	fixture := newWorkloadLookupFixture(t, via, subnets, map[string]workloadLookupResponse{
		"target": {ips: []string{"198.18.0.77"}},
	})

	records, rCode, err := fixture.session.clusterLookup(context.Background(),
		lookupQuestion("service.mesh.example.", dns2.TypeAAAA))

	require.NoError(t, err)
	require.Equal(t, dns2.RcodeSuccess, rCode)
	require.Len(t, records, 2)
	requireLookupAnswer(t, records, "246.246.0.1")
	require.Zero(t, fixture.manager.callCount())
}

func TestClusterLookupWorkloadConcurrentAddressFamilies(t *testing.T) {
	via := []*rpc.SubnetViaWorkload{{Subnet: "198.18.0.0/16", Workload: "target"}}
	subnets := []agentSubnet{{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"}}
	fixture := newWorkloadLookupFixture(t, via, subnets, map[string]workloadLookupResponse{
		"target": {ips: []string{"198.18.0.77"}},
	})
	type result struct {
		records dnsproxy.RRs
		rCode   int
		err     error
		ok      bool
	}
	results := make(chan result, 2)
	for _, qType := range []uint16{dns2.TypeA, dns2.TypeAAAA} {
		go func() {
			records, rCode, err, ok := fixture.session.lookupViaWorkload(context.Background(),
				lookupQuestion("service.mesh.example.", qType))
			results <- result{records: records, rCode: rCode, err: err, ok: ok}
		}()
	}

	for range cap(results) {
		got := <-results
		require.True(t, got.ok)
		require.NoError(t, got.err)
		require.Equal(t, dns2.RcodeSuccess, got.rCode)
		requireLookupAnswer(t, got.records, "246.246.0.1")
	}
	require.Equal(t, 2, fixture.agents["target"].callCount())
	require.Equal(t, 1, fixture.session.virtualIPs.Size())
	require.Zero(t, fixture.manager.callCount())
}

func TestClusterLookupWorkloadPreservesCanceledContext(t *testing.T) {
	via := []*rpc.SubnetViaWorkload{{Subnet: "198.18.0.0/16", Workload: "target"}}
	subnets := []agentSubnet{{Prefix: netip.MustParsePrefix("198.18.0.0/16"), workload: "target"}}
	fixture := newWorkloadLookupFixture(t, via, subnets, map[string]workloadLookupResponse{
		"target": {ips: []string{"198.18.0.77"}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	records, rCode, err := fixture.session.clusterLookup(ctx,
		lookupQuestion("service.mesh.example.", dns2.TypeA))

	require.Empty(t, records)
	require.Equal(t, dns2.RcodeServerFailure, rCode)
	require.Error(t, err)
	require.Equal(t, codes.Canceled, status.Code(err))
	require.Zero(t, fixture.manager.callCount())
}

func TestClusterLookupManagerErrorsReturnSERVFAIL(t *testing.T) {
	for _, code := range []codes.Code{codes.Canceled, codes.DeadlineExceeded, codes.Unavailable} {
		t.Run(code.String(), func(t *testing.T) {
			fixture := newWorkloadLookupFixture(t, nil, nil, nil)
			fixture.manager.err = status.Error(code, "lookup failed")

			records, rCode, err := fixture.session.complexClusterLookup(context.Background(),
				lookupQuestion("service.mesh.example.", dns2.TypeA))

			require.Empty(t, records)
			require.Equal(t, dns2.RcodeServerFailure, rCode)
			require.Equal(t, code, status.Code(err))
			require.Equal(t, 1, fixture.manager.callCount())
		})
	}
}
