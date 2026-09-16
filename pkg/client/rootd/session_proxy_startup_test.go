package rootd

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/agentpf"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
	"github.com/telepresenceio/telepresence/v2/pkg/log"
)

type embeddedProxyRequest struct {
	name      string
	namespace string
	deadline  time.Time
}

type embeddedProxyManager struct {
	manager.UnimplementedManagerServer
	gates            map[string]chan struct{}
	started          chan embeddedProxyRequest
	canceled         chan string
	watchStarted     chan struct{}
	watchStopped     chan struct{}
	watchDeltas      chan *manager.AgentPodInfoDelta
	watchScopes      chan []string
	watchFailure     chan struct{}
	legacyAgentWatch bool
}

func (m *embeddedProxyManager) EnsureAgent(ctx context.Context, req *manager.EnsureAgentRequest) (*manager.AgentInfoSnapshot, error) {
	dl, _ := ctx.Deadline()
	m.started <- embeddedProxyRequest{name: req.Name, namespace: req.Namespace, deadline: dl}
	select {
	case <-m.gates[req.Name]:
		return &manager.AgentInfoSnapshot{}, nil
	case <-ctx.Done():
		m.canceled <- req.Name
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (m *embeddedProxyManager) WatchAgentPodsInNamespacesDelta(req *manager.AgentsRequest, stream grpc.ServerStreamingServer[manager.AgentPodInfoDelta]) error {
	m.watchStarted <- struct{}{}
	m.watchScopes <- req.Namespaces
	if m.legacyAgentWatch {
		return status.Error(codes.Unimplemented, "legacy manager only supports connected-namespace agent watches")
	}
	return m.streamAgentPods(stream)
}

func (m *embeddedProxyManager) WatchAgentPodsDelta(_ *manager.SessionInfo, stream grpc.ServerStreamingServer[manager.AgentPodInfoDelta]) error {
	m.watchStarted <- struct{}{}
	m.watchScopes <- []string{"default"}
	return m.streamAgentPods(stream)
}

func (m *embeddedProxyManager) streamAgentPods(stream grpc.ServerStreamingServer[manager.AgentPodInfoDelta]) error {
	defer func() { m.watchStopped <- struct{}{} }()
	for {
		select {
		case delta := <-m.watchDeltas:
			if err := stream.Send(delta); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-m.watchFailure:
			return status.Error(codes.Unavailable, "previous physical manager is unavailable")
		}
	}
}

func newEmbeddedProxyManager() *embeddedProxyManager {
	return &embeddedProxyManager{
		gates: make(map[string]chan struct{}), started: make(chan embeddedProxyRequest, 8), canceled: make(chan string, 8),
		watchStarted: make(chan struct{}, 8), watchStopped: make(chan struct{}, 8), watchDeltas: make(chan *manager.AgentPodInfoDelta, 8),
		watchScopes: make(chan []string, 8), watchFailure: make(chan struct{}),
	}
}

func newEmbeddedProxyFixture(t *testing.T, names ...string) (*InProcSession, *embeddedProxyManager, context.CancelFunc) {
	t.Helper()
	return newEmbeddedProxyFixtureForAgentNamespaces(t, []string{"default"}, false, false, names...)
}

func newEmbeddedProxyFixtureForAgentNamespaces(t *testing.T, namespaces []string, external, legacy bool, names ...string) (*InProcSession, *embeddedProxyManager, context.CancelFunc) {
	t.Helper()
	config := client.GetDefaultConfig()
	config.DNS().PreserveLocalClusterDNS = false
	config.DNS().UseComplexLookup = true
	config.Cluster().AgentPortForward = true
	if external {
		config.Cluster().ManagerAddress = "tls://traffic-manager.example.com:8443"
	}
	config.Grpc().WatchRetryInterval = 10 * time.Millisecond
	persistent, stop := context.WithCancel(client.WithConfig(t.Context(), config))
	t.Cleanup(stop)
	mgr := newEmbeddedProxyManager()
	mgr.legacyAgentWatch = legacy
	for _, name := range names {
		if name != "" && name != "local" {
			mgr.gates[name] = make(chan struct{})
		}
	}
	conn := newWorkloadLookupConnection(t, func(s *grpc.Server) { manager.RegisterManagerServer(s, mgr) })
	cluster := &k8s.Cluster{Kubeconfig: &k8s.Kubeconfig{Context: persistent, Namespace: "default"}}
	nc := &rpc.NetworkConfig{
		Namespace: "default", Session: &manager.SessionInfo{SessionId: "embedded-proxy"}, AgentPodNamespaces: namespaces,
	}
	for _, name := range names {
		nc.SubnetViaWorkloads = append(nc.SubnetViaWorkloads, &rpc.SubnetViaWorkload{Workload: name, Subnet: "10.8.0.1/32"})
	}
	version := "2.32.0"
	if legacy {
		version = "2.27.9"
	}
	rd, err := NewInProcSession(cluster, nc, conn, semver.MustParse(version), nil, true)
	require.NoError(t, err)
	return rd, mgr, stop
}

func TestEmbeddedNamedProxyRejectsUnwatchedConnectedNamespaceBeforeManagerInjection(t *testing.T) {
	for _, tt := range []struct {
		name             string
		external, legacy bool
	}{
		{name: "direct"},
		{name: "external", external: true},
		{name: "direct legacy", legacy: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rd, mgr, stopSession := newEmbeddedProxyFixtureForAgentNamespaces(t, []string{"other"}, tt.external, tt.legacy, "proxy")
			g := log.NewGroup(rd)
			defer func() {
				stopSession()
				require.NoError(t, g.Wait())
			}()
			startup, stopStartup := context.WithTimeout(t.Context(), 5*time.Second)
			defer stopStartup()
			finished := make(chan error, 1)
			go func() { finished <- rd.StartWithContext(startup, g, 0) }()

			select {
			case request := <-mgr.started:
				t.Fatalf("named proxy asked the real manager to inject into its unwatched connected namespace: %#v", request)
			case err := <-finished:
				require.Error(t, err)
				require.Equal(t, errcat.User, errcat.GetCategory(err))
				require.ErrorContains(t, err, `--proxy-via cannot use traffic-agents in the connected namespace "default"`)
				require.ErrorContains(t, err, `this session does not watch its traffic-agents`)
				require.ErrorContains(t, err, `--mapped-namespaces including "default"`)
				if tt.external {
					require.NotContains(t, err.Error(), "kubectl")
				} else {
					require.ErrorContains(t, err, `kubectl auth can-i create pods/portforward --namespace default`)
				}
			case <-time.After(time.Second):
				t.Fatal("unwatched named proxy did not reject before its five-second agent arrival deadline")
			}
			select {
			case request := <-mgr.started:
				t.Fatalf("unwatched named proxy injected after rejecting: %#v", request)
			default:
			}
			select {
			case scope := <-mgr.watchScopes:
				t.Fatalf("unwatched named proxy started an unusable direct manager watch: %v", scope)
			default:
			}
		})
	}
}

func TestEmbeddedNamedProxyWatchedConnectedNamespaceReachesManagerAndNativeWatch(t *testing.T) {
	for _, tt := range []struct {
		name             string
		external, legacy bool
	}{
		{name: "direct"},
		{name: "external", external: true},
		{name: "direct legacy", legacy: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rd, mgr, stopSession := newEmbeddedProxyFixtureForAgentNamespaces(t, []string{"other", "default"}, tt.external, tt.legacy, "proxy")
			g := log.NewGroup(rd)
			defer func() {
				stopSession()
				require.NoError(t, g.Wait())
			}()
			close(mgr.gates["proxy"])
			require.NoError(t, rd.StartWithContext(t.Context(), g, 0))
			request := embeddedProxyReceive(t, mgr.started)
			require.Equal(t, "proxy", request.name)
			require.Empty(t, request.namespace, "EnsureAgent uses the connected namespace from this real session")
			require.ElementsMatch(t, []string{"default", "other"}, embeddedProxyReceive(t, mgr.watchScopes))
			if tt.legacy {
				require.Equal(t, []string{"default"}, embeddedProxyReceive(t, mgr.watchScopes), "native older manager fallback retains the connected namespace")
			}
		})
	}
}

func TestEmbeddedOrdinaryAndLocalProxyDoNotRequireConnectedAgentWatch(t *testing.T) {
	for _, tt := range []struct {
		name, workload string
		external       bool
	}{
		{name: "direct ordinary"},
		{name: "direct local", workload: "local"},
		{name: "external ordinary", external: true},
		{name: "external local", workload: "local", external: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rd, mgr, stopSession := newEmbeddedProxyFixtureForAgentNamespaces(t, []string{"other"}, tt.external, false, tt.workload)
			g := log.NewGroup(rd)
			defer func() {
				stopSession()
				require.NoError(t, g.Wait())
			}()
			require.NoError(t, rd.StartWithContext(t.Context(), g, 0))
			select {
			case request := <-mgr.started:
				t.Fatalf("ordinary or local proxy requested a manager agent: %#v", request)
			case scope := <-mgr.watchScopes:
				t.Fatalf("ordinary or local proxy started a direct manager watch: %v", scope)
			case <-time.After(30 * time.Millisecond):
			}
		})
	}
}

func embeddedProxyReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(time.Second):
		t.Fatal("embedded root proxy fixture did not produce its expected event")
		var zero T
		return zero
	}
}

func TestEmbeddedProxyStartupCallerCancellationStopsSecondSerialManagerWait(t *testing.T) {
	rd, mgr, stopSession := newEmbeddedProxyFixture(t, "first", "second")
	defer stopSession()
	startup, stopStartup := context.WithTimeout(t.Context(), 600*time.Millisecond)
	defer stopStartup()
	g := log.NewGroup(rd)
	result := make(chan error, 1)
	go func() { result <- rd.StartWithContext(startup, g, 0) }()
	first := embeddedProxyReceive(t, mgr.started)
	require.Contains(t, []string{"first", "second"}, first.name)
	deadline, ok := startup.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, deadline, first.deadline, 30*time.Millisecond)
	select {
	case next := <-mgr.started:
		t.Fatalf("next manager workload was started before the first was released: %#v", next)
	case <-time.After(30 * time.Millisecond):
	}
	close(mgr.gates[first.name])
	second := embeddedProxyReceive(t, mgr.started)
	require.Contains(t, []string{"first", "second"}, second.name)
	require.NotEqual(t, first.name, second.name)
	require.WithinDuration(t, deadline, second.deadline, 30*time.Millisecond)
	stopStartup()
	require.Equal(t, codes.Canceled, status.Code(embeddedProxyReceive(t, result)))
	require.Equal(t, second.name, embeddedProxyReceive(t, mgr.canceled))
	require.NoError(t, rd.Err())
	stopSession()
	require.NoError(t, g.Wait())
}

func TestEmbeddedProxyStartupDeadlineStopsManagerWait(t *testing.T) {
	rd, mgr, stopSession := newEmbeddedProxyFixture(t, "missing")
	defer stopSession()
	startup, stopStartup := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer stopStartup()
	g := log.NewGroup(rd)
	result := make(chan error, 1)
	go func() { result <- rd.StartWithContext(startup, g, 0) }()
	require.Equal(t, "missing", embeddedProxyReceive(t, mgr.started).name)
	require.Equal(t, codes.DeadlineExceeded, status.Code(embeddedProxyReceive(t, result)))
	require.Equal(t, "missing", embeddedProxyReceive(t, mgr.canceled))
	require.NoError(t, rd.Err())
	stopSession()
	require.NoError(t, g.Wait())
}

func TestEmbeddedProxySuccessfulStartupOutlivesCallerAndDeduplicatesLocalRoutes(t *testing.T) {
	rd, mgr, stopSession := newEmbeddedProxyFixture(t, "same", "local", "same", "")
	defer stopSession()
	close(mgr.gates["same"])
	startup, stopStartup := context.WithTimeout(t.Context(), 600*time.Millisecond)
	defer stopStartup()
	g := log.NewGroup(rd)
	require.NoError(t, rd.StartWithContext(startup, g, 0))
	require.Equal(t, "same", embeddedProxyReceive(t, mgr.started).name)
	select {
	case request := <-mgr.started:
		t.Fatalf("duplicate or local translation started an additional manager request: %#v", request)
	default:
	}
	groupResult := make(chan error, 1)
	go func() { groupResult <- g.Wait() }()
	stopStartup()
	require.NoError(t, rd.Err())
	select {
	case err := <-groupResult:
		t.Fatalf("successful embedded root relay stopped with its startup caller: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	_, err := rd.managerClient().EnsureAgent(rd, &manager.EnsureAgentRequest{Name: "same"})
	require.NoError(t, err)
	require.Equal(t, "same", embeddedProxyReceive(t, mgr.started).name)
	stopSession()
	require.NoError(t, embeddedProxyReceive(t, groupResult))
}

func TestEmbeddedProxyNetworkReadinessPreservesCancellationBeforeAndAfterVIF(t *testing.T) {
	for _, afterVIF := range []bool{false, true} {
		name := "before VIF"
		if afterVIF {
			name = "after VIF before DNS"
		}
		t.Run(name, func(t *testing.T) {
			rd, _, stopSession := newEmbeddedProxyFixture(t)
			defer stopSession()
			if afterVIF {
				close(rd.vifReady)
			}
			startup, stopStartup := context.WithCancel(t.Context())
			defer stopStartup()
			result := make(chan error, 1)
			go func() { _, err := rd.WaitForNetwork(startup, &emptypb.Empty{}); result <- err }()
			select {
			case err := <-result:
				t.Fatalf("unready embedded network returned before its caller canceled: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			stopStartup()
			require.Equal(t, codes.Canceled, status.Code(embeddedProxyReceive(t, result)))
			require.NoError(t, rd.Err())
		})
	}
}

type embeddedProxyClients struct {
	agentpf.Clients
	started  chan string
	canceled chan string
	gates    map[string]chan error
}

func (c *embeddedProxyClients) SetProxyVia(string) {}

func (c *embeddedProxyClients) WaitForWorkload(ctx context.Context, _ time.Duration, name string) error {
	c.started <- name
	select {
	case err := <-c.gates[name]:
		return err
	case <-ctx.Done():
		c.canceled <- name
		return ctx.Err()
	}
}

func TestEmbeddedProxyPortForwardReportsActualFailedNameAndCancelsOtherWaits(t *testing.T) {
	rd, _, stopSession := newEmbeddedProxyFixture(t, "first", "second", "first", "local", "")
	defer stopSession()
	clients := &embeddedProxyClients{
		started: make(chan string, 8), canceled: make(chan string, 8),
		gates: map[string]chan error{"first": make(chan error), "second": make(chan error)},
	}
	rd.agentClients = clients
	result := make(chan error, 1)
	go func() { result <- rd.waitForProxyViaWorkloads(t.Context()) }()
	started := []string{embeddedProxyReceive(t, clients.started), embeddedProxyReceive(t, clients.started)}
	require.ElementsMatch(t, []string{"first", "second"}, started)
	errSecond := errors.New("second port-forward is unavailable")
	clients.gates["second"] <- errSecond
	err := embeddedProxyReceive(t, result)
	require.ErrorIs(t, err, errSecond)
	require.ErrorContains(t, err, "proxy-via agent in second failed")
	require.Equal(t, "first", embeddedProxyReceive(t, clients.canceled))
	require.NoError(t, rd.Err())
}

func TestEmbeddedProxyPortForwardCallerCancellationStopsRealWorkloadWaits(t *testing.T) {
	rd, mgr, stopSession := newEmbeddedProxyFixture(t, "first", "second")
	defer stopSession()
	close(mgr.gates["first"])
	close(mgr.gates["second"])
	g := log.NewGroup(rd)
	require.NoError(t, rd.StartWithContext(t.Context(), g, 0))
	embeddedProxyReceive(t, mgr.started)
	embeddedProxyReceive(t, mgr.started)
	startup, stopStartup := context.WithCancel(t.Context())
	defer stopStartup()
	result := make(chan error, 1)
	go func() { result <- rd.waitForProxyViaWorkloads(startup) }()
	select {
	case err := <-result:
		t.Fatalf("absent real port-forward workloads returned before cancellation: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	stopStartup()
	require.ErrorIs(t, embeddedProxyReceive(t, result), context.Canceled)
	require.NoError(t, rd.Err())
	stopSession()
	require.NoError(t, g.Wait())
}

func TestEmbeddedProxyNamedSessionUsesOnlyPersistentNativeManagerProjection(t *testing.T) {
	rd, mgr, stopSession := newEmbeddedProxyFixture(t, "proxy")
	defer stopSession()
	close(mgr.gates["proxy"])
	g := log.NewGroup(rd)
	startup, stopCaller := context.WithCancel(t.Context())
	defer stopCaller()
	require.NoError(t, rd.StartWithContext(startup, g, 0))
	require.Equal(t, "proxy", embeddedProxyReceive(t, mgr.started).name)
	embeddedProxyReceive(t, mgr.watchStarted)
	require.Equal(t, []string{"default"}, embeddedProxyReceive(t, mgr.watchScopes))
	oldIP := netip.MustParseAddr("10.8.0.10")
	old := &manager.AgentPodInfo{Namespace: "default", PodName: "proxy-old", WorkloadName: "proxy", PodIp: oldIP.AsSlice()}
	mgr.watchDeltas <- &manager.AgentPodInfoDelta{Upserts: map[string]*manager.AgentPodInfo{"proxy-old.default": old}}
	require.Eventually(t, func() bool {
		workload, namespace, ok := rd.agentClients.WorkloadForIP(oldIP)
		return ok && workload == "proxy" && namespace == "default"
	}, time.Second, 5*time.Millisecond)
	stopCaller()
	require.NoError(t, rd.Err())
	groupResult := make(chan error, 1)
	go func() { groupResult <- g.Wait() }()
	select {
	case err := <-groupResult:
		t.Fatalf("named embedded manager watch stopped with its startup caller: %v", err)
	case <-mgr.watchStopped:
		t.Fatal("native manager stream stopped with the startup caller")
	case <-time.After(30 * time.Millisecond):
	}
	newIP := netip.MustParseAddr("10.8.0.11")
	newAgent := &manager.AgentPodInfo{Namespace: "default", PodName: "proxy-new", WorkloadName: "proxy", PodIp: newIP.AsSlice()}
	require.NoError(t, rd.ApplyAgentPodsDelta(rd, &rpc.AgentPodsDelta{
		Reset_: true, Upserts: map[string]*manager.AgentPodInfo{"proxy-new.default": newAgent},
	}))
	_, _, foundOld := rd.agentClients.WorkloadForIP(oldIP)
	require.True(t, foundOld, "user daemon relay must not replace the direct source")
	_, _, foundNew := rd.agentClients.WorkloadForIP(newIP)
	require.False(t, foundNew, "user daemon relay must not populate the direct source")
	mgr.watchDeltas <- &manager.AgentPodInfoDelta{Removals: []string{"proxy-old.default"}, Upserts: map[string]*manager.AgentPodInfo{"proxy-new.default": newAgent}}
	require.Eventually(t, func() bool {
		_, _, oldFound := rd.agentClients.WorkloadForIP(oldIP)
		workload, namespace, newFound := rd.agentClients.WorkloadForIP(newIP)
		return !oldFound && newFound && workload == "proxy" && namespace == "default"
	}, time.Second, 5*time.Millisecond)
	stopSession()
	embeddedProxyReceive(t, mgr.watchStopped)
	require.NoError(t, embeddedProxyReceive(t, groupResult))
}

func TestEmbeddedProxyOrdinaryAndLocalSessionsStillUseOnlyUserDaemonRelay(t *testing.T) {
	for _, name := range []string{"", "local"} {
		t.Run("workload="+name, func(t *testing.T) {
			rd, mgr, stopSession := newEmbeddedProxyFixture(t, name)
			defer stopSession()
			g := log.NewGroup(rd)
			require.NoError(t, rd.StartWithContext(t.Context(), g, 0))
			select {
			case <-mgr.watchStarted:
				t.Fatal("ordinary or local-only session started a direct manager agent watch")
			case <-time.After(30 * time.Millisecond):
			}
			ip := netip.MustParseAddr("10.8.0.10")
			a := &manager.AgentPodInfo{Namespace: "default", PodName: "proxy", WorkloadName: "proxy", PodIp: ip.AsSlice()}
			require.NoError(t, rd.ApplyAgentPodsDelta(rd, &rpc.AgentPodsDelta{Reset_: true, Upserts: map[string]*manager.AgentPodInfo{"proxy.default": a}}))
			workload, namespace, found := rd.agentClients.WorkloadForIP(ip)
			require.True(t, found)
			require.Equal(t, "proxy", workload)
			require.Equal(t, "default", namespace)
			require.NoError(t, rd.ApplyAgentPodsDelta(rd, &rpc.AgentPodsDelta{Reset_: true}))
			_, _, found = rd.agentClients.WorkloadForIP(ip)
			require.False(t, found)
			stopSession()
			require.NoError(t, g.Wait())
		})
	}
}

func TestEmbeddedProxyNamedDirectWatchRetriesAgainstNewPhysicalManager(t *testing.T) {
	rd, first, stopSession := newEmbeddedProxyFixture(t, "proxy")
	defer stopSession()
	close(first.gates["proxy"])
	second := newEmbeddedProxyManager()
	secondConn := newWorkloadLookupConnection(t, func(s *grpc.Server) { manager.RegisterManagerServer(s, second) })
	g := log.NewGroup(rd)
	require.NoError(t, rd.StartWithContext(t.Context(), g, 0))
	embeddedProxyReceive(t, first.started)
	embeddedProxyReceive(t, first.watchStarted)
	firstIP, secondIP := netip.MustParseAddr("10.8.0.10"), netip.MustParseAddr("10.8.0.11")
	firstAgent := &manager.AgentPodInfo{Namespace: "default", PodName: "first", WorkloadName: "proxy", PodIp: firstIP.AsSlice()}
	first.watchDeltas <- &manager.AgentPodInfoDelta{Upserts: map[string]*manager.AgentPodInfo{"first.default": firstAgent}}
	require.Eventually(t, func() bool { _, _, ok := rd.agentClients.WorkloadForIP(firstIP); return ok }, time.Second, 5*time.Millisecond)
	rd.managerConnMu.Lock()
	rd.managerConn = secondConn
	rd.managerConnMu.Unlock()
	close(first.watchFailure)
	embeddedProxyReceive(t, first.watchStopped)
	embeddedProxyReceive(t, second.watchStarted)
	require.Equal(t, []string{"default"}, embeddedProxyReceive(t, second.watchScopes))
	secondAgent := &manager.AgentPodInfo{Namespace: "default", PodName: "second", WorkloadName: "proxy", PodIp: secondIP.AsSlice()}
	second.watchDeltas <- &manager.AgentPodInfoDelta{Upserts: map[string]*manager.AgentPodInfo{"second.default": secondAgent}}
	require.Eventually(t, func() bool {
		_, _, gotFirst := rd.agentClients.WorkloadForIP(firstIP)
		_, _, gotSecond := rd.agentClients.WorkloadForIP(secondIP)
		return !gotFirst && gotSecond
	}, time.Second, 5*time.Millisecond)
	stopSession()
	embeddedProxyReceive(t, second.watchStopped)
	require.NoError(t, g.Wait())
}
