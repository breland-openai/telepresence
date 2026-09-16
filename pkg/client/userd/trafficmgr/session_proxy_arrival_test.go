package trafficmgr

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	rootdRpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
)

const proxyTestManagerConnect = 120 * time.Millisecond

func TestNativeProxyViaWaitsPastOrdinaryDeadlineForEffectiveAgentTimeout(t *testing.T) {
	for _, tt := range []struct {
		name          string
		localAgent    time.Duration
		wantAgent     time.Duration
		preAgent      time.Duration
		namedWithVnat bool
	}{
		{name: "manager configured agent wait", wantAgent: 600 * time.Millisecond, preAgent: 30 * time.Second},
		{name: "explicit local agent wait takes priority", localAgent: 900 * time.Millisecond, wantAgent: 900 * time.Millisecond, preAgent: 900 * time.Millisecond, namedWithVnat: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, root, mgr := newProxyArrivalSession(t, tt.localAgent)
			preAgent := client.GetConfig(s).Timeouts().Get(client.TimeoutIntercept)
			require.Equal(t, tt.preAgent, preAgent)
			s.updateClientConfig(t.Context(), []string{"default"})
			require.Equal(t, int32(1), mgr.configRequests.Load())
			require.Equal(t, tt.wantAgent, client.GetConfig(s).Timeouts().Get(client.TimeoutIntercept))
			nc := proxyArrivalNetwork("proxy-fixture")
			if tt.namedWithVnat {
				nc.SubnetViaWorkloads = append([]*rootdRpc.SubnetViaWorkload{{Workload: "local", Subnet: "10.8.0.2/32"}}, nc.SubnetViaWorkloads...)
			}
			ctx, cancel := s.rootDaemonConnectTimeout(t.Context(), nc)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.connectRootDaemon(ctx, nc, nil, false) }()
			request := receiveProxyEvent(t, mgr.started)
			require.Equal(t, "proxy-fixture", request.workload)
			require.InDelta(t, (proxyTestManagerConnect + tt.wantAgent).Seconds(), request.deadline.Sub(request.received).Seconds(), 0.12)
			select {
			case err := <-result:
				t.Fatalf("cold agent exhausted ordinary deadline before its configured arrival: %v", err)
			case <-time.After(2 * proxyTestManagerConnect):
			}
			close(mgr.release)
			require.NoError(t, receiveProxyEvent(t, result))
			require.Equal(t, int32(1), root.networkRequests.Load())
		})
	}
}

func TestNativeProxyViaOrdinaryAndLocalOnlyRetainConnectDeadline(t *testing.T) {
	for _, tt := range []struct {
		name string
		via  []*rootdRpc.SubnetViaWorkload
	}{
		{name: "ordinary native connect"},
		{name: "local vnat only", via: []*rootdRpc.SubnetViaWorkload{{Workload: "local", Subnet: "10.8.0.2/32"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, root, mgr := newProxyArrivalSession(t, 900*time.Millisecond)
			s.updateClientConfig(t.Context(), []string{"default"})
			nc := proxyArrivalNetwork("")
			nc.SubnetViaWorkloads = tt.via
			ctx, cancel := s.rootDaemonConnectTimeout(t.Context(), nc)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.connectRootDaemon(ctx, nc, nil, false) }()
			deadline := receiveProxyEvent(t, root.standardStarted)
			require.InDelta(t, proxyTestManagerConnect.Seconds(), time.Until(deadline).Seconds(), 0.10)
			err := receiveProxyEvent(t, result)
			require.Equal(t, codes.DeadlineExceeded, status.Code(err))
			receiveProxyEvent(t, root.standardCanceled)
			require.Equal(t, int32(0), mgr.agentRequests.Load())
			require.Equal(t, int32(0), root.networkRequests.Load())
		})
	}
}

func TestNativeProxyViaBudgetsEverySerialDistinctColdWorkload(t *testing.T) {
	const arrival = 650 * time.Millisecond
	s, root, mgr := newProxyArrivalSession(t, arrival)
	mgr.releases = map[string]chan struct{}{"first": make(chan struct{}), "second": make(chan struct{})}
	nc := proxyArrivalNetwork("first")
	nc.SubnetViaWorkloads = append(nc.SubnetViaWorkloads, &rootdRpc.SubnetViaWorkload{Workload: "second", Subnet: "10.8.0.2/32"})
	ctx, cancel := s.rootDaemonConnectTimeout(t.Context(), nc)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.connectRootDaemon(ctx, nc, nil, false) }()
	first := receiveProxyEvent(t, mgr.started)
	require.Equal(t, "first", first.workload)
	require.InDelta(t, (proxyTestManagerConnect + 2*arrival).Seconds(), first.deadline.Sub(first.received).Seconds(), 0.12)
	select {
	case unexpected := <-mgr.started:
		t.Fatalf("second manager request began before the first agent arrived: %#v", unexpected)
	case err := <-result:
		t.Fatalf("cold first agent returned prematurely: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	close(mgr.releases["first"])
	second := receiveProxyEvent(t, mgr.started)
	require.Equal(t, "second", second.workload)
	require.WithinDuration(t, first.deadline, second.deadline, 30*time.Millisecond)
	select {
	case err := <-result:
		t.Fatalf("second cold agent exhausted the single-workload deadline: %v", err)
	case <-time.After(375 * time.Millisecond):
	}
	close(mgr.releases["second"])
	require.NoError(t, receiveProxyEvent(t, result))
	require.Equal(t, int32(2), mgr.agentRequests.Load())
	require.Equal(t, int32(1), root.networkRequests.Load())
}

func TestNativeProxyViaDuplicateAndLocalRoutesDoNotAddAgentAllowances(t *testing.T) {
	const arrival = 650 * time.Millisecond
	s, root, mgr := newProxyArrivalSession(t, arrival)
	nc := proxyArrivalNetwork("same")
	nc.SubnetViaWorkloads = append(nc.SubnetViaWorkloads,
		&rootdRpc.SubnetViaWorkload{Workload: "same", Subnet: "10.8.0.2/32"},
		&rootdRpc.SubnetViaWorkload{Workload: "local", Subnet: "10.8.0.3/32"},
		&rootdRpc.SubnetViaWorkload{Subnet: "10.8.0.4/32"}, nil)
	ctx, cancel := s.rootDaemonConnectTimeout(t.Context(), nc)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.connectRootDaemon(ctx, nc, nil, false) }()
	request := receiveProxyEvent(t, mgr.started)
	require.Equal(t, "same", request.workload)
	require.InDelta(t, (proxyTestManagerConnect + arrival).Seconds(), request.deadline.Sub(request.received).Seconds(), 0.12)
	close(mgr.release)
	require.NoError(t, receiveProxyEvent(t, result))
	require.Equal(t, int32(1), mgr.agentRequests.Load())
	require.Equal(t, int32(1), root.networkRequests.Load())
}

func TestNativeProxyViaCallerCancellationStopsTheSecondColdWorkload(t *testing.T) {
	s, root, mgr := newProxyArrivalSession(t, 650*time.Millisecond)
	mgr.releases = map[string]chan struct{}{"first": make(chan struct{}), "second": make(chan struct{})}
	nc := proxyArrivalNetwork("first")
	nc.SubnetViaWorkloads = append(nc.SubnetViaWorkloads, &rootdRpc.SubnetViaWorkload{Workload: "second", Subnet: "10.8.0.2/32"})
	parent, stop := context.WithCancel(t.Context())
	defer stop()
	ctx, cancel := s.rootDaemonConnectTimeout(parent, nc)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.connectRootDaemon(ctx, nc, nil, false) }()
	require.Equal(t, "first", receiveProxyEvent(t, mgr.started).workload)
	close(mgr.releases["first"])
	require.Equal(t, "second", receiveProxyEvent(t, mgr.started).workload)
	started := time.Now()
	stop()
	require.Equal(t, codes.Canceled, status.Code(receiveProxyEvent(t, result)))
	receiveProxyEvent(t, mgr.canceled)
	require.Less(t, time.Since(started), 250*time.Millisecond)
	require.Equal(t, int32(2), mgr.agentRequests.Load())
	require.Equal(t, int32(0), root.networkRequests.Load())
}

type proxyArrivalEmbeddedService struct{ rootDaemonReconnectTestService }

func (proxyArrivalEmbeddedService) RootSessionInProcess() bool { return true }

func TestNativeEmbeddedProxyViaFailedStartupRegistersAndJoinsPersistentWorkers(t *testing.T) {
	s, _, mgr := newProxyArrivalSession(t, 650*time.Millisecond)
	s.service = proxyArrivalEmbeddedService{}
	persistent, stopSession := context.WithCancel(s.Context)
	defer stopSession()
	s.Context = persistent
	nc := proxyArrivalNetwork("missing")
	nc.AgentPodNamespaces = []string{"default"}
	parent, stopCaller := context.WithCancel(t.Context())
	defer stopCaller()
	ctx, stopTimeout := s.rootDaemonConnectTimeout(parent, nc)
	defer stopTimeout()
	var wg sync.WaitGroup
	result := make(chan error, 1)
	go func() { result <- s.connectRootDaemon(ctx, nc, &wg, true) }()
	require.Equal(t, "missing", receiveProxyEvent(t, mgr.started).workload)
	receiveProxyEvent(t, mgr.watchStarted)
	stopCaller()
	require.Equal(t, codes.Canceled, status.Code(receiveProxyEvent(t, result)))
	receiveProxyEvent(t, mgr.canceled)
	require.NoError(t, persistent.Err())
	joined := make(chan struct{}, 1)
	go func() { wg.Wait(); joined <- struct{}{} }()
	select {
	case <-joined:
		t.Fatal("the failed embedded root did not register its still-running native manager watch")
	case <-time.After(30 * time.Millisecond):
	}
	stopSession()
	receiveProxyEvent(t, mgr.watchStopped)
	receiveProxyEvent(t, joined)
}

func TestNativeProxyViaAgentWaitStopsImmediatelyOnCallerCancellation(t *testing.T) {
	s, root, mgr := newProxyArrivalSession(t, 900*time.Millisecond)
	s.updateClientConfig(t.Context(), []string{"default"})
	parent, stop := context.WithCancel(t.Context())
	defer stop()
	nc := proxyArrivalNetwork("proxy-fixture")
	ctx, cancel := s.rootDaemonConnectTimeout(parent, nc)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.connectRootDaemon(ctx, nc, nil, false) }()
	receiveProxyEvent(t, mgr.started)
	started := time.Now()
	stop()
	require.Equal(t, codes.Canceled, status.Code(receiveProxyEvent(t, result)))
	receiveProxyEvent(t, mgr.canceled)
	require.Less(t, time.Since(started), 250*time.Millisecond)
	require.Equal(t, int32(0), root.networkRequests.Load())
}

func TestNativeProxyViaCannotExtendEarlierCallerDeadline(t *testing.T) {
	s, _, mgr := newProxyArrivalSession(t, 900*time.Millisecond)
	s.updateClientConfig(t.Context(), []string{"default"})
	parent, stop := context.WithTimeout(t.Context(), 75*time.Millisecond)
	defer stop()
	nc := proxyArrivalNetwork("proxy-fixture")
	ctx, cancel := s.rootDaemonConnectTimeout(parent, nc)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.connectRootDaemon(ctx, nc, nil, false) }()
	request := receiveProxyEvent(t, mgr.started)
	parentDeadline, ok := parent.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, parentDeadline, request.deadline, 30*time.Millisecond)
	require.Equal(t, codes.DeadlineExceeded, status.Code(receiveProxyEvent(t, result)))
	receiveProxyEvent(t, mgr.canceled)
}

func TestNativeProxyViaInitialRootUsesMergedOrdinaryTimeout(t *testing.T) {
	for _, tt := range []struct {
		name     string
		workload string
	}{
		{name: "ordinary", workload: ""},
		{name: "named", workload: "proxy-fixture"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, root, mgr := newProxyArrivalSessionWithTimeouts(t, 0, 0)
			originalTimeouts := client.GetConfig(s.Cluster).Timeouts()
			require.Equal(t, time.Minute, originalTimeouts.Get(client.TimeoutTrafficManagerConnect))
			s.updateClientConfig(t.Context(), []string{"default"})
			require.Same(t, originalTimeouts, client.GetConfig(s).Timeouts())
			require.Equal(t, 450*time.Millisecond, originalTimeouts.Get(client.TimeoutTrafficManagerConnect))
			require.Equal(t, 600*time.Millisecond, client.GetConfig(s).Timeouts().Get(client.TimeoutIntercept))
			nc := proxyArrivalNetwork(tt.workload)
			ctx, cancel := s.rootDaemonConnectTimeout(t.Context(), nc)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.connectRootDaemon(ctx, nc, nil, false) }()
			if tt.workload == "" {
				deadline := receiveProxyEvent(t, root.standardStarted)
				require.InDelta(t, 0.45, time.Until(deadline).Seconds(), 0.12)
			} else {
				request := receiveProxyEvent(t, mgr.started)
				require.InDelta(t, 1.05, request.deadline.Sub(request.received).Seconds(), 0.12)
			}
			cancel()
			require.Equal(t, codes.Canceled, status.Code(receiveProxyEvent(t, result)))
		})
	}
}

func TestNativeProxyViaRootReconnectUsesEffectiveOrdinaryAndAgentTimeouts(t *testing.T) {
	s, root, mgr := newProxyArrivalSessionWithTimeouts(t, 0, 0)
	s.updateClientConfig(t.Context(), []string{"default"})
	nc := proxyArrivalNetwork("proxy-fixture")
	s.sessionInfo = nc.Session
	s.subnetViaWorkloads = nc.SubnetViaWorkloads
	oldConn, cleanup, err := dialTestRootDaemon(&rootDaemonReconnectTestServer{sessionID: nc.Session.SessionId})
	require.NoError(t, err)
	t.Cleanup(cleanup)
	generation := s.setRootDaemon(rootdRpc.NewDaemonClient(oldConn), oldConn, nc, false)
	result := make(chan struct{}, 1)
	go func() {
		s.reconnectRootDaemon(generation, status.Error(codes.Unavailable, "previous native root connection stopped"))
		result <- struct{}{}
	}()
	request := receiveProxyEvent(t, mgr.started)
	require.InDelta(t, 1.05, request.deadline.Sub(request.received).Seconds(), 0.12)
	select {
	case <-result:
		t.Fatal("native reconnect returned before the cold proxy agent became available")
	case <-time.After(550 * time.Millisecond):
	}
	close(mgr.release)
	receiveProxyEvent(t, result)
	require.Equal(t, int32(1), mgr.agentRequests.Load())
	require.Equal(t, int32(1), root.networkRequests.Load())
	newRoot, newGeneration := s.getRootDaemon()
	require.NotNil(t, newRoot)
	require.Greater(t, newGeneration, generation)
}

func TestNativeProxyViaReportsOnlyItsOwnComposedTimeout(t *testing.T) {
	s, _, _ := newProxyArrivalSessionWithTimeouts(t, 25*time.Millisecond, 25*time.Millisecond)
	for _, tt := range []struct {
		name     string
		workload string
		parent   bool
		wantInfo bool
	}{
		{name: "named configured deadline", workload: "proxy-fixture", wantInfo: true},
		{name: "ordinary configured deadline"},
		{name: "local configured deadline", workload: "local"},
		{name: "earlier caller deadline", workload: "proxy-fixture", parent: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parent := t.Context()
			if tt.parent {
				var cancel context.CancelFunc
				parent, cancel = context.WithTimeout(parent, 10*time.Millisecond)
				defer cancel()
			}
			ctx, cancel := s.rootDaemonConnectTimeout(parent, proxyArrivalNetwork(tt.workload))
			defer cancel()
			<-ctx.Done()
			original := status.Error(codes.DeadlineExceeded, "native RPC timed out")
			err := rootDaemonConnectError(ctx, original)
			require.Equal(t, codes.DeadlineExceeded, status.Code(err))
			if tt.wantInfo {
				require.ErrorContains(t, err, "proxy-via startup exceeded trafficManagerConnect (25ms) plus intercept (25ms) for agent arrival")
			} else {
				require.Same(t, original, err)
				if !tt.parent {
					require.ErrorContains(t, ctx.Err(), "port-forward connection to the traffic manager timed out")
				}
			}
		})
	}
}

func TestNativeProxyViaNetworkWaitPreservesCallerAndConfiguredRPCCodes(t *testing.T) {
	for _, tt := range []struct {
		name, workload string
		caller         bool
		wantCode       codes.Code
	}{
		{name: "named configured deadline", workload: "same", wantCode: codes.DeadlineExceeded},
		{name: "named caller cancellation", workload: "same", caller: true, wantCode: codes.Canceled},
		{name: "ordinary configured deadline", wantCode: codes.DeadlineExceeded},
		{name: "ordinary caller cancellation", caller: true, wantCode: codes.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, root, mgr := newProxyArrivalSessionWithTimeouts(t, 60*time.Millisecond, 60*time.Millisecond)
			root.networkBlocked = true
			root.connectOrdinary = true
			close(mgr.release)
			parent, stop := context.WithCancel(t.Context())
			defer stop()
			nc := proxyArrivalNetwork(tt.workload)
			ctx, cancel := s.rootDaemonConnectTimeout(parent, nc)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.connectRootDaemon(ctx, nc, nil, false) }()
			receiveProxyEvent(t, root.networkStarted)
			if tt.caller {
				stop()
			}
			err := receiveProxyEvent(t, result)
			receiveProxyEvent(t, root.networkCanceled)
			require.Equal(t, tt.wantCode, status.Code(err))
			require.ErrorContains(t, err, "failed to connect to root daemon")
			if tt.workload != "" && !tt.caller {
				require.ErrorContains(t, err, "1 distinct workload(s) (60ms total agent allowance)")
			} else {
				require.NotContains(t, err.Error(), "total agent allowance")
			}
		})
	}
}

func TestNativeProxyViaTimeoutReportsDistinctCountAndTotal(t *testing.T) {
	s, _, _ := newProxyArrivalSessionWithTimeouts(t, 10*time.Millisecond, 10*time.Millisecond)
	nc := proxyArrivalNetwork("first")
	nc.SubnetViaWorkloads = append(nc.SubnetViaWorkloads,
		&rootdRpc.SubnetViaWorkload{Workload: "first", Subnet: "10.8.0.2/32"},
		&rootdRpc.SubnetViaWorkload{Workload: "second", Subnet: "10.8.0.3/32"},
		&rootdRpc.SubnetViaWorkload{Workload: "local", Subnet: "10.8.0.4/32"})
	ctx, cancel := s.rootDaemonConnectTimeout(t.Context(), nc)
	defer cancel()
	<-ctx.Done()
	err := rootDaemonConnectError(ctx, status.Error(codes.DeadlineExceeded, "root timed out"))
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.ErrorContains(t, err, "2 distinct workload(s) (20ms total agent allowance)")
}

func proxyArrivalNetwork(workload string) *rootdRpc.NetworkConfig {
	nc := &rootdRpc.NetworkConfig{Namespace: "default", Session: &manager.SessionInfo{SessionId: "proxy-session"}}
	if workload != "" {
		nc.SubnetViaWorkloads = []*rootdRpc.SubnetViaWorkload{{Workload: workload, Subnet: "10.8.0.1/32"}}
	}
	return nc
}

func newProxyArrivalSession(t *testing.T, localAgent time.Duration) (*session, *proxyArrivalRootServer, *proxyArrivalManagerServer) {
	t.Helper()
	return newProxyArrivalSessionWithTimeouts(t, proxyTestManagerConnect, localAgent)
}

func newProxyArrivalSessionWithTimeouts(t *testing.T, localManager, localAgent time.Duration) (*session, *proxyArrivalRootServer, *proxyArrivalManagerServer) {
	t.Helper()
	config := client.GetDefaultConfig()
	if localManager != 0 {
		config.Timeouts().PrivateTrafficManagerConnect = localManager
	}
	if localAgent != 0 {
		config.Timeouts().PrivateIntercept = localAgent
	}
	ctx, cancel := context.WithCancel(client.WithConfig(t.Context(), config))
	mgr := &proxyArrivalManagerServer{
		started: make(chan proxyArrivalRequest, 8), release: make(chan struct{}), canceled: make(chan struct{}, 8),
		watchStarted: make(chan struct{}, 8), watchStopped: make(chan struct{}, 8),
	}
	mgrConn := dialProxyArrivalManager(t, mgr)
	root := &proxyArrivalRootServer{
		manager: manager.NewManagerClient(mgrConn), standardStarted: make(chan time.Time, 1), standardCanceled: make(chan struct{}, 1),
		networkStarted: make(chan struct{}, 1), networkCanceled: make(chan struct{}, 1),
	}
	rootConn, cleanup, err := dialTestRootDaemon(root)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	s := &session{
		Cluster: &k8s.Cluster{Kubeconfig: &k8s.Kubeconfig{Context: ctx, Namespace: "default"}, MappedNamespaces: []string{"default"}},
		service: rootDaemonReconnectTestService{}, managerConn: mgrConn,
		dialRootDaemon: func(context.Context, bool) (*grpc.ClientConn, error) { return rootConn, nil },
	}
	t.Cleanup(func() { cancel(); s.closeRootDaemon() })
	return s, root, mgr
}

func receiveProxyEvent[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case item := <-ch:
		return item
	case <-time.After(2 * time.Second):
		t.Fatal("native gRPC fixture did not return its expected event")
		var zero T
		return zero
	}
}

type proxyArrivalRequest struct {
	workload string
	deadline time.Time
	received time.Time
}

type proxyArrivalManagerServer struct {
	manager.UnimplementedManagerServer
	started        chan proxyArrivalRequest
	release        chan struct{}
	releases       map[string]chan struct{}
	canceled       chan struct{}
	watchStarted   chan struct{}
	watchStopped   chan struct{}
	configRequests atomic.Int32
	agentRequests  atomic.Int32
}

func (s *proxyArrivalManagerServer) GetClientConfig(context.Context, *emptypb.Empty) (*manager.CLIConfig, error) {
	s.configRequests.Add(1)
	return &manager.CLIConfig{ConfigYaml: []byte("timeouts:\n  intercept: 600ms\n  trafficManagerConnect: 450ms\n")}, nil
}

func (s *proxyArrivalManagerServer) EnsureAgent(ctx context.Context, request *manager.EnsureAgentRequest) (*manager.AgentInfoSnapshot, error) {
	s.agentRequests.Add(1)
	deadline, _ := ctx.Deadline()
	s.started <- proxyArrivalRequest{workload: request.Name, deadline: deadline, received: time.Now()}
	release := s.release
	if forWorkload, ok := s.releases[request.Name]; ok {
		release = forWorkload
	}
	select {
	case <-release:
		return &manager.AgentInfoSnapshot{Agents: []*manager.AgentInfo{{Name: request.Name}}}, nil
	case <-ctx.Done():
		s.canceled <- struct{}{}
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (s *proxyArrivalManagerServer) WatchAgentPodsInNamespacesDelta(_ *manager.AgentsRequest, stream grpc.ServerStreamingServer[manager.AgentPodInfoDelta]) error {
	s.watchStarted <- struct{}{}
	defer func() { s.watchStopped <- struct{}{} }()
	<-stream.Context().Done()
	return stream.Context().Err()
}

type proxyArrivalRootServer struct {
	rootdRpc.UnimplementedDaemonServer
	manager          manager.ManagerClient
	standardStarted  chan time.Time
	standardCanceled chan struct{}
	connectOrdinary  bool
	networkBlocked   bool
	networkStarted   chan struct{}
	networkCanceled  chan struct{}
	networkRequests  atomic.Int32
}

func (s *proxyArrivalRootServer) Connect(ctx context.Context, nc *rootdRpc.NetworkConfig) (*rootdRpc.DaemonStatus, error) {
	ensured := make(map[string]struct{})
	for _, via := range nc.SubnetViaWorkloads {
		if name := via.GetWorkload(); name != "" && name != "local" {
			if _, ok := ensured[name]; ok {
				continue
			}
			ensured[name] = struct{}{}
			if _, err := s.manager.EnsureAgent(ctx, &manager.EnsureAgentRequest{Session: nc.Session, Name: name}); err != nil {
				return nil, err
			}
		}
	}
	if len(ensured) > 0 || s.connectOrdinary {
		return &rootdRpc.DaemonStatus{OutboundConfig: nc}, nil
	}
	deadline, _ := ctx.Deadline()
	s.standardStarted <- deadline
	<-ctx.Done()
	s.standardCanceled <- struct{}{}
	return nil, status.FromContextError(ctx.Err()).Err()
}

func (s *proxyArrivalRootServer) WaitForNetwork(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	s.networkRequests.Add(1)
	if s.networkBlocked {
		s.networkStarted <- struct{}{}
		<-ctx.Done()
		s.networkCanceled <- struct{}{}
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &emptypb.Empty{}, nil
}

func (*proxyArrivalRootServer) SetDNSTopLevelDomains(context.Context, *rootdRpc.Domains) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (*proxyArrivalRootServer) ActivityWatcher(_ *emptypb.Empty, stream grpc.ServerStreamingServer[rootdRpc.Activity]) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

func dialProxyArrivalManager(t *testing.T, managerServer manager.ManagerServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	manager.RegisterManagerServer(server, managerServer)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///proxy-manager", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(); server.Stop(); _ = listener.Close() })
	return conn
}
