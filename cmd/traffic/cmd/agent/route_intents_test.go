package agent

import (
	"context"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/agent/fwd"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func exampleAgentRouteSnapshot() *rpc.RouteIntentSnapshot {
	return &rpc.RouteIntentSnapshot{SyncState: rpc.RouteIntentSnapshot_AUTHORITATIVE, ManagerEpoch: "manager-1", Intents: []*rpc.RouteIntent{{
		InterceptId: "client:web", Incarnation: "creation-1", Revision: 1, State: rpc.RouteIntent_DESIRED, RoutingKey: "developer-key",
		Targets: []*rpc.RouteIntentTarget{{Namespace: "development", WorkloadKind: "Deployment", WorkloadName: "web", ContainerPort: 8080}},
	}}}
}

type recordingGuardedState struct {
	InterceptState
	sets, marks int
}

func (s *recordingGuardedState) supportsRouteGuards() bool            { return true }
func (s *recordingGuardedState) setRouteGuards(_, _ []fwd.RouteGuard) { s.sets++ }
func (s *recordingGuardedState) markRouteSnapshotInstalled() {
	if s.sets > s.marks {
		s.marks++
	}
}

func TestAgentMarksStartupSafeOnlyAfterValidAuthoritativeGuards(t *testing.T) {
	ctx := t.Context()
	s := &state{Config: &fakeConfig{sidecar: &agentconfig.Sidecar{Namespace: "development", WorkloadKind: k8sapi.DeploymentKind, WorkloadName: "web", RequireAuthoritativeRoutes: true}}}
	forwarder := fwd.NewHTTPMediatedTCPInterceptor(ctx, types.PortAndProto{Port: 8080, Proto: types.ProtoTCP}, tunnel.AgentToClient, nil, netip.MustParseAddrPort("127.0.0.1:9999"))
	route := &recordingGuardedState{InterceptState: s.NewInterceptState(forwarder, agentconfig.NewInterceptTarget([]*agentconfig.Intercept{{ContainerPort: 8080, Protocol: types.ProtoTCP}}), "web")}
	s.AddInterceptState(route)
	applied, err := s.HandleRouteIntents(ctx, &rpc.RouteIntentSnapshot{SyncState: rpc.RouteIntentSnapshot_SYNCHRONIZING})
	require.NoError(t, err)
	require.False(t, applied)
	require.Zero(t, route.marks)
	malformed := exampleAgentRouteSnapshot()
	malformed.Intents[0].Targets[0].ContainerPort = 9999
	applied, err = s.HandleRouteIntents(ctx, malformed)
	require.Error(t, err)
	require.False(t, applied)
	require.Zero(t, route.sets)
	require.Zero(t, route.marks)
	applied, err = s.HandleRouteIntents(ctx, &rpc.RouteIntentSnapshot{SyncState: rpc.RouteIntentSnapshot_AUTHORITATIVE, ManagerEpoch: "current"})
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, 1, route.sets)
	require.Equal(t, 1, route.marks, "even an empty authoritative list unlocks unrelated-key staging, after installation")
}

func TestRouteIntentStateScopesAndRetainsUntilAuthoritative(t *testing.T) {
	ctx := context.Background()
	s := &state{Config: &fakeConfig{sidecar: &agentconfig.Sidecar{
		Namespace: "development", WorkloadKind: k8sapi.DeploymentKind, WorkloadName: "web", RequireAuthoritativeRoutes: true,
	}}}
	makeForwarder := func(port uint16, protocol types.Proto) fwd.Interceptor {
		var f fwd.Interceptor
		if port == 8080 && protocol == types.ProtoTCP {
			f = fwd.NewHTTPMediatedTCPInterceptor(ctx, types.PortAndProto{Port: port, Proto: protocol}, tunnel.AgentToClient, nil, netip.MustParseAddrPort("127.0.0.1:9999"))
		} else {
			f = fwd.NewInterceptor(ctx, types.PortAndProto{Port: port, Proto: protocol}, tunnel.AgentToClient, netip.MustParseAddrPort("127.0.0.1:9999"))
		}
		s.AddInterceptState(s.NewInterceptState(f, agentconfig.NewInterceptTarget([]*agentconfig.Intercept{{ContainerPort: port, Protocol: protocol}}), "web"))
		return f
	}
	web, ordinary, udp := makeForwarder(8080, types.ProtoTCP), makeForwarder(9090, types.ProtoTCP), makeForwarder(8080, types.ProtoUDP)
	require.True(t, web.IsHTTP(), "eligible ports mediate even with no guards yet")
	snapshot := exampleAgentRouteSnapshot()
	// A manager may choose to send more targets than the agent owns. An unrelated
	// workload at a local port must not activate that local forwarder.
	snapshot.Intents[0].Targets = append(snapshot.Intents[0].Targets,
		&rpc.RouteIntentTarget{Namespace: "development", WorkloadKind: "Deployment", WorkloadName: "unrelated", ContainerPort: 9090},
		&rpc.RouteIntentTarget{Namespace: "development", WorkloadKind: "StatefulSet", WorkloadName: "web", ContainerPort: 9090},
		&rpc.RouteIntentTarget{Namespace: "other", WorkloadKind: "Deployment", WorkloadName: "web", ContainerPort: 9090},
	)
	applied, err := s.HandleRouteIntents(ctx, snapshot)
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, web.IsHTTP())
	require.False(t, ordinary.IsHTTP())
	require.False(t, udp.IsHTTP())
	require.Equal(t, []*rpc.RouteIntentAck{{InterceptId: "client:web", Incarnation: "creation-1", Revision: 1}}, s.RouteIntentAcknowledgments(snapshot))
	unsupported := proto.CloneOf(snapshot)
	unsupported.Intents[0].Targets = append(unsupported.Intents[0].Targets,
		&rpc.RouteIntentTarget{Namespace: "development", WorkloadKind: "Deployment", WorkloadName: "web", ContainerPort: 9090})
	applied, err = s.HandleRouteIntents(ctx, unsupported)
	require.NoError(t, err)
	require.True(t, applied)
	require.Empty(t, s.RouteIntentAcknowledgments(unsupported), "one unsupported target must prevent the entire desired route acknowledgment")
	require.False(t, ordinary.IsHTTP(), "a desired header must never move raw or undeclared ports into HTTP")
	unsupported.Intents[0].State = rpc.RouteIntent_REMOVED
	require.Len(t, s.RouteIntentAcknowledgments(unsupported), 1, "removal can be acknowledged even on a raw port")

	applied, err = s.HandleRouteIntents(ctx, &rpc.RouteIntentSnapshot{SyncState: rpc.RouteIntentSnapshot_SYNCHRONIZING, ManagerEpoch: "manager-2"})
	require.NoError(t, err)
	require.False(t, applied)
	require.True(t, web.IsHTTP())

	malformed := proto.CloneOf(snapshot)
	malformed.Intents[0].Targets = append(malformed.Intents[0].Targets,
		&rpc.RouteIntentTarget{Namespace: "development", WorkloadKind: "Deployment", WorkloadName: "web", ContainerPort: 9191})
	applied, err = s.HandleRouteIntents(ctx, malformed)
	require.ErrorContains(t, err, "9191")
	require.False(t, applied)
	require.True(t, web.IsHTTP())
	require.False(t, ordinary.IsHTTP())

	removed := proto.CloneOf(snapshot)
	removed.Intents[0].State = rpc.RouteIntent_REMOVED
	removed.Intents[0].Revision++
	applied, err = s.HandleRouteIntents(ctx, removed)
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, web.IsHTTP(), "an authoritative removal must not create raw bypass connections")

	_, err = s.HandleRouteIntents(ctx, &rpc.RouteIntentSnapshot{SyncState: rpc.RouteIntentSnapshot_AUTHORITATIVE})
	require.ErrorContains(t, err, "manager epoch")
}

type authoritativeTestManager struct {
	rpc.ManagerClient
	watch func(context.Context) (grpc.ServerStreamingClient[rpc.RouteIntentSnapshot], error)
	ack   func(context.Context, *rpc.RouteIntentAckRequest) (*emptypb.Empty, error)
}

func (m *authoritativeTestManager) WatchRouteIntents(ctx context.Context, _ *rpc.SessionInfo, _ ...grpc.CallOption) (grpc.ServerStreamingClient[rpc.RouteIntentSnapshot], error) {
	return m.watch(ctx)
}

func (m *authoritativeTestManager) AcknowledgeRouteIntents(ctx context.Context, request *rpc.RouteIntentAckRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	if m.ack == nil {
		return &emptypb.Empty{}, nil
	}
	return m.ack(ctx, request)
}

type authoritativeTestState struct {
	State
	handleRoutes func(context.Context, *rpc.RouteIntentSnapshot) (bool, error)
	handleLive   func(context.Context, []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest
}

func (s *authoritativeTestState) HandleRouteIntents(ctx context.Context, snapshot *rpc.RouteIntentSnapshot) (bool, error) {
	return s.handleRoutes(ctx, snapshot)
}

func (s *authoritativeTestState) RouteIntentAcknowledgments(snapshot *rpc.RouteIntentSnapshot) (result []*rpc.RouteIntentAck) {
	for _, intent := range snapshot.GetIntents() {
		result = append(result, &rpc.RouteIntentAck{InterceptId: intent.InterceptId, Incarnation: intent.Incarnation, Revision: intent.Revision})
	}
	return result
}

func (s *authoritativeTestState) HandleIntercepts(ctx context.Context, intercepts []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest {
	return s.handleLive(ctx, intercepts)
}

func TestAuthoritativeRouteReadinessWaitsForInstallAndSurvivesReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(interceptReadinessContext(t))
	defer cancel()
	readiness := newAuthoritativeRouteReadiness()
	require.NoError(t, readiness.setUnsynchronized(ctx))
	liveHandled := make(chan struct{})
	seen := make(chan rpc.RouteIntentSnapshot_SyncState, 4)
	allowInstall := make(chan struct{})
	allowACK := make(chan struct{})
	ackRequests := make(chan *rpc.RouteIntentAckRequest, 1)
	var ackAttempts atomic.Int32
	st := &authoritativeTestState{
		handleLive: func(context.Context, []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest {
			close(liveHandled)
			return nil
		},
		handleRoutes: func(ctx context.Context, snapshot *rpc.RouteIntentSnapshot) (bool, error) {
			seen <- snapshot.SyncState
			if snapshot.SyncState != rpc.RouteIntentSnapshot_AUTHORITATIVE {
				return false, nil
			}
			select {
			case <-allowInstall:
				return true, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		},
	}
	live := make(chan interceptSnapshot, 1)
	live <- interceptSnapshot{}
	legacyDone := make(chan error, 1)
	go func() { legacyDone <- handleInterceptLoop(ctx, nil, nil, live, st, readiness) }()
	awaitReadinessSignal(t, liveHandled, "ordinary empty intercept snapshot")
	require.False(t, readinessMarkerExists(ctx))

	opened := make(chan *fakeInterceptReadinessStream[rpc.RouteIntentSnapshot], 2)
	manager := &authoritativeTestManager{
		watch: func(ctx context.Context) (grpc.ServerStreamingClient[rpc.RouteIntentSnapshot], error) {
			s := newFakeInterceptReadinessStream[rpc.RouteIntentSnapshot](ctx)
			opened <- s
			return s, nil
		},
		ack: func(ctx context.Context, request *rpc.RouteIntentAckRequest) (*emptypb.Empty, error) {
			if ackAttempts.Add(1) == 1 {
				ackRequests <- request
				return nil, status.Error(codes.Unavailable, "manager still starting")
			}
			select {
			case <-allowACK:
				return &emptypb.Empty{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	watchDone := make(chan error, 1)
	session := &rpc.SessionInfo{SessionId: "agent:stable-pod"}
	go func() { watchDone <- routeIntentWatchLoop(ctx, manager, session, st, time.Millisecond, readiness) }()
	first := <-opened
	first.values <- &rpc.RouteIntentSnapshot{SyncState: rpc.RouteIntentSnapshot_SYNCHRONIZING}
	require.Equal(t, rpc.RouteIntentSnapshot_SYNCHRONIZING, <-seen)
	require.False(t, readinessMarkerExists(ctx))
	require.Zero(t, ackAttempts.Load())
	first.values <- exampleAgentRouteSnapshot()
	require.Equal(t, rpc.RouteIntentSnapshot_AUTHORITATIVE, <-seen)
	require.False(t, readinessMarkerExists(ctx), "readiness cannot precede installing the guards")
	require.Zero(t, ackAttempts.Load())
	close(allowInstall)
	request := <-ackRequests
	require.Equal(t, session, request.Session)
	require.Equal(t, []*rpc.RouteIntentAck{{InterceptId: "client:web", Incarnation: "creation-1", Revision: 1}}, request.Applied)
	require.Equal(t, processRouteGuardInstance, request.AgentGuardInstance)
	require.NoError(t, request.AppliedAt.CheckValid())
	require.False(t, request.AppliedAt.AsTime().Before(processRouteGuardStartedAt))
	require.Eventually(t, func() bool { return ackAttempts.Load() == 2 }, time.Second, time.Millisecond)
	require.False(t, readinessMarkerExists(ctx), "readiness cannot precede manager acknowledgment")
	close(allowACK)
	require.Eventually(t, func() bool { return readinessMarkerExists(ctx) }, time.Second, time.Millisecond)
	first.errors <- status.Error(codes.Unavailable, "manager restarted")
	second := <-opened
	second.values <- &rpc.RouteIntentSnapshot{SyncState: rpc.RouteIntentSnapshot_SYNCHRONIZING, ManagerEpoch: "next-manager"}
	require.Equal(t, rpc.RouteIntentSnapshot_SYNCHRONIZING, <-seen)
	require.True(t, readinessMarkerExists(ctx), "installed guards remain usable during the new manager's startup")
	// A whole TalkToManager session boundary uses the same State-owned readiness.
	require.NoError(t, readiness.setUnsynchronized(ctx))
	require.True(t, readinessMarkerExists(ctx))
	cancel()
	require.NoError(t, <-watchDone)
	require.NoError(t, <-legacyDone)
}

func TestAuthoritativeRouteReadinessDoesNotFallbackToUnsupportedManager(t *testing.T) {
	ctx := interceptReadinessContext(t)
	readiness := newAuthoritativeRouteReadiness()
	require.NoError(t, readiness.setUnsynchronized(ctx))
	manager := &authoritativeTestManager{watch: func(context.Context) (grpc.ServerStreamingClient[rpc.RouteIntentSnapshot], error) {
		return nil, status.Error(codes.Unimplemented, "old manager")
	}}
	err := routeIntentWatchLoop(ctx, manager, nil, nil, time.Millisecond, readiness)
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.False(t, readinessMarkerExists(ctx))
}

func TestAuthoritativeRouteAcknowledgesEmptyBaselineAndTombstone(t *testing.T) {
	ctx, cancel := context.WithCancel(interceptReadinessContext(t))
	defer cancel()
	readiness := newAuthoritativeRouteReadiness()
	require.NoError(t, readiness.setUnsynchronized(ctx))
	opened := make(chan *fakeInterceptReadinessStream[rpc.RouteIntentSnapshot], 1)
	installed := make(chan *rpc.RouteIntentSnapshot, 2)
	acks := make(chan *rpc.RouteIntentAckRequest, 2)
	st := &authoritativeTestState{handleRoutes: func(_ context.Context, snapshot *rpc.RouteIntentSnapshot) (bool, error) {
		installed <- snapshot
		return true, nil
	}}
	manager := &authoritativeTestManager{
		watch: func(ctx context.Context) (grpc.ServerStreamingClient[rpc.RouteIntentSnapshot], error) {
			stream := newFakeInterceptReadinessStream[rpc.RouteIntentSnapshot](ctx)
			opened <- stream
			return stream, nil
		},
		ack: func(_ context.Context, request *rpc.RouteIntentAckRequest) (*emptypb.Empty, error) {
			select {
			case <-installed:
			default:
				t.Error("manager called before installing snapshot")
			}
			acks <- request
			return &emptypb.Empty{}, nil
		},
	}
	done := make(chan error, 1)
	session := &rpc.SessionInfo{SessionId: "agent:pod"}
	go func() { done <- routeIntentWatchLoop(ctx, manager, session, st, time.Millisecond, readiness) }()
	stream := <-opened
	stream.values <- &rpc.RouteIntentSnapshot{SyncState: rpc.RouteIntentSnapshot_AUTHORITATIVE, ManagerEpoch: "current"}
	first := <-acks
	require.Equal(t, session, first.Session)
	require.Empty(t, first.Applied)
	require.Equal(t, processRouteGuardInstance, first.AgentGuardInstance)
	require.NoError(t, first.AppliedAt.CheckValid())
	require.Eventually(t, func() bool { return readinessMarkerExists(ctx) }, time.Second, time.Millisecond)
	removed := exampleAgentRouteSnapshot()
	removed.Intents[0].State, removed.Intents[0].Revision = rpc.RouteIntent_REMOVED, 7
	stream.values <- removed
	require.Equal(t, []*rpc.RouteIntentAck{{InterceptId: "client:web", Incarnation: "creation-1", Revision: 7}}, (<-acks).Applied)
	cancel()
	require.NoError(t, <-done)
}

func TestRouteIntentAckFreshlyStampsEachUnaryAttempt(t *testing.T) {
	request := &rpc.RouteIntentAckRequest{Session: &rpc.SessionInfo{SessionId: "agent:pod"}, AgentGuardInstance: processRouteGuardInstance}
	first := stampRouteIntentAck(request)
	require.NoError(t, first.AppliedAt.CheckValid())
	time.Sleep(time.Millisecond)
	second := stampRouteIntentAck(request)
	require.True(t, first.AppliedAt.AsTime().Before(second.AppliedAt.AsTime()))
	require.Equal(t, first.AgentGuardInstance, second.AgentGuardInstance)
	require.Nil(t, request.AppliedAt, "periodic refresh must retain the unstamped base")
}

func TestAuthoritativeRouteRefreshesProofOnlyWhileWatchIsHealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(interceptReadinessContext(t))
	defer cancel()
	readiness := newAuthoritativeRouteReadiness()
	require.NoError(t, readiness.setUnsynchronized(ctx))
	opened := make(chan *fakeInterceptReadinessStream[rpc.RouteIntentSnapshot], 1)
	installed := make(chan rpc.RouteIntentSnapshot_SyncState, 3)
	var ackAttempts atomic.Int32
	var failOne atomic.Bool
	lastVersion := new(atomic.Uint64)
	st := &authoritativeTestState{handleRoutes: func(_ context.Context, snapshot *rpc.RouteIntentSnapshot) (bool, error) {
		installed <- snapshot.SyncState
		return snapshot.SyncState == rpc.RouteIntentSnapshot_AUTHORITATIVE, nil
	}}
	manager := &authoritativeTestManager{
		watch: func(ctx context.Context) (grpc.ServerStreamingClient[rpc.RouteIntentSnapshot], error) {
			stream := newFakeInterceptReadinessStream[rpc.RouteIntentSnapshot](ctx)
			opened <- stream
			return stream, nil
		},
		ack: func(_ context.Context, request *rpc.RouteIntentAckRequest) (*emptypb.Empty, error) {
			ackAttempts.Add(1)
			if failOne.CompareAndSwap(true, false) {
				return nil, status.Error(codes.Unavailable, "temporary refresh error")
			}
			lastVersion.Store(request.GetApplied()[0].GetRevision())
			return &emptypb.Empty{}, nil
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- routeIntentWatchLoopWithRefresh(ctx, manager, &rpc.SessionInfo{SessionId: "agent:pod"}, st, time.Millisecond, 15*time.Millisecond, readiness)
	}()
	stream := <-opened
	stream.values <- exampleAgentRouteSnapshot()
	require.Equal(t, rpc.RouteIntentSnapshot_AUTHORITATIVE, <-installed)
	require.Eventually(t, func() bool { return readinessMarkerExists(ctx) }, time.Second, time.Millisecond)
	failOne.Store(true)
	require.Eventually(t, func() bool { return ackAttempts.Load() >= 4 }, time.Second, 5*time.Millisecond)
	require.True(t, readinessMarkerExists(ctx), "a transient periodic proof failure must not revoke a running agent's readiness")
	require.EqualValues(t, 1, lastVersion.Load())
	stream.values <- &rpc.RouteIntentSnapshot{SyncState: rpc.RouteIntentSnapshot_SYNCHRONIZING, ManagerEpoch: "replacement"}
	require.Equal(t, rpc.RouteIntentSnapshot_SYNCHRONIZING, <-installed)
	// Allow an already issued unary request to return before measuring the stopped refreshes.
	time.Sleep(30 * time.Millisecond)
	pausedAt := ackAttempts.Load()
	require.Eventually(t, func() bool { return readinessMarkerExists(ctx) }, time.Second, time.Millisecond)
	require.Never(t, func() bool { return ackAttempts.Load() != pausedAt }, 100*time.Millisecond, 5*time.Millisecond)
	next := exampleAgentRouteSnapshot()
	next.ManagerEpoch, next.Intents[0].Revision = "replacement", 2
	stream.values <- next
	require.Equal(t, rpc.RouteIntentSnapshot_AUTHORITATIVE, <-installed)
	require.Eventually(t, func() bool { return ackAttempts.Load() >= pausedAt+3 && lastVersion.Load() == 2 }, time.Second, 5*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestAuthoritativeRouteReadinessNewProcessClearsOldMarker(t *testing.T) {
	ctx := interceptReadinessContext(t)
	previous := newAuthoritativeRouteReadiness()
	require.NoError(t, previous.setUnsynchronized(ctx))
	require.NoError(t, previous.setSynchronized(ctx, previous.currentGeneration()))
	require.True(t, readinessMarkerExists(ctx))
	newProcess := newAuthoritativeRouteReadiness()
	require.NoError(t, newProcess.setUnsynchronized(ctx))
	require.False(t, readinessMarkerExists(ctx), "a new process has not installed the old process's guards")
}

func TestProvisionalRouteGuardOnlyAcceptsExactDurableRoute(t *testing.T) {
	runtime := &rpc.InterceptInfo{Id: "client:web", RouteIncarnation: "creation", Spec: &rpc.InterceptSpec{
		HeaderFilters: map[string]string{"x-local-routing-key": "developer"},
	}}
	guard, accepted := provisionalRouteGuard(runtime)
	require.True(t, accepted)
	require.Equal(t, fwd.RouteGuard{RoutingKey: "developer", InterceptID: "client:web", Incarnation: "creation"}, guard)
	for _, edit := range []func(*rpc.InterceptInfo){
		func(ii *rpc.InterceptInfo) { ii.RouteIncarnation = "" },
		func(ii *rpc.InterceptInfo) { ii.Spec.HeaderFilters["x-local-routing-key"] = "dev.*" },
		func(ii *rpc.InterceptInfo) { ii.Spec.HeaderFilters["extra"] = "value" },
		func(ii *rpc.InterceptInfo) { ii.Spec.PathFilters = []string{"/one-path"} },
		func(ii *rpc.InterceptInfo) { ii.Spec.Wiretap = true },
	} {
		invalid := proto.CloneOf(runtime)
		edit(invalid)
		_, accepted = provisionalRouteGuard(invalid)
		require.False(t, accepted)
	}
}
