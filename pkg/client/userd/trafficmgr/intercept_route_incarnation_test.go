package trafficmgr

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/clog/testutil"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
)

type routeCreationOutcome struct {
	ii  *manager.InterceptInfo
	err error
}

type routeCreationTestClient struct {
	manager.ManagerClient
	results  []routeCreationOutcome
	requests []*manager.CreateInterceptRequest
	lookup   *manager.InterceptInfo
	getErr   error
	gets     []*manager.GetInterceptRequest
}

func (m *routeCreationTestClient) CreateIntercept(_ context.Context, req *manager.CreateInterceptRequest, _ ...grpc.CallOption) (*manager.InterceptInfo, error) {
	m.requests = append(m.requests, req)
	r := m.results[0]
	m.results = m.results[1:]
	return r.ii, r.err
}

func (m *routeCreationTestClient) GetIntercept(_ context.Context, req *manager.GetInterceptRequest, _ ...grpc.CallOption) (*manager.InterceptInfo, error) {
	m.gets = append(m.gets, req)
	return m.lookup, m.getErr
}

func routeInfo(name, incarnation string) *manager.InterceptInfo {
	return &manager.InterceptInfo{
		Id:               "session:" + name,
		Spec:             &manager.InterceptSpec{Name: name},
		Disposition:      manager.InterceptDispositionType_WAITING,
		RouteIncarnation: incarnation,
	}
}

func TestCreateInterceptGeneratesNewIncarnationOnlyForEachCreation(t *testing.T) {
	s := newRouteFenceSession(t)
	spec := &manager.InterceptSpec{Name: "web"}
	first := s.newInterceptCreation(spec)
	second := s.newInterceptCreation(spec)
	_, err := uuid.Parse(first.RouteIncarnation)
	require.NoError(t, err)
	_, err = uuid.Parse(second.RouteIncarnation)
	require.NoError(t, err)
	require.NotEqual(t, first.RouteIncarnation, second.RouteIncarnation)
	require.Empty(t, s.newCreateInterceptRequest(spec, "").RouteIncarnation, "a prepare request must not create or reserve a route")
}

func TestCreateInterceptRetriesSameRouteIncarnation(t *testing.T) {
	for _, first := range []error{
		status.Error(codes.Unavailable, "connection lost"),
		status.Error(codes.NotFound, "Client session session does not exist"),
	} {
		t.Run(status.Code(first).String(), func(t *testing.T) {
			req := &manager.CreateInterceptRequest{
				Session:          &manager.SessionInfo{SessionId: "session"},
				InterceptSpec:    &manager.InterceptSpec{Name: "web"},
				RouteIncarnation: "same-attempt",
			}
			want := routeInfo("web", "same-attempt")
			mgr := &routeCreationTestClient{results: []routeCreationOutcome{{err: first}, {ii: want}}}
			reconnects := 0
			got, err := createInterceptWithRetry(t.Context(), req, func() (manager.ManagerClient, uint64) { return mgr, 7 }, func(generation uint64) error {
				reconnects++
				require.EqualValues(t, 7, generation)
				return nil
			})
			require.NoError(t, err)
			require.Same(t, want, got)
			require.Equal(t, 1, reconnects)
			require.Len(t, mgr.requests, 2)
			require.Same(t, mgr.requests[0], mgr.requests[1])
			require.Equal(t, "same-attempt", mgr.requests[1].RouteIncarnation)
			require.Empty(t, mgr.gets)
		})
	}
}

func TestCreateInterceptRetryOnlyAcceptsAnAcknowledgedMatchingRoute(t *testing.T) {
	first := status.Error(codes.Unavailable, "response lost")
	for _, tc := range []struct {
		name string
		info *manager.InterceptInfo
		err  error
		want bool
	}{
		{name: "same creation", info: routeInfo("web", "mine"), want: true},
		{name: "older manager with no token", info: routeInfo("web", "")},
		{name: "different creation", info: routeInfo("web", "theirs")},
		{name: "cannot verify", err: status.Error(codes.NotFound, "not found")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &manager.CreateInterceptRequest{
				Session:          &manager.SessionInfo{SessionId: "session"},
				InterceptSpec:    &manager.InterceptSpec{Name: "web"},
				RouteIncarnation: "mine",
			}
			mgr := &routeCreationTestClient{
				results: []routeCreationOutcome{{err: first}, {err: status.Error(codes.AlreadyExists, "already exists")}},
				lookup:  tc.info, getErr: tc.err,
			}
			got, err := createInterceptWithRetry(t.Context(), req, func() (manager.ManagerClient, uint64) { return mgr, 1 }, func(uint64) error { return nil })
			if tc.want {
				require.NoError(t, err)
				require.Same(t, tc.info, got)
			} else {
				require.ErrorIs(t, err, first)
				require.Nil(t, got)
			}
			require.Len(t, mgr.gets, 1)
			require.Equal(t, "session", mgr.gets[0].Session.SessionId)
			require.Equal(t, "web", mgr.gets[0].Name)
		})
	}
}

func TestCreateInterceptDoesNotRetryExistingRouteOrMissingWorkload(t *testing.T) {
	for _, first := range []error{
		status.Error(codes.AlreadyExists, "already exists"),
		status.Error(codes.NotFound, "workload does not exist"),
	} {
		mgr := &routeCreationTestClient{results: []routeCreationOutcome{{err: first}}}
		_, err := createInterceptWithRetry(t.Context(), &manager.CreateInterceptRequest{}, func() (manager.ManagerClient, uint64) { return mgr, 1 }, func(uint64) error {
			t.Fatal("permanent creation error must not reconnect")
			return nil
		})
		require.ErrorIs(t, err, first)
		require.Len(t, mgr.requests, 1)
	}
}

func newRouteFenceSession(t *testing.T) *session {
	t.Helper()
	ctx := client.WithConfig(testutil.NewContext(t, false), client.GetDefaultConfig())
	return &session{
		Cluster:          &k8s.Cluster{Kubeconfig: &k8s.Kubeconfig{Context: ctx}},
		sessionInfo:      &manager.SessionInfo{SessionId: "session"},
		interceptWaiters: make(map[string]*awaitIntercept),
	}
}

func TestAcknowledgedRouteIncarnationSurvivesSnapshotsAndReconnectData(t *testing.T) {
	for _, beforeWatch := range []bool{false, true} {
		name := "after watch"
		if beforeWatch {
			name = "before watch"
		}
		t.Run(name, func(t *testing.T) {
			s := newRouteFenceSession(t)
			s.interceptWaiters["web"] = &awaitIntercept{routeIncarnation: "acknowledged"}
			if beforeWatch {
				s.rememberRouteIncarnation(routeInfo("web", "acknowledged"), "acknowledged")
			}
			s.setCurrentIntercepts([]*manager.InterceptInfo{routeInfo("web", "")})
			if !beforeWatch {
				s.rememberRouteIncarnation(routeInfo("web", "acknowledged"), "acknowledged")
			}
			delete(s.interceptWaiters, "web")
			s.setCurrentIntercepts([]*manager.InterceptInfo{routeInfo("web", "")})
			reconnectInfos := s.getCurrentInterceptInfos()
			require.Len(t, reconnectInfos, 1)
			require.Equal(t, "acknowledged", reconnectInfos[0].RouteIncarnation)
		})
	}

	t.Run("older manager never echoed proposal", func(t *testing.T) {
		s := newRouteFenceSession(t)
		s.interceptWaiters["web"] = &awaitIntercept{routeIncarnation: "not-acknowledged"}
		s.rememberRouteIncarnation(routeInfo("web", ""), "not-acknowledged")
		s.setCurrentIntercepts([]*manager.InterceptInfo{routeInfo("web", "")})
		require.Empty(t, s.getCurrentInterceptInfos()[0].RouteIncarnation)
	})

	t.Run("late response must not fence a newer legacy creation", func(t *testing.T) {
		s := newRouteFenceSession(t)
		s.interceptWaiters["web"] = &awaitIntercept{routeIncarnation: "new-proposal"}
		s.setCurrentIntercepts([]*manager.InterceptInfo{routeInfo("web", "")})
		s.rememberRouteIncarnation(routeInfo("web", "old-acknowledgment"), "old-acknowledgment")
		require.Empty(t, s.getCurrentInterceptInfos()[0].RouteIncarnation)
		delete(s.interceptWaiters, "web")
		s.rememberRouteIncarnation(routeInfo("web", "old-acknowledgment"), "old-acknowledgment")
		require.Empty(t, s.getCurrentInterceptInfos()[0].RouteIncarnation)
	})
}

func TestRecreatedRouteHasIndependentWatchLifecycle(t *testing.T) {
	s := newRouteFenceSession(t)
	s.setCurrentIntercepts([]*manager.InterceptInfo{routeInfo("web", "old")})
	old := s.getInterceptByName("web")
	aw := &awaitIntercept{routeIncarnation: "new", mountPoint: "/new-mount"}
	s.interceptWaiters["web"] = aw
	s.setCurrentIntercepts([]*manager.InterceptInfo{routeInfo("web", "old")})
	require.Same(t, old, s.getInterceptByName("web"))
	require.Empty(t, old.ClientMountPoint)
	require.False(t, aw.acceptsRoute("old"))
	require.True(t, aw.acceptsRoute("new"))
	require.True(t, aw.acceptsRoute(""), "legacy managers must still satisfy the waiter")

	s.setCurrentIntercepts([]*manager.InterceptInfo{routeInfo("web", "new")})
	current := s.getInterceptByName("web")
	require.NotSame(t, old, current)
	require.Equal(t, "/new-mount", current.ClientMountPoint)
	require.ErrorIs(t, old.ctx.Err(), context.Canceled)
	select {
	case <-old.finalRemovalDone:
	default:
		t.Fatal("the older intercept still waits for removal")
	}
	require.NoError(t, current.ctx.Err())
}

type routeRemovalTestManager struct {
	manager.UnimplementedManagerServer
	requests chan *manager.RemoveInterceptRequest2
	onRemove func()
}

func (m *routeRemovalTestManager) RemoveIntercept(_ context.Context, req *manager.RemoveInterceptRequest2) (*emptypb.Empty, error) {
	m.requests <- req
	if m.onRemove != nil {
		m.onRemove()
	}
	return &emptypb.Empty{}, nil
}

func attachRouteRemovalManager(t *testing.T, s *session) *routeRemovalTestManager {
	t.Helper()
	mgr := &routeRemovalTestManager{requests: make(chan *manager.RemoveInterceptRequest2, 3)}
	conn, cleanup := dialRemainTestManager(t, mgr)
	t.Cleanup(cleanup)
	s.managerConn = conn
	return mgr
}

func TestFailedCreateRemovalDoesNotCancelNewRouteOrAChildAlias(t *testing.T) {
	s := newRouteFenceSession(t)
	mgr := attachRouteRemovalManager(t, s)
	parentInfo := routeInfo("web", "current")
	parentInfo.Spec.ContainerName = "container"
	s.setCurrentIntercepts([]*manager.InterceptInfo{parentInfo})
	current := s.getInterceptByName("web")

	require.NoError(t, s.removeCreatedIntercept("web", "previous"))
	request := <-mgr.requests
	require.Equal(t, "web", request.Name)
	require.Equal(t, "previous", request.RouteIncarnation)
	require.NoError(t, current.ctx.Err())

	require.Same(t, current, s.getInterceptByName("web/container"), "ordinary explicit lookup accepts this alias")
	require.NoError(t, s.removeCreatedIntercept("web/container", "child-attempt"))
	request = <-mgr.requests
	require.Equal(t, "web/container", request.Name)
	require.Equal(t, "child-attempt", request.RouteIncarnation)
	require.NoError(t, current.ctx.Err())

	require.NoError(t, s.removeCreatedIntercept("web", ""), "an unacknowledged legacy attempt cannot clean up a fenced replacement")
	select {
	case req := <-mgr.requests:
		t.Fatalf("removed a different route on behalf of a legacy creation: %v", req)
	default:
	}
}

func TestExplicitRouteRemovalSendsObservedIncarnation(t *testing.T) {
	for _, incarnation := range []string{"acknowledged", ""} {
		t.Run(incarnation, func(t *testing.T) {
			s := newRouteFenceSession(t)
			mgr := attachRouteRemovalManager(t, s)
			ii := routeInfo("web", incarnation)
			ii.Spec.ContainerName = "container"
			s.setCurrentIntercepts([]*manager.InterceptInfo{ii})
			mgr.onRemove = func() { s.setCurrentIntercepts(nil) }
			require.NoError(t, s.RemoveIntercept("web/container"))
			request := <-mgr.requests
			require.Equal(t, "web", request.Name)
			require.Equal(t, incarnation, request.RouteIncarnation)
			require.Equal(t, "session", request.Session.SessionId)
		})
	}
}
