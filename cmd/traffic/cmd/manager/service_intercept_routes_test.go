package manager

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	authnv1 "k8s.io/api/authentication/v1"
	authv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/telepresenceio/clog/testutil"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/auth"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/state"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

var routingObserver = &auth.Principal{Username: "system:serviceaccount:routing:observer", UID: "observer-uid"}

func TestWatchInterceptRoutesGRPCVerifiesProjectedIdentityAndNamespace(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx := testutil.NewContext(t, true)
	_, mgr, sctx := getTestClientConnAndService(ctx, t, nil)
	cs := k8sapi.GetK8sInterface(sctx).(*fake.Clientset)
	audiencesSeen := make(chan []string, 16)
	k8sapi.InstallFakeTokenReviews(cs, func(token string, audiences []string) *authnv1.TokenReviewStatus {
		select {
		case audiencesSeen <- audiences:
		default:
		}
		if token != "projected-observer-token" {
			return &authnv1.TokenReviewStatus{Authenticated: false}
		}
		return &authnv1.TokenReviewStatus{
			Authenticated: true, Audiences: audiences,
			User: authnv1.UserInfo{Username: routingObserver.Username, UID: routingObserver.UID},
		}
	})
	observedUsers := make(chan string, 16)
	cs.PrependReactor("create", "subjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		review := action.(k8stesting.CreateAction).GetObject().(*authv1.SubjectAccessReview)
		ra := review.Spec.ResourceAttributes
		if ra.Namespace == "default" {
			observedUsers <- review.Spec.User
		}
		review.Status.Allowed = review.Spec.User == routingObserver.Username && ra.Namespace == "default" && ra.Verb == "watch" && ra.Group == "telepresence.io" && ra.Resource == "interceptroutes"
		return true, review, nil
	})
	listener := bufconn.Listen(64 * 1024)
	interceptor := auth.NewInterceptor(auth.NewAuthenticator(cs), auth.ModePermissive)
	gs := grpc.NewServer(grpc.StreamInterceptor(interceptor.Stream()))
	rpc.RegisterManagerServer(gs, mgr)
	go func() { _ = gs.Serve(listener) }()
	defer gs.Stop()
	conn, err := grpc.NewClient("passthrough:///routing-observer",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := rpc.NewManagerClient(conn)
	require.NoError(t, uuid.Validate(mgr.ID()))
	for _, tc := range []struct {
		name, token, namespace string
		want                   codes.Code
	}{
		{name: "anonymous", namespace: "default", want: codes.Unauthenticated},
		{name: "invalid", token: "wrong", namespace: "default", want: codes.Unauthenticated},
		{name: "wrong namespace", token: "projected-observer-token", namespace: "other", want: codes.PermissionDenied},
		{name: "valid projected identity", token: "projected-observer-token", namespace: "default", want: codes.OK},
		{name: "reopened projected identity", token: "projected-observer-token", namespace: "default", want: codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if tc.token != "" {
				wctx = metadata.AppendToOutgoingContext(wctx, "authorization", "Bearer "+tc.token)
			}
			watch, err := client.WatchInterceptRoutes(wctx, &rpc.WatchInterceptRoutesRequest{Namespaces: []string{tc.namespace}})
			require.NoError(t, err)
			snapshot, err := watch.Recv()
			require.Equal(t, tc.want, status.Code(err))
			if tc.want == codes.OK {
				require.Empty(t, snapshot.Routes)
				require.Equal(t, mgr.ID(), snapshot.ManagerInstanceId)
			} else {
				require.Nil(t, snapshot, "a rejected observer cannot see the manager instance ID")
			}
		})
	}
	select {
	case observedUser := <-observedUsers:
		require.Equal(t, routingObserver.Username, observedUser)
	default:
		t.Fatal("the expected namespace subject access review was not performed")
	}
	var seen [][]string
	for len(audiencesSeen) > 0 {
		seen = append(seen, <-audiencesSeen)
	}
	require.Contains(t, seen, []string{agentconfig.ManagerTokenAudience})
}

func TestWatchInterceptRoutesAuthorization(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx := testutil.NewContext(t, true)
	_, mgr, sctx := getTestClientConnAndService(ctx, t, nil, func(env *managerutil.Env) {
		env.AuthenticationMode = auth.ModePermissive
		env.AuthorizationRequiredGrant = auth.GrantAny
	})
	cs := k8sapi.GetK8sInterface(sctx).(*fake.Clientset)
	rec := &sarRecorder{}
	installRecordingSAR(cs, rec, func(ra *authv1.ResourceAttributes) bool {
		return isPortForwardReview(ra) || ra.Namespace == "default" && ra.Group == "telepresence.io" && ra.Resource == "interceptroutes" && ra.Verb == "watch"
	})
	pctx := auth.WithPrincipal(sctx, routingObserver)
	for _, tc := range []struct {
		name       string
		ctx        context.Context
		namespaces []string
		want       codes.Code
	}{
		{name: "anonymous permissive caller", ctx: sctx, namespaces: []string{"default"}, want: codes.Unauthenticated},
		{name: "unavailable authentication", ctx: auth.WithAuthUnavailable(sctx), namespaces: []string{"default"}, want: codes.Unavailable},
		{name: "no namespaces", ctx: pctx, want: codes.InvalidArgument},
		{name: "blank namespace", ctx: pctx, namespaces: []string{"default", ""}, want: codes.InvalidArgument},
		{name: "invalid namespace", ctx: pctx, namespaces: []string{"default", "INVALID"}, want: codes.InvalidArgument},
		{name: "too many namespaces", ctx: pctx, namespaces: make([]string, interceptRouteWatchNamespaceLimit+1), want: codes.InvalidArgument},
		{name: "mixed authorization", ctx: pctx, namespaces: []string{"default", "other"}, want: codes.PermissionDenied},
		{name: "client portforward alone", ctx: pctx, namespaces: []string{"other"}, want: codes.PermissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := newFakeServerStream[rpc.InterceptRouteSnapshot](tc.ctx)
			err := mgr.WatchInterceptRoutes(&rpc.WatchInterceptRoutesRequest{Namespaces: tc.namespaces}, stream)
			require.Equal(t, tc.want, status.Code(err))
			require.Empty(t, stream.ch)
		})
	}
	require.Equal(t, []authv1.ResourceAttributes{
		{Namespace: "default", Group: "telepresence.io", Resource: "interceptroutes", Verb: "watch"},
		{Namespace: "other", Group: "telepresence.io", Resource: "interceptroutes", Verb: "watch"},
	}, rec.all(), "no portforward or attachment review may substitute for the observer grant")

	cs.PrependReactor("create", "subjectaccessreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("Kubernetes unavailable")
	})
	stream := newFakeServerStream[rpc.InterceptRouteSnapshot](pctx)
	err := mgr.WatchInterceptRoutes(&rpc.WatchInterceptRoutesRequest{Namespaces: []string{"unreviewed"}}, stream)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Empty(t, stream.ch)
}

func TestWatchInterceptRoutesScopedSnapshotsUpdatesAndReauthentication(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx := testutil.NewContext(t, true)
	_, mgr, sctx := getTestClientConnAndService(ctx, t, nil)
	managerID := mgr.ID()
	require.NoError(t, uuid.Validate(managerID))
	cs := k8sapi.GetK8sInterface(sctx).(*fake.Clientset)
	rec := &sarRecorder{}
	installRecordingSAR(cs, rec, func(ra *authv1.ResourceAttributes) bool {
		return ra.Namespace == "default"
	})
	pctx := auth.WithPrincipal(sctx, routingObserver)
	wctx, cancel := context.WithCancel(pctx)
	defer cancel()
	stream := newFakeServerStream[rpc.InterceptRouteSnapshot](wctx)
	done := make(chan error, 1)
	go func() {
		done <- mgr.WatchInterceptRoutes(&rpc.WatchInterceptRoutesRequest{Namespaces: []string{"default", "default"}}, stream)
	}()
	require.Empty(t, recvInterceptRoutesForInstance(t, stream, managerID).Routes, "a new watch must emit a complete initial snapshot")
	visible := routingTestIntercept("client-session:visible", "default", "visible")
	foreign := routingTestIntercept("foreign-session:hidden", "other", "hidden")
	mgr.State().RestoreIntercepts(sctx, []*rpc.InterceptInfo{foreign}, time.Now())
	select {
	case snap := <-stream.ch:
		t.Fatalf("a foreign change triggered the scoped stream: %v", snap)
	case <-time.After(50 * time.Millisecond):
	}
	mgr.State().RestoreIntercepts(sctx, []*rpc.InterceptInfo{visible}, time.Now())
	routes := recvInterceptRoutesForInstance(t, stream, managerID).Routes
	require.Len(t, routes, 1)
	require.Equal(t, "visible", routes[0].Name)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, routes[0].Disposition)
	require.Equal(t, []authv1.ResourceAttributes{{Namespace: "default", Group: "telepresence.io", Resource: "interceptroutes", Verb: "watch"}}, rec.all())
	visibleID := routes[0].Id
	mgr.State().UpdateIntercept(visible.Id, func(intercept *state.Intercept) {
		intercept.Disposition = rpc.InterceptDispositionType_ACTIVE
	})
	routes = recvInterceptRoutesForInstance(t, stream, managerID).Routes
	require.Len(t, routes, 1)
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, routes[0].Disposition)
	require.Equal(t, visibleID, routes[0].Id)
	mgr.State().UpdateIntercept(visible.Id, func(intercept *state.Intercept) {
		intercept.Disposition = rpc.InterceptDispositionType_REMOVED
	})
	require.Empty(t, recvInterceptRoutesForInstance(t, stream, managerID).Routes, "leaving the scope must emit the complete remaining set")
	cancel()
	require.NoError(t, <-done)

	stream = newFakeServerStream[rpc.InterceptRouteSnapshot](pctx)
	done = make(chan error, 1)
	go func() {
		done <- mgr.(*service).watchInterceptRoutes(&rpc.WatchInterceptRoutesRequest{Namespaces: []string{"default"}}, stream, 20*time.Millisecond)
	}()
	require.Empty(t, recvInterceptRoutesForInstance(t, stream, managerID).Routes)
	select {
	case err := <-done:
		require.NoError(t, err, "bounded expiry closes normally so the observer can reconnect")
	case <-time.After(time.Second):
		t.Fatal("observer stream did not expire")
	}
}

func TestWatchInterceptRoutesManagerInstanceChangesAfterRestart(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx := testutil.NewContext(t, true)
	var previousID string
	intercept := routingTestIntercept("client-session:visible", "default", "visible")
	for _, name := range []string{"initial manager", "restarted manager"} {
		t.Run(name, func(t *testing.T) {
			_, mgr, sctx := getTestClientConnAndService(ctx, t, nil)
			managerID := mgr.ID()
			require.NoError(t, uuid.Validate(managerID))
			require.NotEqual(t, previousID, managerID)
			previousID = managerID
			cs := k8sapi.GetK8sInterface(sctx).(*fake.Clientset)
			installRecordingSAR(cs, &sarRecorder{}, func(ra *authv1.ResourceAttributes) bool {
				return ra.Namespace == "default"
			})
			wctx, cancel := context.WithCancel(auth.WithPrincipal(sctx, routingObserver))
			defer cancel()
			stream := newFakeServerStream[rpc.InterceptRouteSnapshot](wctx)
			done := make(chan error, 1)
			go func() {
				done <- mgr.WatchInterceptRoutes(&rpc.WatchInterceptRoutesRequest{Namespaces: []string{"default"}}, stream)
			}()
			require.Empty(t, recvInterceptRoutesForInstance(t, stream, managerID).Routes,
				"the restarted manager sends its changed instance ID before clients restore routes")
			mgr.State().RestoreIntercepts(sctx, []*rpc.InterceptInfo{intercept}, time.Now())
			snapshot := recvInterceptRoutesForInstance(t, stream, managerID)
			require.Len(t, snapshot.Routes, 1)
			require.Equal(t, rpc.InterceptDispositionType_WAITING, snapshot.Routes[0].Disposition)
			mgr.State().UpdateIntercept(intercept.Id, func(intercept *state.Intercept) {
				intercept.Disposition = rpc.InterceptDispositionType_ACTIVE
			})
			snapshot = recvInterceptRoutesForInstance(t, stream, managerID)
			require.Len(t, snapshot.Routes, 1)
			require.Equal(t, rpc.InterceptDispositionType_ACTIVE, snapshot.Routes[0].Disposition)
			cancel()
			require.NoError(t, <-done)
		})
	}
}

func TestInterceptRouteProjectionOmitsSensitiveAndUnauthorizedData(t *testing.T) {
	info := routingTestIntercept("client-session:visible", "default", "visible")
	info.Spec.Client = "client-name"
	info.Spec.TargetHost = "sensitive-local-address"
	info.Spec.Metadata = map[string]string{"private": "sensitive-metadata"}
	info.Spec.HeaderFilters = map[string]string{"x-local-routing-key": "routing-key"}
	info.Spec.PathFilters = []string{"/api/*"}
	info.Spec.Wiretap = true
	info.Environment = map[string]string{"SECRET": "application-secret"}
	info.Mounts = map[string]int32{"/private/mount": 2}
	info.PodName = "private-pod"
	info.PodIp = "10.0.0.42"
	info.ServiceWorkloads = []*rpc.InterceptWorkload{
		{Namespace: "default", WorkloadKind: "Deployment", WorkloadName: "primary"},
		{Namespace: "other", WorkloadKind: "StatefulSet", WorkloadName: "foreign-participant"},
		{WorkloadKind: "StatefulSet", WorkloadName: "implicit-default"},
	}
	route := interceptRoute(info, map[string]struct{}{"default": {}})
	require.Regexp(t, "^[0-9a-f]{64}$", route.Id)
	require.NotContains(t, route.Id, "client-session")
	require.True(t, proto.Equal(route, &rpc.InterceptRoute{
		Id: route.Id, Name: "visible", Namespace: "default", WorkloadName: "hello", WorkloadKind: "Deployment",
		Mechanism: "http", ContainerPort: 54337, Disposition: rpc.InterceptDispositionType_WAITING,
		Wiretap: true, HeaderFilters: map[string]string{"x-local-routing-key": "routing-key"}, PathFilters: []string{"/api/*"},
		ServiceWorkloads: []*rpc.InterceptRouteWorkload{
			{Namespace: "default", WorkloadKind: "Deployment", WorkloadName: "primary"},
			{Namespace: "default", WorkloadKind: "StatefulSet", WorkloadName: "implicit-default"},
		},
	}))
	encoded, err := proto.Marshal(route)
	require.NoError(t, err)
	for _, value := range []string{"client-session", "client-name", "sensitive-local-address", "sensitive-metadata", "application-secret", "/private/mount", "private-pod", "10.0.0.42", "foreign-participant"} {
		require.NotContains(t, string(encoded), value)
	}
	wide := interceptRoute(info, map[string]struct{}{"default": {}, "other": {}})
	require.Len(t, wide.ServiceWorkloads, 3)
	route.HeaderFilters["x-local-routing-key"] = "changed"
	route.PathFilters[0] = "/changed"
	require.Equal(t, "routing-key", info.Spec.HeaderFilters["x-local-routing-key"])
	require.Equal(t, "/api/*", info.Spec.PathFilters[0])
}

func TestExternalServiceRejectsInterceptRouteObserver(t *testing.T) {
	es := newExternalService(nil)
	for _, ctx := range []context.Context{context.Background(), auth.WithPrincipal(context.Background(), routingObserver)} {
		stream := newFakeServerStream[rpc.InterceptRouteSnapshot](ctx)
		err := es.WatchInterceptRoutes(&rpc.WatchInterceptRoutesRequest{Namespaces: []string{"default"}}, stream)
		require.Equal(t, codes.Unimplemented, status.Code(err))
		require.ErrorContains(t, err, "internal")
		require.Empty(t, stream.ch)
	}
}

func routingTestIntercept(id, namespace, name string) *rpc.InterceptInfo {
	return &rpc.InterceptInfo{
		Id: id, ClientSession: &rpc.SessionInfo{SessionId: "sensitive-client-session"}, Disposition: rpc.InterceptDispositionType_WAITING,
		Spec: &rpc.InterceptSpec{Name: name, Namespace: namespace, Agent: "hello", WorkloadKind: "Deployment", Mechanism: "http", ContainerPort: 54337},
	}
}

func recvInterceptRoutes(t *testing.T, stream *fakeServerStream[rpc.InterceptRouteSnapshot]) *rpc.InterceptRouteSnapshot {
	t.Helper()
	select {
	case snapshot := <-stream.ch:
		return snapshot
	case <-time.After(5 * time.Second):
		t.Fatal("timed out receiving an intercept route snapshot")
		return nil
	}
}

func recvInterceptRoutesForInstance(t *testing.T, stream *fakeServerStream[rpc.InterceptRouteSnapshot], managerID string) *rpc.InterceptRouteSnapshot {
	t.Helper()
	snapshot := recvInterceptRoutes(t, stream)
	require.Equal(t, managerID, snapshot.ManagerInstanceId)
	return snapshot
}
