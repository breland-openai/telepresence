package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	empty "google.golang.org/protobuf/types/known/emptypb"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/telepresenceio/clog/testutil"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/auth"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/routeintent"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/state"
	testdata "github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/test"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

func newManagerRouteStore() (*fake.Clientset, routeintent.Store) {
	client := fake.NewClientset()
	// The stock tracker doesn't set resourceVersion; the store refuses to update
	// a record without one. These manager tests exercise sequential transitions.
	client.PrependReactor("create", "configmaps", func(action clienttesting.Action) (bool, runtime.Object, error) {
		action.(clienttesting.CreateAction).GetObject().(*core.ConfigMap).ResourceVersion = "1"
		return false, nil, nil
	})
	return client, routeintent.NewKubernetes(client.CoreV1().ConfigMaps("ambassador"))
}

func managerRouteSpec() *rpc.InterceptSpec {
	return &rpc.InterceptSpec{
		Name: "local-web", Namespace: "default", WorkloadKind: "Deployment", Agent: "web", ContainerPort: 8080,
		Mechanism: "http", Protocol: "TCP", HeaderFilters: map[string]string{routeintent.RoutingHeader: "developer-key"},
	}
}

func managerRouteIdentity(t *testing.T) (context.Context, *rpc.SessionInfo, string) {
	t.Helper()
	ctx := auth.WithPrincipal(t.Context(), &auth.Principal{Username: "alice", UID: "uid-1"})
	session := &rpc.SessionInfo{SessionId: "original-session"}
	owner, err := durableRouteOwner(session)
	require.NoError(t, err)
	return ctx, session, owner
}

func historicalManagerRouteOwner(username, uid, sessionID string) string {
	sum := sha256.Sum256([]byte(username + "\x00" + uid))
	return hex.EncodeToString(sum[:]) + "/" + sessionID
}

func TestRouteIntentNamespaceGatePreservesLegacyOutsideScope(t *testing.T) {
	ctx, session, owner := managerRouteIdentity(t)
	client, store := newManagerRouteStore()
	allowed := newRouteIntentManager(store, nil, "ambassador")
	svc := &service{routeIntents: allowed}
	require.True(t, newRouteIntentManager(store, nil).managesNamespace("default"), "an empty allowlist retains the existing all-namespaces behavior")
	require.True(t, allowed.managesNamespace("ambassador"))
	require.False(t, allowed.managesNamespace("default"))
	outside := managerRouteSpec()
	inside := proto.CloneOf(outside)
	inside.Namespace = "ambassador"
	require.False(t, svc.usesDurableRoute(outside))
	require.True(t, svc.usesDurableRoute(inside))
	outsideInfo := &rpc.InterceptInfo{Spec: outside, ClientSession: session, Id: session.SessionId + ":" + outside.Name, RouteIncarnation: "ignore-legacy-proposal"}
	insideInfo := &rpc.InterceptInfo{Spec: inside, ClientSession: session, Id: session.SessionId + ":" + inside.Name}
	require.False(t, svc.reconnectMayUseDurableRoutes(&rpc.ReconnectClientRequest{
		Session: session, Client: &rpc.ClientInfo{Namespace: "default"}, Intercepts: []*rpc.InterceptInfo{outsideInfo},
	}))
	require.True(t, svc.reconnectMayUseDurableRoutes(&rpc.ReconnectClientRequest{
		Session: session, Client: &rpc.ClientInfo{Namespace: "default"}, Intercepts: []*rpc.InterceptInfo{outsideInfo, insideInfo},
	}))
	client.PrependReactor("*", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("legacy namespace must not consult durable storage")
	})
	restored, err := svc.durableRestoredIntercepts(t.Context(), session, []*rpc.InterceptInfo{outsideInfo})
	require.NoError(t, err, "anonymous old clients must reconnect outside the namespace even when durable storage is unavailable")
	require.Len(t, restored, 1)
	require.Empty(t, restored[0].RouteIncarnation)
	require.Equal(t, "ignore-legacy-proposal", outsideInfo.RouteIncarnation, "the caller's cached snapshot stays untouched")
	_, err = svc.durableRestoredIntercepts(t.Context(), session, []*rpc.InterceptInfo{insideInfo})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "a scoped route must supply its durable incarnation")
	outsideLive := &state.Intercept{InterceptInfo: outsideInfo}
	require.False(t, svc.shouldLookupDurableRemoval("default", outsideLive, ""))
	require.False(t, svc.shouldLookupDurableRemoval("ambassador", outsideLive, "stale-supplied-incarnation"), "a known nonallowlisted target always removes as legacy")
	require.False(t, svc.shouldLookupDurableRemoval("default", nil, ""), "an old out-of-scope client may retry a missing removal anonymously")
	require.True(t, svc.shouldLookupDurableRemoval("default", nil, "cross-namespace-incarnation"), "an updated client may reconnect across namespaces without separate authentication")
	require.True(t, svc.shouldLookupDurableRemoval("ambassador", nil, ""))
	client.ReactionChain = client.ReactionChain[1:]

	outsidePredicate, ok := durableRoutePredicate(outside)
	require.True(t, ok)
	outsideKey := routeintent.Key{Namespace: "default", Owner: owner, Name: outside.Name}
	_, err = store.Create(ctx, outsideKey, "historical", outsidePredicate, time.Time{})
	require.NoError(t, err)
	found, err := allowed.findOwned(ctx, owner, outside.Name)
	require.NoError(t, err)
	require.Nil(t, found, "a scoped manager must never claim a historical record from another namespace")
	require.NoError(t, svc.departDurableRoutes(ctx, session, nil))
	historical, err := store.Get(ctx, outsideKey)
	require.NoError(t, err)
	require.Equal(t, routeintent.Desired, historical.State, "scoped departure must not tombstone other namespaces")
}

func TestRouteIntentNamespaceGateLegacyCreateAndRemoveWithoutAuthentication(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	const namespace, workload = "default", "legacy-route-target"
	deployment := &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: workload, Namespace: namespace}, Spec: apps.DeploymentSpec{
		Selector: &meta.LabelSelector{MatchLabels: map[string]string{"app": workload}},
		Template: core.PodTemplateSpec{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{"app": workload}}, Spec: core.PodSpec{
			Containers: []core.Container{{Name: "app", Ports: []core.ContainerPort{{ContainerPort: 8080}}}},
		}},
	}}
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: workload + "-abc", Namespace: namespace, Labels: map[string]string{"app": workload}},
		Spec:       core.PodSpec{NodeName: "node-1"}, Status: core.PodStatus{Phase: core.PodRunning},
	}
	_, svc, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t, []runtime.Object{deployment, pod}, func(env *managerutil.Env) {
		env.RouteIntentEnabled = true
		env.RouteIntentNamespaces = []string{"ambassador"}
		env.AgentArrivalTimeout = 5 * time.Second
	})
	clientInfo := proto.CloneOf(testdata.GetTestClients(t)["alice"])
	session, err := svc.ArriveAsClient(ctx, clientInfo)
	require.NoError(t, err)
	agentInfo := proto.CloneOf(testdata.GetTestAgents(t)["helloPro"])
	agentInfo.Name, agentInfo.Namespace, agentInfo.NodeAgent = workload, namespace, true
	_, err = svc.ArriveAsAgent(ctx, agentInfo)
	require.NoError(t, err)
	for _, requestedIncarnation := range []string{"", "new-client-proposed-incarnation"} {
		name := "old-exact-key"
		if requestedIncarnation != "" {
			name = "new-exact-key"
		}
		created, createErr := svc.CreateIntercept(ctx, &rpc.CreateInterceptRequest{
			Session: session, RouteIncarnation: requestedIncarnation,
			InterceptSpec: &rpc.InterceptSpec{
				Name: name, Client: clientInfo.Name, Agent: workload, Namespace: namespace,
				WorkloadKind: "Deployment", NodeAgent: true, Mechanism: "http", Protocol: "TCP", ContainerPort: 8080,
				HeaderFilters: map[string]string{routeintent.RoutingHeader: name},
			},
		})
		require.NoError(t, createErr, "exact-key clients outside the scoped namespaces retain legacy creation")
		require.Empty(t, created.RouteIncarnation, "legacy creation must not advertise a durable route")
		_, err = svc.RemoveIntercept(ctx, &rpc.RemoveInterceptRequest2{Session: session, Name: name, RouteIncarnation: requestedIncarnation})
		require.NoError(t, err, "known out-of-scope routes retain legacy removal, including an unacknowledged proposal")
		_, err = svc.RemoveIntercept(ctx, &rpc.RemoveInterceptRequest2{Session: session, Name: name})
		require.NoError(t, err, "an old client's removal retry outside the scoped namespaces does not need authentication")
	}
}

type managerRouteClientCalls struct {
	arrive    func(context.Context, *rpc.ClientInfo) (*rpc.SessionInfo, error)
	reconnect func(context.Context, *rpc.ReconnectClientRequest) (*empty.Empty, error)
	create    func(context.Context, *rpc.CreateInterceptRequest) (*rpc.InterceptInfo, error)
	get       func(context.Context, *rpc.GetInterceptRequest) (*rpc.InterceptInfo, error)
	remove    func(context.Context, *rpc.RemoveInterceptRequest2) (*empty.Empty, error)
	depart    func(context.Context, *rpc.SessionInfo) (*empty.Empty, error)
}

func TestRouteIntentClientSessionSurvivesManagerRestartWithoutNewAuthentication(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	for _, authenticated := range []bool{false, true} {
		name := "anonymous-macbook-grpc"
		if authenticated {
			name = "existing-authenticated-client"
		}
		t.Run(name, func(t *testing.T) {
			const namespace, workload = "default", "durable-route-target"
			deployment := &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: workload, Namespace: namespace}, Spec: apps.DeploymentSpec{
				Selector: &meta.LabelSelector{MatchLabels: map[string]string{"app": workload}},
				Template: core.PodTemplateSpec{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{"app": workload}}, Spec: core.PodSpec{
					Containers: []core.Container{{Name: "app", Ports: []core.ContainerPort{{ContainerPort: 8080}}}},
				}},
			}}
			pod := &core.Pod{
				ObjectMeta: meta.ObjectMeta{Name: workload + "-abc", Namespace: namespace, Labels: map[string]string{"app": workload}},
				Spec:       core.PodSpec{NodeName: "node-1"}, Status: core.PodStatus{Phase: core.PodRunning},
			}
			_, store := newManagerRouteStore()
			start := func() (*service, managerRouteClientCalls, *grpc.ClientConn, context.Context, context.CancelFunc) {
				base, stop := context.WithCancel(testutil.NewContext(t, false))
				conn, manager, managerCtx := getTestClientConnAndService(base, t, []runtime.Object{deployment.DeepCopy(), pod.DeepCopy()}, func(env *managerutil.Env) {
					env.AgentArrivalTimeout = 5 * time.Second
				})
				svc := manager.(*service)
				svc.routeIntents = newRouteIntentManager(store, nil, namespace)
				if authenticated {
					return svc, managerRouteClientCalls{svc.ArriveAsClient, svc.ReconnectClient, svc.CreateIntercept, svc.GetIntercept, svc.RemoveIntercept, svc.Depart}, conn, managerCtx, stop
				}
				remote := rpc.NewManagerClient(conn)
				return svc, managerRouteClientCalls{
					arrive: func(ctx context.Context, req *rpc.ClientInfo) (*rpc.SessionInfo, error) {
						return remote.ArriveAsClient(ctx, req)
					},
					reconnect: func(ctx context.Context, req *rpc.ReconnectClientRequest) (*empty.Empty, error) {
						return remote.ReconnectClient(ctx, req)
					},
					create: func(ctx context.Context, req *rpc.CreateInterceptRequest) (*rpc.InterceptInfo, error) {
						return remote.CreateIntercept(ctx, req)
					},
					get: func(ctx context.Context, req *rpc.GetInterceptRequest) (*rpc.InterceptInfo, error) {
						return remote.GetIntercept(ctx, req)
					},
					remove: func(ctx context.Context, req *rpc.RemoveInterceptRequest2) (*empty.Empty, error) {
						return remote.RemoveIntercept(ctx, req)
					},
					depart: func(ctx context.Context, req *rpc.SessionInfo) (*empty.Empty, error) { return remote.Depart(ctx, req) },
				}, conn, managerCtx, stop
			}
			clientCtx := func(ctx context.Context, who string) context.Context {
				if authenticated {
					return auth.WithPrincipal(ctx, &auth.Principal{Username: who, UID: who + "-uid"})
				}
				return ctx
			}
			clientInfo := func(who string) *rpc.ClientInfo {
				info := proto.CloneOf(testdata.GetTestClients(t)["alice"])
				info.Name, info.InstallId = who, who+"-install"
				return info
			}
			create := func(ctx context.Context, calls managerRouteClientCalls, session *rpc.SessionInfo, client *rpc.ClientInfo, name string) *rpc.InterceptInfo {
				intercept, err := calls.create(ctx, &rpc.CreateInterceptRequest{
					Session: session, RouteIncarnation: name + "-incarnation",
					InterceptSpec: &rpc.InterceptSpec{
						Name: name, Client: client.Name, Agent: workload, Namespace: namespace,
						WorkloadKind: "Deployment", NodeAgent: true, Mechanism: "http", Protocol: "TCP", ContainerPort: 8080,
						HeaderFilters: map[string]string{routeintent.RoutingHeader: name + "-key"},
					},
				})
				require.NoError(t, err)
				require.Equal(t, name+"-incarnation", intercept.RouteIncarnation)
				return intercept
			}
			key := func(session *rpc.SessionInfo, name string) routeintent.Key {
				owner, err := durableRouteOwner(session)
				require.NoError(t, err)
				return routeintent.Key{Namespace: namespace, Owner: owner, Name: name}
			}
			first, firstCalls, firstConn, firstCtx, firstStop := start()
			defer firstStop()
			agent := proto.CloneOf(testdata.GetTestAgents(t)["helloPro"])
			agent.Name, agent.Namespace, agent.NodeAgent = workload, namespace, true
			_, err := first.ArriveAsAgent(firstCtx, agent)
			require.NoError(t, err)
			alice, bob := clientInfo("alice-macbook"), clientInfo("bob-macbook")
			aliceSession, err := firstCalls.arrive(clientCtx(firstCtx, "alice"), alice)
			require.NoError(t, err)
			bobSession, err := firstCalls.arrive(clientCtx(firstCtx, "bob"), bob)
			require.NoError(t, err)
			require.NotEqual(t, aliceSession.SessionId, bobSession.SessionId)
			aliceRoute := create(clientCtx(firstCtx, "alice"), firstCalls, aliceSession, alice, "protected-web")
			bobRoute := create(clientCtx(firstCtx, "bob"), firstCalls, bobSession, bob, "other-protected")
			if authenticated {
				_, err = firstCalls.get(clientCtx(firstCtx, "bob"), &rpc.GetInterceptRequest{Session: aliceSession, Name: aliceRoute.Spec.Name})
				require.Equal(t, codes.PermissionDenied, status.Code(err), "the manager's pre-existing authenticated session behavior remains unchanged")
			}
			firstStop()
			require.NoError(t, firstConn.Close())
			_, nextCalls, nextConn, nextCtx, nextStop := start()
			defer nextStop()
			defer nextConn.Close()
			_, err = nextCalls.reconnect(clientCtx(nextCtx, "bob"), &rpc.ReconnectClientRequest{Session: bobSession, Client: bob, Intercepts: []*rpc.InterceptInfo{aliceRoute}})
			require.Equal(t, codes.PermissionDenied, status.Code(err), "a distinct session cannot restore another session's snapshot")
			_, err = nextCalls.reconnect(clientCtx(nextCtx, "bob"), &rpc.ReconnectClientRequest{Session: bobSession, Client: bob, Intercepts: []*rpc.InterceptInfo{bobRoute}})
			require.NoError(t, err)
			_, err = nextCalls.remove(clientCtx(nextCtx, "bob"), &rpc.RemoveInterceptRequest2{Session: bobSession, Name: aliceRoute.Spec.Name, RouteIncarnation: aliceRoute.RouteIncarnation})
			require.Equal(t, codes.NotFound, status.Code(err), "a distinct session cannot remove another session's route")
			stillDesired, err := store.Get(nextCtx, key(aliceSession, aliceRoute.Spec.Name))
			require.NoError(t, err)
			require.Equal(t, routeintent.Desired, stillDesired.State)
			_, err = nextCalls.reconnect(clientCtx(nextCtx, "alice"), &rpc.ReconnectClientRequest{Session: aliceSession, Client: alice, Intercepts: []*rpc.InterceptInfo{aliceRoute}})
			require.NoError(t, err)
			restored, err := nextCalls.get(clientCtx(nextCtx, "alice"), &rpc.GetInterceptRequest{Session: aliceSession, Name: aliceRoute.Spec.Name})
			require.NoError(t, err)
			require.Equal(t, aliceRoute.RouteIncarnation, restored.RouteIncarnation)
			if !authenticated {
				watchCtx, cancelWatch := context.WithCancel(nextCtx)
				defer cancelWatch()
				watch, watchErr := rpc.NewManagerClient(nextConn).WatchIntercepts(watchCtx, aliceSession)
				require.NoError(t, watchErr)
				snapshot, watchErr := watch.Recv()
				require.NoError(t, watchErr)
				require.Len(t, snapshot.Intercepts, 1)
				require.Equal(t, aliceRoute.Id, snapshot.Intercepts[0].Id)
				cancelWatch()
			}
			_, err = nextCalls.remove(clientCtx(nextCtx, "alice"), &rpc.RemoveInterceptRequest2{Session: aliceSession, Name: aliceRoute.Spec.Name, RouteIncarnation: aliceRoute.RouteIncarnation})
			require.NoError(t, err)
			removed, err := store.Get(nextCtx, key(aliceSession, aliceRoute.Spec.Name))
			require.NoError(t, err)
			require.Equal(t, routeintent.Removed, removed.State)
			_, err = nextCalls.depart(clientCtx(nextCtx, "bob"), bobSession)
			require.NoError(t, err)
			removed, err = store.Get(nextCtx, key(bobSession, bobRoute.Spec.Name))
			require.NoError(t, err)
			require.Equal(t, routeintent.Removed, removed.State)
		})
	}
}

func TestRouteIntentManagerFencesRetryRecreationAndReconnect(t *testing.T) {
	ctx, session, owner := managerRouteIdentity(t)
	_, store := newManagerRouteStore()
	firstManager, nextManager := newRouteIntentManager(store, nil), newRouteIntentManager(store, nil)
	spec := managerRouteSpec()
	predicate, ok := durableRoutePredicate(spec)
	require.True(t, ok)
	key := routeintent.Key{Namespace: spec.Namespace, Owner: owner, Name: spec.Name}
	_, err := firstManager.create(ctx, key, "", predicate)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	first, err := firstManager.create(ctx, key, "one", predicate)
	require.NoError(t, err)
	require.Equal(t, session.SessionId+":"+spec.Name, durableRouteInterceptID(first))
	duplicate, err := nextManager.create(ctx, key, "one", predicate)
	require.NoError(t, err)
	require.Equal(t, first, duplicate)
	require.Equal(t, codes.FailedPrecondition, status.Code(nextManager.remove(ctx, first, "wrong")))

	svc := &service{routeIntents: nextManager}
	info := &rpc.InterceptInfo{Spec: spec, ClientSession: session, Id: durableRouteInterceptID(first), RouteIncarnation: "one"}
	accepted, err := svc.durableRestoredIntercepts(ctx, session, []*rpc.InterceptInfo{info})
	require.NoError(t, err)
	require.Equal(t, []*rpc.InterceptInfo{info}, accepted)
	forged := proto.Clone(info).(*rpc.InterceptInfo)
	forged.Id = "different-session:local-web"
	_, err = svc.durableRestoredIntercepts(ctx, session, []*rpc.InterceptInfo{forged})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	forged = proto.Clone(info).(*rpc.InterceptInfo)
	forged.Spec.HeaderFilters[routeintent.RoutingHeader] = "other-key"
	_, err = svc.durableRestoredIntercepts(ctx, session, []*rpc.InterceptInfo{forged})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	require.NoError(t, firstManager.remove(ctx, first, "one"))
	accepted, err = svc.durableRestoredIntercepts(ctx, session, []*rpc.InterceptInfo{info})
	require.NoError(t, err)
	require.Empty(t, accepted)
	_, err = nextManager.create(ctx, key, "one", predicate)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	second, err := nextManager.create(ctx, key, "two", predicate)
	require.NoError(t, err)
	require.Greater(t, second.Revision.Version, first.Revision.Version)
	require.Equal(t, codes.FailedPrecondition, status.Code(firstManager.remove(ctx, first, "one")))
	accepted, err = svc.durableRestoredIntercepts(ctx, session, []*rpc.InterceptInfo{info})
	require.NoError(t, err)
	require.Empty(t, accepted)
	restoredSecond := proto.Clone(info).(*rpc.InterceptInfo)
	restoredSecond.RouteIncarnation = "two"
	accepted, err = svc.durableRestoredIntercepts(ctx, session, []*rpc.InterceptInfo{restoredSecond})
	require.NoError(t, err)
	require.Len(t, accepted, 1)
}

func TestRouteIntentContinuesPublishedPrincipalKeyByClientSession(t *testing.T) {
	ctx := t.Context()
	_, store := newManagerRouteStore()
	manager := newRouteIntentManager(store, nil)
	svc := &service{routeIntents: manager}
	session := &rpc.SessionInfo{SessionId: "macbook-session"}
	owner, err := durableRouteOwner(session)
	require.NoError(t, err)
	spec := managerRouteSpec()
	predicate, supported := durableRoutePredicate(spec)
	require.True(t, supported)
	canonical := routeintent.Key{Namespace: spec.Namespace, Owner: owner, Name: spec.Name}
	historical := canonical
	historical.Owner = historicalManagerRouteOwner("former-developer", "former-uid", session.SessionId)
	old, err := store.Create(ctx, historical, "original", predicate, time.Time{})
	require.NoError(t, err)
	other := canonical
	other.Owner = historicalManagerRouteOwner("former-developer", "former-uid", "other-session")
	_, err = store.Create(ctx, other, "other", predicate, time.Time{})
	require.NoError(t, err)
	invalid := canonical
	invalid.Owner = "not-a-published-principal-hash/" + session.SessionId
	_, err = store.Create(ctx, invalid, "invalid", predicate, time.Time{})
	require.NoError(t, err)
	require.Equal(t, session.SessionId+":"+spec.Name, durableRouteInterceptID(old))
	require.Empty(t, durableRouteInterceptID(routeintent.Record{Key: invalid}))

	continued, err := manager.create(ctx, canonical, "original", predicate)
	require.NoError(t, err)
	require.Equal(t, old, continued, "a retried create reuses the published record")
	_, err = store.Get(ctx, canonical)
	require.ErrorIs(t, err, routeintent.ErrNotFound, "no duplicate is written under the new key")
	info := &rpc.InterceptInfo{Spec: spec, ClientSession: session, Id: durableRouteInterceptID(old), RouteIncarnation: "original"}
	restored, err := svc.durableRestoredIntercepts(auth.WithAuthUnavailable(ctx), session, []*rpc.InterceptInfo{info})
	require.NoError(t, err)
	require.Equal(t, []*rpc.InterceptInfo{info}, restored, "the old record can be restored without any authentication service")
	found, err := manager.findOwned(ctx, owner, spec.Name)
	require.NoError(t, err)
	require.Equal(t, historical, found.Key)
	require.NoError(t, manager.remove(ctx, *found, "original"))
	removed, err := store.Get(ctx, historical)
	require.NoError(t, err)
	require.Equal(t, routeintent.Removed, removed.State)

	recreated, err := manager.create(ctx, canonical, "recreated", predicate)
	require.NoError(t, err)
	require.Equal(t, historical, recreated.Key, "the transition updates the actual historical ConfigMap key")
	require.Greater(t, recreated.Revision.Version, old.Revision.Version)
	_, err = store.Get(ctx, canonical)
	require.ErrorIs(t, err, routeintent.ErrNotFound)
	require.NoError(t, svc.departDurableRoutes(auth.WithAuthUnavailable(ctx), session, nil))
	removed, err = store.Get(ctx, historical)
	require.NoError(t, err)
	require.Equal(t, routeintent.Removed, removed.State)
	for _, unaffected := range []routeintent.Key{other, invalid} {
		remaining, getErr := store.Get(ctx, unaffected)
		require.NoError(t, getErr)
		require.Equal(t, routeintent.Desired, remaining.State)
	}
}

func TestRouteIntentRollingKeysDeduplicateEquivalentAndRejectConflictingRoutes(t *testing.T) {
	canonical := routeintent.Record{
		Key:      routeintent.Key{Namespace: "default", Owner: "session/macbook", Name: "web"},
		Revision: routeintent.Revision{Incarnation: "original", Version: 1}, State: routeintent.Desired,
		Predicate: routeintent.Predicate{Namespace: "default", RoutingKey: "developer-key"},
	}
	legacy := canonical
	legacy.Key.Owner = historicalManagerRouteOwner("alice", "uid", "macbook")
	canonicalRemoved, historicalRemoved := canonical, legacy
	canonicalRemoved.State = routeintent.Removed
	historicalRemoved.State, historicalRemoved.Revision.Version = routeintent.Removed, 2
	replacement, differentPredicate, independentlyRemoved := legacy, legacy, legacy
	replacement.Revision.Incarnation = "replacement"
	differentPredicate.Predicate.RoutingKey = "different"
	independentlyRemoved.State, independentlyRemoved.Revision.Incarnation = routeintent.Removed, "different"
	for _, tc := range []struct {
		name        string
		canonical   routeintent.Record
		legacy      routeintent.Record
		want        routeintent.Record
		wantFailure bool
	}{
		{name: "equivalent desired", canonical: canonical, legacy: legacy, want: canonical},
		{name: "historical removal wins", canonical: canonical, legacy: historicalRemoved, want: historicalRemoved},
		{name: "canonical removal wins", canonical: canonicalRemoved, legacy: legacy, want: canonicalRemoved},
		{name: "replacement incarnation", canonical: canonical, legacy: replacement, wantFailure: true},
		{name: "different predicate", canonical: canonical, legacy: differentPredicate, wantFailure: true},
		{name: "canonical recreation after historical removal", canonical: canonical, legacy: independentlyRemoved, want: canonical},
		{name: "historical active after different canonical removal", canonical: canonicalRemoved, legacy: replacement, wantFailure: true},
		{name: "independently removed", canonical: canonicalRemoved, legacy: independentlyRemoved, want: canonicalRemoved},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, records := range [][]routeintent.Record{{tc.canonical, tc.legacy}, {tc.legacy, tc.canonical}} {
				actual, err := deduplicateDurableRouteRecords(records)
				if tc.wantFailure {
					require.ErrorIs(t, err, errAmbiguousDurableRoute)
				} else {
					require.NoError(t, err)
					require.Equal(t, []routeintent.Record{tc.want}, actual)
				}
			}
		})
	}
	oldOnly, err := deduplicateDurableRouteRecords([]routeintent.Record{legacy})
	require.NoError(t, err)
	require.Equal(t, []routeintent.Record{legacy}, oldOnly)
	otherNamespace := legacy
	otherNamespace.Key.Namespace, otherNamespace.Predicate.Namespace = "elsewhere", "elsewhere"
	both, err := deduplicateDurableRouteRecords([]routeintent.Record{canonical, otherNamespace})
	require.NoError(t, err)
	require.Equal(t, []routeintent.Record{canonical, otherNamespace}, both)
}

func TestRouteIntentRollingDuplicateRemovalAndRecreation(t *testing.T) {
	ctx := t.Context()
	_, store := newManagerRouteStore()
	m := newRouteIntentManager(store, nil)
	svc := &service{routeIntents: m}
	session := &rpc.SessionInfo{SessionId: "rolling-macbook"}
	owner, err := durableRouteOwner(session)
	require.NoError(t, err)
	spec := managerRouteSpec()
	predicate, supported := durableRoutePredicate(spec)
	require.True(t, supported)
	canonicalKey := routeintent.Key{Namespace: spec.Namespace, Owner: owner, Name: spec.Name}
	legacyKey := func(username string) routeintent.Key {
		key := canonicalKey
		key.Owner = historicalManagerRouteOwner(username, "uid", session.SessionId)
		return key
	}
	oldKey, lateKey, conflictingKey := legacyKey("old"), legacyKey("late"), legacyKey("conflicting")
	_, err = store.Create(ctx, oldKey, "original", predicate, time.Time{})
	require.NoError(t, err)
	canonical, err := store.Create(ctx, canonicalKey, "original", predicate, time.Time{})
	require.NoError(t, err)
	m.refresh(ctx)
	records, ready, _ := m.snapshot()
	require.True(t, ready)
	require.Equal(t, []routeintent.Record{canonical}, records)
	owned, err := m.findOwned(ctx, owner, spec.Name)
	require.NoError(t, err)
	require.Equal(t, canonical, *owned)
	require.NoError(t, m.removeOwned(ctx, *owned, "original"))
	for _, key := range []routeintent.Key{oldKey, canonicalKey} {
		current, getErr := store.Get(ctx, key)
		require.NoError(t, getErr)
		require.Equal(t, routeintent.Removed, current.State)
	}
	_, err = store.Create(ctx, lateKey, "original", predicate, time.Time{})
	require.NoError(t, err, "an old manager can finish an in-flight create after the new manager removed the route")
	m.refresh(ctx)
	records, ready, _ = m.snapshot()
	require.True(t, ready)
	require.Len(t, records, 1)
	require.Equal(t, routeintent.Removed, records[0].State)
	recreated, err := m.create(ctx, canonicalKey, "new", predicate)
	require.NoError(t, err)
	require.Equal(t, canonicalKey, recreated.Key)
	late, err := store.Get(ctx, lateKey)
	require.NoError(t, err)
	require.Equal(t, routeintent.Removed, late.State)
	m.refresh(ctx)
	records, ready, _ = m.snapshot()
	require.True(t, ready)
	require.Equal(t, []routeintent.Record{recreated}, records)
	_, err = store.Create(ctx, conflictingKey, "replacement", predicate, time.Time{})
	require.NoError(t, err)
	m.refresh(ctx)
	_, ready, _ = m.snapshot()
	require.False(t, ready, "ambiguous active incarnations must not be published as authoritative")
	_, err = m.findOwned(ctx, owner, spec.Name)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.NoError(t, m.removeOwned(ctx, recreated, "new"))
	conflicting, err := store.Get(ctx, conflictingKey)
	require.NoError(t, err)
	require.Equal(t, routeintent.Desired, conflicting.State, "removing one incarnation cannot remove a different replacement")
	require.NoError(t, svc.departDurableRoutes(ctx, session, nil))
	m.refresh(ctx)
	_, ready, _ = m.snapshot()
	require.True(t, ready, "an explicit departure terminates every route belonging to the session")
}

func TestRouteIntentGracefulDepartureRequiresStorageAndIsOwnerScoped(t *testing.T) {
	ctx, session, owner := managerRouteIdentity(t)
	client, store := newManagerRouteStore()
	m := newRouteIntentManager(store, nil)
	svc := &service{routeIntents: m}
	predicate, ok := durableRoutePredicate(managerRouteSpec())
	require.True(t, ok)
	key := routeintent.Key{Namespace: "default", Owner: owner, Name: "route"}
	_, err := m.create(ctx, key, "one", predicate)
	require.NoError(t, err)
	otherCtx := t.Context()
	otherOwner, err := durableRouteOwner(&rpc.SessionInfo{SessionId: "other-session"})
	require.NoError(t, err)
	otherKey := routeintent.Key{Namespace: "default", Owner: otherOwner, Name: "route"}
	_, err = m.create(otherCtx, otherKey, "other", predicate)
	require.NoError(t, err)
	client.PrependReactor("list", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("API temporarily unavailable")
	})
	err = svc.departDurableRoutes(ctx, session, nil)
	require.Equal(t, codes.Unavailable, status.Code(err))
	client.ReactionChain = client.ReactionChain[1:]
	require.NoError(t, svc.departDurableRoutes(t.Context(), session, nil))
	removed, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, routeintent.Removed, removed.State)
	other, err := store.Get(ctx, otherKey)
	require.NoError(t, err)
	require.Equal(t, routeintent.Desired, other.State)
	require.NoError(t, svc.departDurableRoutes(t.Context(), session, nil), "a retried explicit departure is idempotent")
}

func TestRouteIntentKnownOutOfScopeClientDepartsWhenStorageIsUnavailable(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	conn, manager, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t, nil)
	defer conn.Close()
	client, store := newManagerRouteStore()
	client.PrependReactor("list", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("durable storage unavailable")
	})
	svc := manager.(*service)
	svc.routeIntents = newRouteIntentManager(store, nil, "ambassador")
	session, err := svc.ArriveAsClient(ctx, proto.CloneOf(testdata.GetTestClients(t)["alice"]))
	require.NoError(t, err)
	require.True(t, svc.state.GetClient(tunnel.SessionID(session.SessionId)).CreatedByThisManager())
	_, err = svc.Depart(ctx, session)
	require.NoError(t, err, "an ordinary out-of-scope client has no route state to write")
	_, err = svc.Depart(ctx, &rpc.SessionInfo{SessionId: "unknown-after-manager-restart"})
	require.Equal(t, codes.Unavailable, status.Code(err), "an unknown session may have had routes before a manager restart")
}

func TestRouteIntentRestoredOutOfScopeClientWithoutSnapshotDepartsHistoricalRoute(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	conn, manager, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t, nil)
	defer conn.Close()
	kube, store := newManagerRouteStore()
	svc := manager.(*service)
	svc.routeIntents = newRouteIntentManager(store, nil, "ambassador")
	session := &rpc.SessionInfo{SessionId: "reconnecting-cross-namespace-macbook"}
	client := proto.CloneOf(testdata.GetTestClients(t)["alice"])
	require.Equal(t, "default", client.Namespace)
	owner, err := durableRouteOwner(session)
	require.NoError(t, err)
	spec := managerRouteSpec()
	spec.Namespace = "ambassador"
	predicate, supported := durableRoutePredicate(spec)
	require.True(t, supported)
	key := routeintent.Key{Namespace: "ambassador", Owner: owner, Name: spec.Name}
	_, err = store.Create(ctx, key, "created-before-restart", predicate, time.Time{})
	require.NoError(t, err)
	_, err = svc.ReconnectClient(ctx, &rpc.ReconnectClientRequest{Session: session, Client: client})
	require.NoError(t, err, "the client may not have received the create response or watch snapshot before the old manager disappeared")
	restoredClient := svc.state.GetClient(tunnel.SessionID(session.SessionId))
	require.NotNil(t, restoredClient)
	require.False(t, restoredClient.CreatedByThisManager())
	require.False(t, svc.state.HasClientDurableIntercept(tunnel.SessionID(session.SessionId)))
	kube.PrependReactor("list", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("durable storage temporarily unavailable")
	})
	_, err = svc.Depart(ctx, session)
	require.Equal(t, codes.Unavailable, status.Code(err), "the empty reconnect snapshot cannot prove that no durable record exists")
	kube.ReactionChain = kube.ReactionChain[1:]
	_, err = svc.Depart(ctx, session)
	require.NoError(t, err)
	removed, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, routeintent.Removed, removed.State)
}

func TestRouteIntentOutOfScopeClientDepartsManagedCrossNamespaceRoute(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	conn, manager, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t, nil)
	defer conn.Close()
	_, store := newManagerRouteStore()
	svc := manager.(*service)
	svc.routeIntents = newRouteIntentManager(store, nil, "default")
	session := &rpc.SessionInfo{SessionId: "cross-namespace-macbook"}
	clientInfo := proto.CloneOf(testdata.GetTestClients(t)["alice"])
	clientInfo.Namespace = "other"
	svc.state.RestoreClient(tunnel.SessionID(session.SessionId), clientInfo, nil, time.Now())
	owner, err := durableRouteOwner(session)
	require.NoError(t, err)
	spec := managerRouteSpec()
	predicate, supported := durableRoutePredicate(spec)
	require.True(t, supported)
	key := routeintent.Key{Namespace: "default", Owner: owner, Name: spec.Name}
	_, err = svc.routeIntents.create(ctx, key, "one", predicate)
	require.NoError(t, err)
	svc.state.RestoreIntercepts(ctx, []*rpc.InterceptInfo{{
		Id: session.SessionId + ":" + spec.Name, ClientSession: session, Spec: spec, RouteIncarnation: "one",
	}}, time.Now())
	require.True(t, svc.state.HasClientDurableIntercept(tunnel.SessionID(session.SessionId)))
	_, err = svc.Depart(ctx, session)
	require.NoError(t, err)
	removed, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, routeintent.Removed, removed.State)
}

type managerRouteStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent chan *rpc.RouteIntentSnapshot
}

func (s *managerRouteStream) Context() context.Context { return s.ctx }

func (s *managerRouteStream) Send(snapshot *rpc.RouteIntentSnapshot) error {
	select {
	case s.sent <- snapshot:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func routeStreamReceive(t *testing.T, stream *managerRouteStream) *rpc.RouteIntentSnapshot {
	t.Helper()
	select {
	case snapshot := <-stream.sent:
		return snapshot
	case <-time.After(5 * time.Second):
		t.Fatal("route stream produced no snapshot")
		return nil
	}
}

func TestRouteIntentWatchIsAuthorizedAndLosesAuthorityOnStoreFailure(t *testing.T) {
	client, store := newManagerRouteStore()
	const controller = "system:serviceaccount:ambassador:controller"
	m := newRouteIntentManager(store, []string{controller, "alice"})
	svc := &service{id: "manager-epoch", routeIntents: m}
	for _, identity := range []string{"", "alice", "system:serviceaccount:ambassador:unlisted"} {
		ctx := t.Context()
		if identity != "" {
			ctx = auth.WithPrincipal(ctx, &auth.Principal{Username: identity})
		}
		stream := &managerRouteStream{ctx: ctx, sent: make(chan *rpc.RouteIntentSnapshot, 10)}
		err := svc.WatchRouteIntents(nil, stream)
		if identity == "" {
			require.Equal(t, codes.Unauthenticated, status.Code(err))
		} else {
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		}
		require.Empty(t, stream.sent)
	}
	ctx, cancel := context.WithCancel(auth.WithPrincipal(t.Context(), &auth.Principal{Username: controller}))
	stream := &managerRouteStream{ctx: ctx, sent: make(chan *rpc.RouteIntentSnapshot, 10)}
	done := make(chan error, 1)
	go func() { done <- svc.WatchRouteIntents(nil, stream) }()
	initial := routeStreamReceive(t, stream)
	require.Equal(t, rpc.RouteIntentSnapshot_SYNCHRONIZING, initial.SyncState)
	require.Equal(t, "manager-epoch", initial.ManagerEpoch)
	ready := routeStreamReceive(t, stream)
	require.Equal(t, rpc.RouteIntentSnapshot_AUTHORITATIVE, ready.SyncState)
	require.Empty(t, ready.Intents)
	client.PrependReactor("list", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("API temporarily unavailable")
	})
	m.refresh(t.Context())
	require.Equal(t, rpc.RouteIntentSnapshot_SYNCHRONIZING, routeStreamReceive(t, stream).SyncState)
	client.ReactionChain = client.ReactionChain[1:]
	m.refresh(t.Context())
	require.Equal(t, rpc.RouteIntentSnapshot_AUTHORITATIVE, routeStreamReceive(t, stream).SyncState)
	cancel()
	require.NoError(t, <-done)
	_, stillReady, _ := m.snapshot()
	canceled, stop := context.WithCancel(t.Context())
	stop()
	m.refresh(canceled)
	_, afterCanceled, _ := m.snapshot()
	require.Equal(t, stillReady, afterCanceled, "an individually canceled watch cannot revoke global authority")
}

func TestRouteIntentWorkloadWatchRequiresPodBoundCallerEvenForOldSession(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	conn, mgr, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t, nil)
	t.Cleanup(func() { _ = conn.Close() })
	svc := mgr.(*service)
	_, store := newManagerRouteStore()
	svc.routeIntents = newRouteIntentManager(store, nil)
	const sessionID tunnel.SessionID = "agent:legacy-pod"
	agent := &rpc.AgentInfo{Name: "web", Kind: "Deployment", Namespace: "default", PodName: "web-abc", PodUid: "pod-uid", PodIp: "10.0.0.1"}
	_, err := svc.state.RestoreAgent(ctx, sessionID, agent, nil, time.Now())
	require.NoError(t, err)
	session := &rpc.SessionInfo{SessionId: string(sessionID)}
	for _, caller := range []*auth.Principal{
		{Username: "alice"},
		{Username: "system:serviceaccount:default:web", PodName: agent.PodName, PodUID: "other-pod"},
		{Username: "system:serviceaccount:another:web", PodName: agent.PodName, PodUID: agent.PodUid},
	} {
		stream := &managerRouteStream{ctx: auth.WithPrincipal(ctx, caller), sent: make(chan *rpc.RouteIntentSnapshot, 2)}
		require.Equal(t, codes.PermissionDenied, status.Code(svc.WatchRouteIntents(session, stream)))
		require.Empty(t, stream.sent)
	}
	watchCtx, cancel := context.WithCancel(auth.WithPrincipal(ctx, &auth.Principal{Username: "system:serviceaccount:default:web", PodName: agent.PodName, PodUID: agent.PodUid}))
	stream := &managerRouteStream{ctx: watchCtx, sent: make(chan *rpc.RouteIntentSnapshot, 2)}
	done := make(chan error, 1)
	go func() { done <- svc.WatchRouteIntents(session, stream) }()
	require.Equal(t, rpc.RouteIntentSnapshot_SYNCHRONIZING, routeStreamReceive(t, stream).SyncState)
	require.Equal(t, rpc.RouteIntentSnapshot_AUTHORITATIVE, routeStreamReceive(t, stream).SyncState)
	cancel()
	require.NoError(t, <-done)
}

func TestRouteIntentOwnerAndSupportedPredicateAreNeverInferred(t *testing.T) {
	_, session, owner := managerRouteIdentity(t)
	got, err := durableRouteOwner(session)
	require.NoError(t, err)
	require.Equal(t, "session/"+session.SessionId, got)
	changed, err := durableRouteOwner(&rpc.SessionInfo{SessionId: "new-session"})
	require.NoError(t, err)
	require.NotEqual(t, owner, changed)
	for _, invalid := range []string{"", "agent:123", "invalid/session"} {
		_, err = durableRouteOwner(&rpc.SessionInfo{SessionId: invalid})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Empty(t, durableRouteOwnerSession("session/"+invalid))
	}
	require.Empty(t, durableRouteOwnerSession("wrong/"+session.SessionId))
	require.Empty(t, durableRouteOwnerSession("gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg/"+session.SessionId))
	spec := managerRouteSpec()
	_, ok := durableRoutePredicate(spec)
	require.True(t, ok)
	spec.PodPorts = []string{"8081:8081"}
	_, ok = durableRoutePredicate(spec)
	require.False(t, ok)
	spec = managerRouteSpec()
	spec.HeaderFilters["X-Other"] = "value"
	_, ok = durableRoutePredicate(spec)
	require.False(t, ok)
	require.True(t, usesLocalRoutingKey(spec))
}

func TestRouteIntentProjectsAllServiceWorkloadsAndScopesAgents(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	sharedService := &core.Service{
		ObjectMeta: meta.ObjectMeta{Name: "shared", Namespace: "default", UID: "shared-uid"},
		Spec:       core.ServiceSpec{Selector: map[string]string{"app": "shared"}, Ports: []core.ServicePort{{Port: 80, Protocol: core.ProtocolTCP, TargetPort: intstr.FromString("app-port")}}},
	}
	makeDeployment := func(name, label string, port int32) *apps.Deployment {
		return &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: name, Namespace: "default"}, Spec: apps.DeploymentSpec{
			Selector: &meta.LabelSelector{MatchLabels: map[string]string{"app": label}},
			Template: core.PodTemplateSpec{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{"app": label}}, Spec: core.PodSpec{Containers: []core.Container{{
				Name: "app", Ports: []core.ContainerPort{{Name: "app-port", ContainerPort: port, Protocol: core.ProtocolTCP}},
			}}}},
		}}
	}
	conn, mgr, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t,
		[]runtime.Object{sharedService, makeDeployment("web", "shared", 8080), makeDeployment("canary", "shared", 8081), makeDeployment("unrelated", "other", 8082)},
		func(env *managerutil.Env) { env.EnabledWorkloadKinds = k8sapi.Kinds{k8sapi.DeploymentKind} })
	t.Cleanup(func() { _ = conn.Close() })
	svc := mgr.(*service)
	_, session, owner := managerRouteIdentity(t)
	spec := managerRouteSpec()
	spec.ServiceName, spec.ServiceUid, spec.ServicePort = "shared", "shared-uid", 80
	predicate, ok := durableRoutePredicate(spec)
	require.True(t, ok)
	record := routeintent.Record{
		Key:      routeintent.Key{Namespace: "default", Owner: owner, Name: spec.Name},
		Revision: routeintent.Revision{Incarnation: "one", Version: 1}, State: routeintent.Desired, Predicate: predicate,
	}
	intents, err := svc.projectRouteIntents(ctx, []routeintent.Record{record}, nil)
	require.NoError(t, err)
	require.Len(t, intents, 1)
	require.Equal(t, session.SessionId+":"+spec.Name, intents[0].InterceptId)
	require.Len(t, intents[0].Targets, 2)
	require.Equal(t, "web", intents[0].Targets[0].WorkloadName)
	require.Equal(t, int32(8080), intents[0].Targets[0].ContainerPort)
	require.Equal(t, "canary", intents[0].Targets[1].WorkloadName)
	require.Equal(t, int32(8081), intents[0].Targets[1].ContainerPort)
	makeAgent := func(name string, port int32) *state.AgentSession {
		return &state.AgentSession{AgentInfo: &rpc.AgentInfo{Name: name, Kind: "Deployment", Namespace: "default", InterceptTargets: []*rpc.AgentInfo_InterceptTarget{{
			ServiceName: "shared", ServiceUid: "shared-uid", ServicePort: 80, Protocol: "TCP", ContainerPort: port,
		}}}}
	}
	intents, err = svc.projectRouteIntents(ctx, []routeintent.Record{record}, makeAgent("canary", 8081))
	require.NoError(t, err)
	require.Len(t, intents, 1)
	require.Equal(t, "canary", intents[0].Targets[0].WorkloadName)
	require.Equal(t, int32(8081), intents[0].Targets[0].ContainerPort)
	intents, err = svc.projectRouteIntents(ctx, []routeintent.Record{record}, makeAgent("unrelated", 8082))
	require.NoError(t, err)
	require.Empty(t, intents, "an obsolete or forged agent target cannot bypass the Service selector")

	client := k8sapi.GetK8sInterface(ctx)
	sharedService, err = client.CoreV1().Services("default").Get(ctx, "shared", meta.GetOptions{})
	require.NoError(t, err)
	sharedService.Spec.Ports[0].TargetPort = intstr.FromString("missing")
	_, err = client.CoreV1().Services("default").Update(ctx, sharedService, meta.UpdateOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		intents, err = svc.projectRouteIntents(ctx, []routeintent.Record{record}, nil)
		return err != nil && intents == nil
	}, 2*time.Second, 10*time.Millisecond, "unknown shared targets must never produce a partial authoritative global snapshot")
}
