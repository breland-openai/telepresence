package state

import (
	"context"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

const durableDialWatchOwner tunnel.SessionID = "client"

func newDurableDialWatch(t *testing.T, name string) (context.Context, *State, *Intercept, *AgentSession, *AgentSession) {
	t.Helper()
	ctx, st := newServiceInterceptState(t)
	st.leases = xsync.NewMap[leaseKey, struct{}]()
	st.RestoreClient(durableDialWatchOwner, &rpc.ClientInfo{Name: "owner"}, nil, time.Now())
	st.RestoreClient("other-client", &rpc.ClientInfo{Name: "foreign"}, nil, time.Now())
	primary := serviceAgent("example-service", "primary", "10.0.0.1")
	sibling := serviceAgent("example-service", "sibling", "10.0.0.2")
	st.agents.Store("primary", primary)
	st.agents.Store("sibling", sibling)
	spec := sharedServiceSpec()
	spec.Name = name
	route := &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id: string(durableDialWatchOwner) + ":" + name, RouteIncarnation: "exact-incarnation",
		Spec: spec, Disposition: rpc.InterceptDispositionType_WAITING,
		ClientSession: &rpc.SessionInfo{SessionId: string(durableDialWatchOwner)}, ServiceWorkloads: sharedServiceWorkloads(primary.Name),
	}}
	st.initializeParticipants(route)
	st.intercepts.Store(route.Id, route)
	return ctx, st, route, primary, sibling
}

func approveDurableDialWatch(ctx context.Context, st *State, route *Intercept, primary *AgentSession) *Intercept {
	return st.ApplyAgentReview(ctx, route.Id, primary, &rpc.ReviewInterceptRequest{
		Disposition: rpc.InterceptDispositionType_ACTIVE, PodIp: primary.PodIp,
	})
}

func TestDurableDialWatchStaysOpenWhileForwardingRemainsWaiting(t *testing.T) {
	ctx, st, route, primary, sibling := newDurableDialWatch(t, "route")
	unrelated := serviceAgent("not-selected", "foreign-pod", "10.0.0.3")
	wrongTarget := serviceAgent(primary.Name, "different-service", "10.0.0.4")
	wrongTarget.InterceptTargets[0].ServiceUid = "other-service-uid"

	require.False(t, st.IsInterceptedBy(primary, durableDialWatchOwner), "a newly requested unapproved route cannot open a transport")
	require.False(t, st.IsInterceptedBy(sibling, durableDialWatchOwner))
	route = approveDurableDialWatch(ctx, st, route, primary)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, route.Disposition, "only the data-plane barrier may permit forwarding")
	require.Empty(t, route.PodName)
	require.NotNil(t, route.routePendingActivation)
	require.True(t, st.IsInterceptedBy(primary, durableDialWatchOwner), "a real retained ACTIVE approval may preconnect while forwarding stays waiting")
	require.True(t, st.IsInterceptedBy(sibling, durableDialWatchOwner), "other agents for the same selected Service target need their dial watchers")
	require.False(t, st.IsInterceptedBy(unrelated, durableDialWatchOwner))
	require.False(t, st.IsInterceptedBy(wrongTarget, durableDialWatchOwner))
	require.False(t, st.IsInterceptedBy(primary, "other-client"))
	require.False(t, st.IsInterceptedBy(primary, ""))
	require.False(t, st.IsInterceptedBy(nil, durableDialWatchOwner))

	route = st.SetRouteActivation(route.Id, route.RouteIncarnation, true)
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, route.Disposition)
	require.True(t, st.IsInterceptedBy(primary, durableDialWatchOwner))
	require.True(t, st.IsInterceptedBy(sibling, durableDialWatchOwner))
	route = st.SetRouteActivation(route.Id, route.RouteIncarnation, false)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, route.Disposition)
	require.Empty(t, route.PodName)
	require.True(t, st.IsInterceptedBy(primary, durableDialWatchOwner))
	require.True(t, st.IsInterceptedBy(sibling, durableDialWatchOwner))
	require.False(t, st.IsInterceptedBy(primary, "other-client"))

	route = st.SetRouteActivation(route.Id, route.RouteIncarnation, true)
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, route.Disposition)
	require.True(t, st.IsInterceptedBy(sibling, durableDialWatchOwner))
	route = st.SetRouteActivation(route.Id, route.RouteIncarnation, false)
	route = st.UpdateIntercept(route.Id, func(value *Intercept) { value.Disposition = rpc.InterceptDispositionType_REMOVED })
	require.Nil(t, route.routePendingActivation)
	require.False(t, st.IsInterceptedBy(primary, durableDialWatchOwner), "graceful removal closes the durable transport even while the client remains connected")
	require.False(t, st.IsInterceptedBy(sibling, durableDialWatchOwner))
}

func TestDurableDialWatchRevokesWhenOwnerDisconnects(t *testing.T) {
	for _, active := range []bool{false, true} {
		name := "waiting"
		if active {
			name = "active"
		}
		t.Run(name, func(t *testing.T) {
			ctx, st, route, primary, sibling := newDurableDialWatch(t, name)
			route = approveDurableDialWatch(ctx, st, route, primary)
			if active {
				route = st.SetRouteActivation(route.Id, route.RouteIncarnation, true)
			}
			require.True(t, st.IsInterceptedBy(sibling, durableDialWatchOwner))
			owner := st.GetClient(durableDialWatchOwner)
			owner.cancel()
			require.False(t, st.IsInterceptedBy(primary, durableDialWatchOwner), "closed owner stops authorization before asynchronous state cleanup")
			require.False(t, st.IsInterceptedBy(sibling, "other-client"))
			st.RemoveSession(ctx, durableDialWatchOwner)
			require.Nil(t, st.GetClient(durableDialWatchOwner))
			_, exists := st.GetIntercept(route.Id)
			require.False(t, exists)
			require.False(t, st.IsInterceptedBy(sibling, durableDialWatchOwner))
		})
	}
	t.Run("owner has no live lifecycle", func(t *testing.T) {
		ctx, st, route, primary, _ := newDurableDialWatch(t, "invalid-owner")
		approveDurableDialWatch(ctx, st, route, primary)
		st.clients.Store(durableDialWatchOwner, &ClientSession{ClientInfo: &rpc.ClientInfo{Name: "incomplete"}})
		require.False(t, st.IsInterceptedBy(primary, durableDialWatchOwner))
	})
}

func TestDurableDialWatchRejectsMismatchedOrInvalidLatentApproval(t *testing.T) {
	ctx, st, route, primary, _ := newDurableDialWatch(t, "route")
	route = approveDurableDialWatch(ctx, st, route, primary)
	require.True(t, st.IsInterceptedBy(primary, durableDialWatchOwner))
	for _, test := range []struct {
		name   string
		change func(*Intercept)
	}{
		{"missing", func(value *Intercept) { value.routePendingActivation = nil }},
		{"inactive approval", func(value *Intercept) {
			value.routePendingActivation.Disposition = rpc.InterceptDispositionType_WAITING
		}},
		{"other route", func(value *Intercept) { value.routePendingActivation.Id = "client:different" }},
		{"other incarnation", func(value *Intercept) { value.routePendingActivation.RouteIncarnation = "old-incarnation" }},
		{"different owner", func(value *Intercept) { value.routePendingActivation.ClientSession.SessionId = "other-client" }},
		{"missing owner", func(value *Intercept) { value.routePendingActivation.ClientSession = nil }},
		{"legacy", func(value *Intercept) { value.RouteIncarnation = "" }},
		{"agent rejected", func(value *Intercept) { value.Disposition = rpc.InterceptDispositionType_AGENT_ERROR }},
		{"no client", func(value *Intercept) { value.Disposition = rpc.InterceptDispositionType_NO_CLIENT }},
		{"removed", func(value *Intercept) { value.Disposition = rpc.InterceptDispositionType_REMOVED }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := route.Clone()
			changed.routePendingActivation = proto.CloneOf(route.routePendingActivation)
			test.change(changed)
			st.intercepts.Store(changed.Id, changed)
			require.False(t, st.IsInterceptedBy(primary, durableDialWatchOwner))
		})
	}
}

func TestLegacyDialWatchRetainsExistingActiveOnlyBehavior(t *testing.T) {
	ctx, st, route, primary, sibling := newDurableDialWatch(t, "legacy")
	route.RouteIncarnation = ""
	st.intercepts.Store(route.Id, route)
	require.False(t, st.IsInterceptedBy(primary, durableDialWatchOwner))
	route = approveDurableDialWatch(ctx, st, route, primary)
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, route.Disposition)
	require.True(t, st.IsInterceptedBy(sibling, durableDialWatchOwner))
	st.UpdateIntercept(route.Id, func(value *Intercept) { value.Disposition = rpc.InterceptDispositionType_WAITING })
	require.False(t, st.IsInterceptedBy(sibling, durableDialWatchOwner), "legacy waiting continues to close transport")
}
