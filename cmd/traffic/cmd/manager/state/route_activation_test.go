package state

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/cache"
)

func TestDurableRouteActivationGatesAllStatePublishPaths(t *testing.T) {
	st := &State{intercepts: cache.NewMap[string, *Intercept](interceptEqual, time.Millisecond)}
	initial := &Intercept{InterceptInfo: &rpc.InterceptInfo{Id: "client:route", RouteIncarnation: "one", Disposition: rpc.InterceptDispositionType_WAITING, Spec: &rpc.InterceptSpec{Name: "route"}}}
	st.intercepts.Store(initial.Id, initial)
	result := st.UpdateIntercept(initial.Id, func(value *Intercept) {
		value.Disposition = rpc.InterceptDispositionType_ACTIVE
		value.PodName = "fast-replica"
		value.PodIp = "10.0.0.1"
	})
	require.Equal(t, rpc.InterceptDispositionType_WAITING, result.Disposition)
	require.Empty(t, result.PodName)
	require.NotNil(t, result.routePendingActivation)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, st.SetRouteActivation(initial.Id, "old", true).Disposition)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, st.SetRouteActivation(initial.Id, "one", false).Disposition)
	result = st.SetRouteActivation(initial.Id, "one", true)
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, result.Disposition)
	require.Equal(t, "fast-replica", result.PodName)
	result = st.SetRouteActivation(initial.Id, "one", false)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, result.Disposition, "a newly routable unacknowledged replica revokes ACTIVE")
	require.Empty(t, result.PodName)
	result = st.SetRouteActivation(initial.Id, "one", true)
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, result.Disposition)
	result = st.SetRouteActivation(initial.Id, "one", false)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, result.Disposition)
	result = st.UpdateIntercept(initial.Id, func(value *Intercept) {
		value.Disposition = rpc.InterceptDispositionType_AGENT_ERROR
		value.Message = "new rejection"
	})
	require.Nil(t, result.routePendingActivation)
	result = st.SetRouteActivation(initial.Id, "one", true)
	require.Equal(t, rpc.InterceptDispositionType_AGENT_ERROR, result.Disposition, "completed guard proof cannot resurrect rejected runtime state")
	legacy := &Intercept{InterceptInfo: &rpc.InterceptInfo{Id: "client:legacy", Disposition: rpc.InterceptDispositionType_WAITING}}
	st.intercepts.Store(legacy.Id, legacy)
	result = st.UpdateIntercept(legacy.Id, func(value *Intercept) { value.Disposition = rpc.InterceptDispositionType_ACTIVE })
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, result.Disposition)
}

func TestDurableRouteRestoredActiveSnapshotStartsWaiting(t *testing.T) {
	restored := &Intercept{InterceptInfo: &rpc.InterceptInfo{Id: "client:route", RouteIncarnation: "one", Disposition: rpc.InterceptDispositionType_ACTIVE, PodName: "stale-runtime", PodIp: "10.0.0.1"}}
	restored.enforceRouteActivationBarrier(nil)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, restored.Disposition)
	require.Empty(t, restored.PodName)
	require.Equal(t, "stale-runtime", restored.routePendingActivation.PodName)
}
