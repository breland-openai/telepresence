package state

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/clog/testutil"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/mutator"
	"github.com/telepresenceio/telepresence/v2/pkg/cache"
	"github.com/telepresenceio/telepresence/v2/pkg/log"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

func newServiceInterceptState(t *testing.T) (context.Context, *State) {
	t.Helper()
	ctx := testutil.NewContext(t, false)
	ctx = mutator.WithMap(ctx, mutator.NewWatcher())
	return ctx, &State{
		backgroundCtx:    ctx,
		intercepts:       cache.NewMap[string, *Intercept](interceptEqual, time.Millisecond),
		agents:           cache.NewMap[tunnel.SessionID, *AgentSession](agentsEqual, time.Millisecond),
		clients:          xsync.NewMap[tunnel.SessionID, *ClientSession](),
		workloadWatchers: xsync.NewMap[string, Watcher](),
		timedLogLevel:    log.NewTimedLevel(slog.LevelDebug, clog.SetTreeLevel),
		llSubs:           newLoglevelSubscribers(),
	}
}

func serviceAgent(name, podName, podIP string) *AgentSession {
	return &AgentSession{AgentInfo: &rpc.AgentInfo{
		Name:      name,
		Kind:      "Deployment",
		Namespace: "default",
		PodName:   podName,
		PodIp:     podIP,
		PodUid:    podName + "-uid",
		Mechanisms: []*rpc.AgentInfo_Mechanism{{
			Name:    "http",
			Product: "telepresence",
			Version: "v2.29.2",
		}},
		InterceptTargets: []*rpc.AgentInfo_InterceptTarget{{
			ServiceUid:  "shared-service-uid",
			ServiceName: "plugin-service",
			ServicePort: 80,
			Protocol:    "TCP",
		}},
	}}
}

func sharedServiceSpec() *rpc.InterceptSpec {
	return &rpc.InterceptSpec{
		Name:         "plugin-service",
		Client:       "alice",
		Agent:        "plugin-service",
		WorkloadKind: "Deployment",
		Namespace:    "default",
		Mechanism:    "http",
		ServiceUid:   "shared-service-uid",
		ServiceName:  "plugin-service",
		ServicePort:  80,
		Protocol:     "TCP",
	}
}

func TestServiceInterceptWaitsForEveryWorkload(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	stable := serviceAgent("plugin-service", "stable-pod", "10.0.0.1")
	canary := serviceAgent("plugin-service-canary", "canary-pod", "10.0.0.2")
	st.agents.Store("stable", stable)
	st.agents.Store("canary", canary)

	intercept := &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id:            "client:plugin-service",
		Spec:          sharedServiceSpec(),
		Disposition:   rpc.InterceptDispositionType_WAITING,
		ClientSession: &rpc.SessionInfo{SessionId: "client"},
	}}
	st.initializeParticipants(intercept)
	require.Len(t, intercept.participants, 2)
	require.Len(t, intercept.ServiceWorkloads, 2)
	require.Equal(t, "plugin-service", intercept.ServiceWorkloads[0].WorkloadName)
	require.Equal(t, "plugin-service-canary", intercept.ServiceWorkloads[1].WorkloadName)
	st.intercepts.Store(intercept.Id, intercept)

	stableReview := &rpc.ReviewInterceptRequest{
		Disposition: rpc.InterceptDispositionType_ACTIVE,
		PodIp:       stable.PodIp,
		Environment: map[string]string{"TRACK": "stable"},
	}
	updated := st.ApplyAgentReview(ctx, intercept.Id, stable, stableReview)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, updated.Disposition)
	require.Contains(t, updated.Message, "plugin-service-canary")

	updated = st.ApplyAgentReview(ctx, intercept.Id, canary, &rpc.ReviewInterceptRequest{
		Disposition: rpc.InterceptDispositionType_ACTIVE,
		PodIp:       canary.PodIp,
		Environment: map[string]string{"TRACK": "canary"},
	})
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, updated.Disposition)
	require.Equal(t, stable.PodIp, updated.PodIp)
	require.Equal(t, stable.PodName, updated.PodName)
	require.Equal(t, map[string]string{"TRACK": "stable"}, updated.Environment)
	require.True(t, st.IsInterceptedBy(stable, "client"))
	require.True(t, st.IsInterceptedBy(canary, "client"))
}

func TestServiceInterceptPublishesLateWorkload(t *testing.T) {
	_, st := newServiceInterceptState(t)
	stable := serviceAgent("plugin-service", "stable-pod", "10.0.0.1")
	canary := serviceAgent("plugin-service-canary", "canary-pod", "10.0.0.2")
	st.agents.Store("stable", stable)

	intercept := &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id:            "client:plugin-service",
		Spec:          sharedServiceSpec(),
		Disposition:   rpc.InterceptDispositionType_ACTIVE,
		ClientSession: &rpc.SessionInfo{SessionId: "client"},
	}}
	st.initializeParticipants(intercept)
	require.Len(t, intercept.ServiceWorkloads, 1)
	require.Equal(t, "plugin-service", intercept.ServiceWorkloads[0].WorkloadName)

	intercept.addParticipant(canary.AgentInfo)
	require.Len(t, intercept.ServiceWorkloads, 2)
	require.Equal(t, "plugin-service", intercept.ServiceWorkloads[0].WorkloadName)
	require.Equal(t, "plugin-service-canary", intercept.ServiceWorkloads[1].WorkloadName)
}

func TestUpdateInterceptSkipsExistingParticipantNoOp(t *testing.T) {
	_, st := newServiceInterceptState(t)
	stable := serviceAgent("plugin-service", "stable-pod", "10.0.0.1")
	intercept := &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id:            "client:plugin-service",
		Spec:          sharedServiceSpec(),
		Disposition:   rpc.InterceptDispositionType_ACTIVE,
		ClientSession: &rpc.SessionInfo{SessionId: "client"},
	}}
	intercept.addParticipant(stable.AgentInfo)
	st.intercepts.Store(intercept.Id, intercept)

	updated := st.UpdateIntercept(intercept.Id, func(intercept *Intercept) {
		intercept.addParticipant(stable.AgentInfo)
	})

	require.Same(t, intercept, updated)
}

func TestAgentMatchesServiceInterceptByClaim(t *testing.T) {
	spec := sharedServiceSpec()
	require.True(t, AgentMatchesIntercept(serviceAgent("plugin-service-canary", "pod", "10.0.0.2").AgentInfo, spec))

	agent := serviceAgent("plugin-service-canary", "pod", "10.0.0.2").AgentInfo
	agent.InterceptTargets[0].ServiceUid = "other-service"
	require.False(t, AgentMatchesIntercept(agent, spec))

	legacyPrimary := serviceAgent("plugin-service", "pod", "10.0.0.1").AgentInfo
	legacyPrimary.InterceptTargets = nil
	require.True(t, AgentMatchesIntercept(legacyPrimary, spec))

	legacyCanary := serviceAgent("plugin-service-canary", "pod", "10.0.0.2").AgentInfo
	legacyCanary.InterceptTargets = nil
	require.False(t, AgentMatchesIntercept(legacyCanary, spec))
}

func TestServiceScopesOverlap(t *testing.T) {
	spec := sharedServiceSpec()
	same := sharedServiceSpec()
	same.Agent = "plugin-service-canary"
	require.True(t, serviceScopesOverlap(spec, same))

	otherPort := sharedServiceSpec()
	otherPort.ServicePort = 81
	require.False(t, serviceScopesOverlap(spec, otherPort))

	otherUID := sharedServiceSpec()
	otherUID.ServiceUid = "other-service"
	require.False(t, serviceScopesOverlap(spec, otherUID))
}

func TestServiceSelectsPod(t *testing.T) {
	svc := &core.Service{}
	pod := &core.Pod{Status: core.PodStatus{Conditions: []core.PodCondition{{
		Type:   core.PodReady,
		Status: core.ConditionTrue,
	}}}}
	require.True(t, serviceSelectsPod(svc, pod))

	pod.Status.Conditions[0].Status = core.ConditionFalse
	require.False(t, serviceSelectsPod(svc, pod))

	svc.Spec.PublishNotReadyAddresses = true
	require.True(t, serviceSelectsPod(svc, pod))

	now := meta.Now()
	pod.DeletionTimestamp = &now
	require.False(t, serviceSelectsPod(svc, pod))
}
