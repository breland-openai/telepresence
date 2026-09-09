package state

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

func protectedReview(agent *AgentSession) *rpc.ReviewInterceptRequest {
	return &rpc.ReviewInterceptRequest{
		Disposition: rpc.InterceptDispositionType_ACTIVE, PodIp: agent.PodIp,
		Environment: map[string]string{"POD": agent.PodName},
	}
}

func TestDurableLatentServiceApprovalDoesNotEchoSiblingReviews(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	primary := serviceAgent("example-service", "pod-a", "10.0.0.1")
	st.agents.Store("primary", primary)
	siblings := []*AgentSession{
		serviceAgent("example-service", "pod-b", "10.0.0.2"),
		serviceAgent("example-service", "pod-c", "10.0.0.3"),
		serviceAgent("example-service", "pod-d", "10.0.0.4"),
	}
	for i, sibling := range siblings {
		st.agents.Store(tunnel.SessionID(fmt.Sprintf("sibling-%d", i)), sibling)
	}
	initial := &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id: "client:route", Spec: sharedServiceSpec(), RouteIncarnation: "one",
		Disposition: rpc.InterceptDispositionType_WAITING, ClientSession: &rpc.SessionInfo{SessionId: "client"},
		ServiceWorkloads: sharedServiceWorkloads(primary.Name),
	}}
	st.initializeParticipants(initial)
	st.intercepts.Store(initial.Id, initial)
	latent := st.ApplyAgentReview(ctx, initial.Id, primary, protectedReview(primary))
	require.Equal(t, rpc.InterceptDispositionType_WAITING, latent.Disposition)
	require.Equal(t, primary.PodName, latent.routePendingActivation.PodName)
	for round := range 25 {
		for _, sibling := range siblings {
			value := st.ApplyAgentReview(ctx, initial.Id, sibling, protectedReview(sibling))
			require.Same(t, latent, value, "round %d must publish no delta for an equivalent sibling", round)
		}
	}
	key := agentParticipantKey(primary.AgentInfo)
	require.Equal(t, primary.PodName, latent.participants[key].podName)
	require.Equal(t, primary.PodIp, latent.participants[key].review.PodIp)
	active := st.SetRouteActivation(initial.Id, "one", true)
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, active.Disposition)
	require.Equal(t, primary.PodName, active.PodName)
	latent = st.SetRouteActivation(initial.Id, "one", false)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, latent.Disposition)
	require.Empty(t, latent.PodName)
	require.Same(t, latent, st.ApplyAgentReview(ctx, initial.Id, siblings[0], protectedReview(siblings[0])))

	updated := protectedReview(primary)
	updated.Environment["CURRENT_REVIEWER_UPDATE"] = "allowed"
	latent = st.ApplyAgentReview(ctx, initial.Id, primary, updated)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, latent.Disposition)
	require.Equal(t, "allowed", latent.participants[key].review.Environment["CURRENT_REVIEWER_UPDATE"])
	require.Same(t, latent, st.ApplyAgentReview(ctx, initial.Id, primary, updated), "an exact reviewer retry does not notify")
	active = st.SetRouteActivation(initial.Id, "one", true)
	require.Equal(t, "allowed", active.Environment["CURRENT_REVIEWER_UPDATE"])
}

func TestDurableLatentServiceReviewerCanStillBeReelectedAndReject(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	primary := serviceAgent("example-service", "pod-a", "10.0.0.1")
	survivor := serviceAgent("example-service", "pod-b", "10.0.0.2")
	other := serviceAgent("example-service", "pod-c", "10.0.0.3")
	st.agents.Store("primary", primary)
	st.agents.Store("survivor", survivor)
	st.agents.Store("other", other)
	initial := &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id: "client:route", Spec: sharedServiceSpec(), RouteIncarnation: "one",
		Disposition: rpc.InterceptDispositionType_WAITING, ClientSession: &rpc.SessionInfo{SessionId: "client"},
		ServiceWorkloads: sharedServiceWorkloads(primary.Name),
	}}
	st.initializeParticipants(initial)
	st.intercepts.Store(initial.Id, initial)
	firstReview := protectedReview(primary)
	firstReview.Environment = nil // fixture siblings do not advertise container environment for transfer
	latent := st.ApplyAgentReview(ctx, initial.Id, primary, firstReview)
	require.Same(t, latent, st.ApplyAgentReview(ctx, initial.Id, survivor, protectedReview(survivor)))
	st.agents.Delete("primary")
	st.consolidateAgentSessionIntercepts(primary)
	latent, ok := st.GetIntercept(initial.Id)
	require.True(t, ok)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, latent.Disposition)
	require.Equal(t, survivor.PodName, latent.routePendingActivation.PodName)
	active := st.SetRouteActivation(initial.Id, "one", true)
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, active.Disposition)
	require.Equal(t, survivor.PodName, active.PodName)
	st.SetRouteActivation(initial.Id, "one", false)
	rejection := &rpc.ReviewInterceptRequest{Disposition: rpc.InterceptDispositionType_AGENT_ERROR, Message: "explicit rejection from matching replica"}
	rejected := st.ApplyAgentReview(ctx, initial.Id, other, rejection)
	require.Equal(t, rpc.InterceptDispositionType_AGENT_ERROR, rejected.Disposition)
	require.Nil(t, rejected.routePendingActivation)
	require.Equal(t, rpc.InterceptDispositionType_AGENT_ERROR, st.SetRouteActivation(initial.Id, "one", true).Disposition)
}

func TestDurableLatentServiceReviewerCanBeReelectedAfterUntransferableApproval(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	primary := serviceAgent("example-service", "pod-a", "10.0.0.1")
	survivor := serviceAgent("example-service", "pod-b", "10.0.0.2")
	st.agents.Store("primary", primary)
	st.agents.Store("survivor", survivor)
	initial := &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id: "client:route", Spec: sharedServiceSpec(), RouteIncarnation: "one",
		Disposition: rpc.InterceptDispositionType_WAITING, ClientSession: &rpc.SessionInfo{SessionId: "client"},
		ServiceWorkloads: sharedServiceWorkloads(primary.Name),
	}}
	st.initializeParticipants(initial)
	st.intercepts.Store(initial.Id, initial)
	st.ApplyAgentReview(ctx, initial.Id, primary, protectedReview(primary))
	st.agents.Delete("primary")
	st.consolidateAgentSessionIntercepts(primary)
	waiting, ok := st.GetIntercept(initial.Id)
	require.True(t, ok)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, waiting.Disposition)
	require.Nil(t, waiting.routePendingActivation, "the old environment cannot be transferred from an unregistered container")
	waiting = st.ApplyAgentReview(ctx, initial.Id, survivor, protectedReview(survivor))
	require.Equal(t, rpc.InterceptDispositionType_WAITING, waiting.Disposition)
	require.Equal(t, survivor.PodName, waiting.routePendingActivation.PodName)
	require.Equal(t, survivor.PodName, st.SetRouteActivation(initial.Id, "one", true).PodName)
}

func TestDurableLatentServiceReviewFeedbackConcurrentWithBarrier(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	primary := serviceAgent("example-service", "pod-a", "10.0.0.1")
	sibling := serviceAgent("example-service", "pod-b", "10.0.0.2")
	st.agents.Store("primary", primary)
	st.agents.Store("sibling", sibling)
	initial := &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id: "client:route", Spec: sharedServiceSpec(), RouteIncarnation: "one",
		Disposition: rpc.InterceptDispositionType_WAITING, ClientSession: &rpc.SessionInfo{SessionId: "client"},
		ServiceWorkloads: sharedServiceWorkloads(primary.Name),
	}}
	st.initializeParticipants(initial)
	st.intercepts.Store(initial.Id, initial)
	st.ApplyAgentReview(ctx, initial.Id, primary, protectedReview(primary))
	var group sync.WaitGroup
	for worker := range 12 {
		group.Go(func() {
			for round := range 80 {
				if worker == 0 {
					st.SetRouteActivation(initial.Id, "one", round%2 == 0)
				} else {
					st.ApplyAgentReview(ctx, initial.Id, sibling, protectedReview(sibling))
				}
			}
		})
	}
	group.Wait()
	active := st.SetRouteActivation(initial.Id, "one", true)
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, active.Disposition)
	require.Equal(t, primary.PodName, active.PodName)
	require.Equal(t, primary.PodName, active.participants[agentParticipantKey(primary.AgentInfo)].podName)
}
