package state

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	testdata "github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/test"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

func TestDifferentPodCannotBypassRegistrationOfSameAgentSessionID(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	now := time.Now()
	agents := testdata.GetTestAgents(t)
	first, second := agents["hello"], agents["demo1"]
	require.NotEqual(t, first.PodUid, second.PodUid)
	id := tunnel.SessionID("agent:caller-supplied-shared-id")
	release := holdIdentityTestIntercept(t, st, first)

	firstResult := startIdentityTestRestore(t, ctx, st, id, first, now, release)
	require.Eventually(t, func() bool { return st.GetAgent(id) != nil }, time.Second, time.Millisecond)
	require.Equal(t, first.PodUid, st.GetAgent(id).PodUid)

	secondResult := startIdentityTestRestore(t, ctx, st, id, second, now, release)
	requireIdentityTestRestoreBlocked(t, firstResult)
	requireIdentityTestRestoreBlocked(t, secondResult)
	release()
	for _, result := range []<-chan identityRestoreResult{firstResult, secondResult} {
		restored := awaitIdentityTestRestore(t, result)
		require.NoError(t, restored.err)
		require.Equal(t, id, restored.id)
	}
	require.Equal(t, first.PodUid, st.GetAgent(id).PodUid)
}

func TestDifferentPodCannotBypassExpiryOfSameAgentSessionID(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	now := time.Now()
	agents := testdata.GetTestAgents(t)
	first, second := agents["hello"], agents["demo1"]
	require.NotEqual(t, first.PodUid, second.PodUid)
	id := tunnel.SessionID("agent:caller-supplied-shared-id")
	_, err := st.RestoreAgent(ctx, id, first, nil, now.Add(-2*agentSessionTTL))
	require.NoError(t, err)
	old := st.GetAgent(id)
	require.NotNil(t, old)
	release := holdIdentityTestIntercept(t, st, first)

	expired := make(chan struct{})
	go func() {
		defer close(expired)
		st.expireAgentSession(old, now.Add(-agentSessionTTL))
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-expired:
		case <-time.After(time.Second):
			t.Error("agent expiry did not finish after intercept was released")
		}
	})
	requireClosedAgentSession(t, old.done())
	require.Nil(t, st.GetAgent(id))

	secondResult := startIdentityTestRestore(t, ctx, st, id, second, now, release)
	requireIdentityTestRestoreBlocked(t, secondResult)
	require.Nil(t, st.GetAgent(id), "the second pod must not publish over an expiry still reconciling the same session ID")
	release()
	restored := awaitIdentityTestRestore(t, secondResult)
	require.NoError(t, restored.err)
	require.Equal(t, id, restored.id)
	require.Equal(t, second.PodUid, st.GetAgent(id).PodUid)
}

type identityRestoreResult struct {
	id  tunnel.SessionID
	err error
}

func holdIdentityTestIntercept(t *testing.T, st *State, agent *rpc.AgentInfo) func() {
	t.Helper()
	const interceptID = "identity-test-intercept"
	st.intercepts.Store(interceptID, &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id:          interceptID,
		Disposition: rpc.InterceptDispositionType_ACTIVE,
		PodName:     agent.PodName,
		PodIp:       agent.PodIp,
		Spec: &rpc.InterceptSpec{
			Agent:     agent.Name,
			Namespace: agent.Namespace,
			Mechanism: "tcp",
		},
	}})
	unlock := st.agentInterceptLocks.lock(interceptID)
	var once sync.Once
	release := func() { once.Do(unlock) }
	t.Cleanup(release)
	return release
}

func startIdentityTestRestore(t *testing.T, ctx context.Context, st *State, id tunnel.SessionID, agent *rpc.AgentInfo, now time.Time, release func()) <-chan identityRestoreResult {
	t.Helper()
	started := make(chan struct{})
	done := make(chan struct{})
	result := make(chan identityRestoreResult, 1)
	go func() {
		defer close(done)
		close(started)
		id, err := st.RestoreAgent(ctx, id, agent, nil, now)
		result <- identityRestoreResult{id, err}
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("agent restore did not finish after intercept was released")
		}
	})
	<-started
	return result
}

func requireIdentityTestRestoreBlocked(t *testing.T, result <-chan identityRestoreResult) {
	t.Helper()
	select {
	case restored := <-result:
		t.Fatalf("agent restore bypassed outstanding reconciliation for the same session ID: id=%q err=%v", restored.id, restored.err)
	case <-time.After(50 * time.Millisecond):
	}
}

func awaitIdentityTestRestore(t *testing.T, result <-chan identityRestoreResult) identityRestoreResult {
	t.Helper()
	select {
	case restored := <-result:
		return restored
	case <-time.After(time.Second):
		t.Fatal("agent restore did not finish after intercept was released")
		return identityRestoreResult{}
	}
}
