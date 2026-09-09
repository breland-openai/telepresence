package state

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/mutator"
	testdata "github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/test"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

func TestExpiredAgentSessionCanReconnect(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	now := time.Now()
	info := testdata.GetTestAgents(t)["hello"]
	id, err := st.AddAgent(ctx, info, nil, now.Add(-2*agentSessionTTL))
	require.NoError(t, err)
	old := st.GetAgent(id)
	done, err := st.SessionDone(id)
	require.NoError(t, err)
	st.intercepts.Store("intercept", &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id:          "intercept",
		Disposition: rpc.InterceptDispositionType_ACTIVE,
		PodName:     info.PodName,
		PodIp:       info.PodIp,
		Spec: &rpc.InterceptSpec{
			Agent:     info.Name,
			Namespace: info.Namespace,
			Mechanism: "tcp",
		},
	}})

	st.expireSessions(now.Add(-agentSessionTTL), now.Add(-agentSessionTTL))
	requireClosedAgentSession(t, done)
	require.Nil(t, st.GetAgent(id))
	require.False(t, mutator.GetMap(ctx).IsInactive(types.UID(info.PodUid)))
	intercept, ok := st.intercepts.Load("intercept")
	require.True(t, ok)
	require.Equal(t, rpc.InterceptDispositionType_NO_AGENT, intercept.Disposition)

	restoredID, err := st.RestoreAgent(ctx, id, info, nil, now)
	require.NoError(t, err)
	require.Equal(t, id, restoredID)
	replacement := st.GetAgent(id)
	require.NotNil(t, replacement)
	require.NotSame(t, old, replacement)
	intercept, ok = st.intercepts.Load("intercept")
	require.True(t, ok)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, intercept.Disposition)

	// A delayed Depart or GC observation belongs to the old session.
	st.RemoveAgentSession(old)
	st.expireAgentSession(old, now.Add(agentSessionTTL))
	require.Same(t, replacement, st.GetAgent(id))
	require.False(t, mutator.GetMap(ctx).IsInactive(types.UID(info.PodUid)))
	select {
	case <-replacement.done():
		t.Fatal("replacement session was canceled")
	default:
	}
}

func TestAgentSessionExpiryRechecksHeartbeat(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	now := time.Now()
	info := testdata.GetTestAgents(t)["hello"]
	id, err := st.AddAgent(ctx, info, nil, now.Add(-2*agentSessionTTL))
	require.NoError(t, err)
	agent := st.GetAgent(id)
	require.True(t, agent.Mark(now))

	st.expireAgentSession(agent, now.Add(-agentSessionTTL))
	require.Same(t, agent, st.GetAgent(id))
	select {
	case <-agent.done():
		t.Fatal("session with a fresh heartbeat was canceled")
	default:
	}
}

func TestAgentReconnectWaitsForExpiredInterceptReconciliation(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	now := time.Now()
	info := testdata.GetTestAgents(t)["hello"]
	id, err := st.AddAgent(ctx, info, nil, now.Add(-2*agentSessionTTL))
	require.NoError(t, err)
	agent := st.GetAgent(id)
	st.intercepts.Store("intercept", &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id:          "intercept",
		Disposition: rpc.InterceptDispositionType_ACTIVE,
		PodName:     info.PodName,
		PodIp:       info.PodIp,
		Spec: &rpc.InterceptSpec{
			Agent:     info.Name,
			Namespace: info.Namespace,
			Mechanism: "tcp",
		},
	}})

	blocked := make(chan struct{})
	release := make(chan struct{})
	blockerDone := make(chan struct{})
	go func() {
		defer close(blockerDone)
		st.intercepts.Compute("intercept", func(intercept *Intercept, _ bool) (*Intercept, xsync.ComputeOp) {
			close(blocked)
			<-release
			return intercept, xsync.CancelOp
		})
	}()
	<-blocked
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		<-blockerDone
	})

	expired := make(chan struct{})
	go func() {
		defer close(expired)
		st.expireAgentSession(agent, now.Add(-agentSessionTTL))
	}()
	requireClosedAgentSession(t, agent.done())
	restored := make(chan error, 1)
	go func() {
		_, restoreErr := st.RestoreAgent(ctx, id, info, nil, now)
		restored <- restoreErr
	}()
	select {
	case err = <-restored:
		t.Fatalf("agent registration finished before intercept reconciliation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err = <-restored:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("agent did not reconnect after intercept reconciliation")
	}
	<-expired
	intercept, ok := st.intercepts.Load("intercept")
	require.True(t, ok)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, intercept.Disposition)
}

func TestUnrelatedAgentCanRegisterDuringExpiredInterceptReconciliation(t *testing.T) {
	for _, tt := range []struct {
		name     string
		register func(context.Context, *State, *rpc.AgentInfo, time.Time) error
	}{
		{"arrive", func(ctx context.Context, st *State, info *rpc.AgentInfo, now time.Time) error {
			_, err := st.AddAgent(ctx, info, nil, now)
			return err
		}},
		{"bulk restore", func(_ context.Context, st *State, info *rpc.AgentInfo, now time.Time) error {
			st.RestoreAgents([]*rpc.AgentInfo{info}, now)
			return nil
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, st := newServiceInterceptState(t)
			now := time.Now()
			agents := testdata.GetTestAgents(t)
			expiringID, _, release := blockAgentExpiryReconciliation(t, st, agents["hello"], now)
			other := agents["demo1"]
			otherID := tunnel.SessionID(AgentSessionIDPrefix + other.PodUid)

			registered := make(chan error, 1)
			completed := make(chan struct{})
			go func() {
				defer close(completed)
				registered <- tt.register(ctx, st, other, now)
			}()
			t.Cleanup(func() {
				release()
				<-completed
			})
			select {
			case err := <-registered:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("unrelated agent registration waited for another pod's intercept reconciliation")
			}
			require.NotNil(t, st.GetAgent(otherID))
			require.Nil(t, st.GetAgent(expiringID))
		})
	}
}

func TestUnrelatedAgentRegistrationDoesNotReconcileServiceParticipants(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	now := time.Now()
	stale := serviceAgent("example-service-canary", "canary-pod", "10.0.0.1")
	stale.InterceptTargets = nil
	st.RestoreAgents([]*rpc.AgentInfo{stale.AgentInfo}, now)
	staleKey := agentParticipantKey(stale.AgentInfo)
	intercept := &Intercept{
		InterceptInfo: &rpc.InterceptInfo{
			Id:          "client:example-service",
			Spec:        sharedServiceSpec(),
			Disposition: rpc.InterceptDispositionType_WAITING,
		},
		participants: map[string]*interceptParticipant{
			staleKey: {namespace: stale.Namespace, kind: stale.Kind, name: stale.Name},
		},
	}
	intercept.syncServiceWorkloads()
	st.intercepts.Store(intercept.Id, intercept)

	unrelated := testdata.GetTestAgents(t)["demo1"]
	_, err := st.AddAgent(ctx, unrelated, nil, now)
	require.NoError(t, err)
	current, ok := st.intercepts.Load(intercept.Id)
	require.True(t, ok)
	require.Same(t, intercept, current)
	require.Contains(t, current.participants, staleKey)

	current = st.reconcileServiceParticipants(intercept.Id, current)
	require.NotContains(t, current.participants, staleKey)
}

func TestCanceledAgentReconnectDoesNotPublishSession(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	now := time.Now()
	info := testdata.GetTestAgents(t)["hello"]
	id, old, release := blockAgentExpiryReconciliation(t, st, info, now)
	reconnectCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	started := make(chan struct{})
	restored := make(chan error, 1)
	completed := make(chan struct{})
	go func() {
		defer close(completed)
		close(started)
		_, err := st.RestoreAgent(reconnectCtx, id, info, nil, now)
		restored <- err
	}()
	t.Cleanup(func() {
		cancel()
		release()
		<-completed
	})
	<-started
	cancel()
	select {
	case err := <-restored:
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(time.Second):
		t.Fatal("canceled reconnect waited for expired intercept reconciliation")
	}
	require.Nil(t, st.GetAgent(id))

	release()
	require.Nil(t, st.GetAgent(id))
	intercept, ok := st.intercepts.Load("intercept")
	require.True(t, ok)
	require.Equal(t, rpc.InterceptDispositionType_NO_AGENT, intercept.Disposition)

	restoredID, err := st.RestoreAgent(ctx, id, info, nil, now)
	require.NoError(t, err)
	require.Equal(t, id, restoredID)
	replacement := st.GetAgent(id)
	require.NotNil(t, replacement)
	require.NotSame(t, old, replacement)
	intercept, ok = st.intercepts.Load("intercept")
	require.True(t, ok)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, intercept.Disposition)
}

func blockAgentExpiryReconciliation(t *testing.T, st *State, info *rpc.AgentInfo, now time.Time) (tunnel.SessionID, *AgentSession, func()) {
	t.Helper()
	id, err := st.AddAgent(st.backgroundCtx, info, nil, now.Add(-2*agentSessionTTL))
	require.NoError(t, err)
	agent := st.GetAgent(id)
	st.intercepts.Store("intercept", &Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id:          "intercept",
		Disposition: rpc.InterceptDispositionType_ACTIVE,
		PodName:     info.PodName,
		PodIp:       info.PodIp,
		Spec: &rpc.InterceptSpec{
			Agent:     info.Name,
			Namespace: info.Namespace,
			Mechanism: "tcp",
		},
	}})
	unlockIntercept := st.agentInterceptLocks.lock("intercept")

	expired := make(chan struct{})
	go func() {
		defer close(expired)
		st.expireAgentSession(agent, now.Add(-agentSessionTTL))
	}()
	var releaseOnce sync.Once
	unblock := func() {
		releaseOnce.Do(unlockIntercept)
		<-expired
	}
	t.Cleanup(unblock)
	requireClosedAgentSession(t, agent.done())
	return id, agent, unblock
}

func TestRemovedAgentSessionRemainsInactive(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	now := time.Now()
	info := testdata.GetTestAgents(t)["hello"]
	id, err := st.AddAgent(ctx, info, nil, now)
	require.NoError(t, err)
	agent := st.GetAgent(id)

	st.RemoveSession(ctx, id)
	requireClosedAgentSession(t, agent.done())
	require.True(t, mutator.GetMap(ctx).IsInactive(types.UID(info.PodUid)))
	_, err = st.AddAgent(ctx, info, nil, now)
	require.Equal(t, codes.Aborted, status.Code(err))
	_, err = st.RestoreAgent(ctx, id, info, nil, now)
	require.Equal(t, codes.Aborted, status.Code(err))
	st.RestoreAgents([]*rpc.AgentInfo{info}, now)
	require.Zero(t, st.CountAgents())
}

func requireClosedAgentSession(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("agent session was not canceled")
	}
}

func TestInactiveAgentCannotReconnectWithoutExistingSession(t *testing.T) {
	ctx, st := newServiceInterceptState(t)
	info := testdata.GetTestAgents(t)["hello"]
	mutator.GetMap(ctx).Inactivate(types.UID(info.PodUid))
	_, err := st.RestoreAgent(ctx, tunnel.SessionID(AgentSessionIDPrefix+info.PodUid), info, nil, time.Now())
	require.Equal(t, codes.Aborted, status.Code(err))
	require.Zero(t, st.CountAgents())
}
