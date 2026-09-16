package trafficmgr

import (
	"context"
	"errors"
	"io"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	rootdRpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
)

func rollingIngestAgent(name, uid, ip string) *manager.AgentInfo {
	return &manager.AgentInfo{
		Name: "echo", Namespace: "default", PodName: name, PodUid: uid, PodIp: ip,
		Containers: map[string]*manager.AgentInfo_ContainerInfo{"app": {}},
	}
}

func rollingIngestPod(name, uid, ip string) *manager.AgentPodInfo {
	return &manager.AgentPodInfo{
		WorkloadName: "echo", Namespace: "default", PodName: name, PodId: uid,
		PodIp: netip.MustParseAddr(ip).AsSlice(),
	}
}

func addRollingIngest(s *session, ai *manager.AgentInfo) *ingest {
	ig := &ingest{
		AgentInfo: ai, ingestKey: ingestKey{workload: "echo", namespace: "default", container: "app"}, ctx: s,
	}
	s.currentIngests.Store(ig.ingestKey, ig)
	return ig
}

func withRollingIngestReadyRoot(t *testing.T, s *session) {
	t.Helper()
	root := &rollingIngestWatchRoot{waiting: make(chan string, 32), release: make(chan struct{})}
	close(root.release)
	conn, cleanup, err := dialTestRootDaemon(root)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	s.setRootDaemon(rootdRpc.NewDaemonClient(conn), conn, nil, false)
}

func TestRollingIngestAdoptsSameNameReplacement(t *testing.T) {
	tests := []struct {
		name           string
		oldUID, newUID string
		oldIP, newIP   string
		project        func(string, string) agentPod
	}{
		{
			name: "combined pod watch", oldUID: "old-uid", newUID: "new-uid", oldIP: "10.0.0.9", newIP: "10.0.0.2",
			project: func(uid, ip string) agentPod { return agentPodFromPodInfo(rollingIngestPod("echo-0", uid, ip)) },
		},
		{
			name: "legacy agent watch", oldUID: "old-uid", newUID: "new-uid", oldIP: "10.0.0.9", newIP: "10.0.0.2",
			project: func(uid, ip string) agentPod { return agentPodFromAgentInfo(rollingIngestAgent("echo-0", uid, ip)) },
		},
		{
			name: "old manager without UIDs uses changed IP", oldIP: "10.0.0.9", newIP: "10.0.0.2",
			project: func(uid, ip string) agentPod { return agentPodFromPodInfo(rollingIngestPod("echo-0", uid, ip)) },
		},
		{
			name: "UID absent only from watch uses changed IP", oldUID: "old-uid", newUID: "new-uid", oldIP: "10.0.0.9", newIP: "10.0.0.2",
			project: func(_ string, ip string) agentPod { return agentPodFromPodInfo(rollingIngestPod("echo-0", "", ip)) },
		},
		{
			name: "changed UID replaces even with the same IP", oldUID: "old-uid", newUID: "new-uid", oldIP: "10.0.0.9", newIP: "10.0.0.9",
			project: func(uid, ip string) agentPod { return agentPodFromPodInfo(rollingIngestPod("echo-0", uid, ip)) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldAgent := rollingIngestAgent("echo-0", tt.oldUID, tt.oldIP)
			newAgent := rollingIngestAgent("echo-0", tt.newUID, tt.newIP)
			fake := &fakeManagerServer{ensureAgentResponse: &manager.AgentInfoSnapshot{Agents: []*manager.AgentInfo{oldAgent, newAgent}}}
			s := newIngestTestSession(t, dialTestManager(t, fake))
			withRollingIngestReadyRoot(t, s)
			ig := addRollingIngest(s, oldAgent)
			pods := []agentPod{tt.project(tt.newUID, tt.newIP)}
			s.setCurrentAgentPods(pods)
			s.handleAgentPodSnapshot(s, pods, s.coveredCombined)
			assert.Equal(t, tt.newUID, ig.PodUid)
			assert.Equal(t, tt.newIP, ig.PodIp)
			assert.Len(t, fake.ensureAgentCalls(), 1)
		})
	}
}

func TestRollingIngestStaleSessionRemovalKeepsActiveGeneration(t *testing.T) {
	active := rollingIngestAgent("echo-0", "active-uid", "10.0.0.2")
	fake := &fakeManagerServer{}
	s := newIngestTestSession(t, dialTestManager(t, fake))
	withRollingIngestReadyRoot(t, s)
	ig := addRollingIngest(s, active)
	podMap := make(map[string]*manager.AgentPodInfo)
	snapshot := applyAgentPodsDelta(podMap, &manager.AgentPodInfoDelta{
		Upserts: map[string]*manager.AgentPodInfo{
			"stale-session":  rollingIngestPod("echo-0", "stale-uid", "10.0.0.9"),
			"active-session": rollingIngestPod("echo-0", "active-uid", "10.0.0.2"),
		},
	})
	s.setCurrentAgentPods(snapshot)
	s.handleAgentPodSnapshot(s, snapshot, s.coveredCombined)
	snapshot = applyAgentPodsDelta(podMap, &manager.AgentPodInfoDelta{Removals: []string{"stale-session"}})
	s.setCurrentAgentPods(snapshot)
	s.handleAgentPodSnapshot(s, snapshot, s.coveredCombined)
	assert.Equal(t, "active-uid", ig.PodUid)
	assert.Equal(t, "10.0.0.2", ig.PodIp)
	assert.Empty(t, fake.ensureAgentCalls())
}

func TestRollingIngestReplacementPrefersComparablePhysicalIdentity(t *testing.T) {
	matching := []agentPod{agentPodFromPodInfo(rollingIngestPod("echo-0", "active-uid", "10.0.0.2"))}
	unknown := rollingIngestAgent("echo-0", "", "")
	stale := rollingIngestAgent("echo-0", "stale-uid", "10.0.0.9")
	active := rollingIngestAgent("echo-0", "active-uid", "10.0.0.2")
	assert.Same(t, active, selectReplacementAgent([]*manager.AgentInfo{unknown, stale, active}, matching))
	assert.Nil(t, selectReplacementAgent([]*manager.AgentInfo{stale}, matching))
	assert.Same(t, unknown, selectReplacementAgent([]*manager.AgentInfo{unknown}, matching))
	mapped := rollingIngestAgent("echo-0", "", "::ffff:10.0.0.2")
	assert.Same(t, mapped, selectReplacementAgent([]*manager.AgentInfo{unknown, mapped}, matching))
	wrongNamespace := rollingIngestAgent("echo-0", "active-uid", "10.0.0.2")
	wrongNamespace.Namespace = "other"
	assert.Nil(t, selectReplacementAgent([]*manager.AgentInfo{wrongNamespace}, matching))
	unlisted := rollingIngestAgent("echo-1", "another-uid", "10.0.0.3")
	assert.Nil(t, selectReplacementAgent([]*manager.AgentInfo{unlisted}, matching))
}

func TestRollingIngestIdentityFallbackDoesNotMaskAKnownConflict(t *testing.T) {
	key := ingestKey{workload: "echo", namespace: "default", container: "app"}
	current := agentInfoPodIdentity(rollingIngestAgent("echo-0", "old-uid", "10.0.0.9"))
	unknown := agentPodFromPodInfo(&manager.AgentPodInfo{WorkloadName: "echo", Namespace: "default", PodName: "echo-0"})
	newPod := agentPodFromPodInfo(rollingIngestPod("echo-0", "new-uid", "10.0.0.2"))
	decision, _ := decideIngestPod([]agentPod{unknown}, key, current)
	assert.Equal(t, ingestPodKeepAlive, decision)
	decision, _ = decideIngestPod([]agentPod{unknown, newPod}, key, current)
	assert.Equal(t, ingestPodReplaced, decision)
	sameUIDChangedIP := agentPodFromPodInfo(rollingIngestPod("echo-0", "old-uid", "10.0.0.8"))
	decision, _ = decideIngestPod([]agentPod{unknown, newPod, sameUIDChangedIP}, key, current)
	assert.Equal(t, ingestPodKeepAlive, decision)
	mapped := agentInfoPodIdentity(rollingIngestAgent("echo-0", "", "::ffff:10.0.0.2"))
	decision, _ = decideIngestPod([]agentPod{newPod}, key, mapped)
	assert.Equal(t, ingestPodKeepAlive, decision)
}

func TestRollingIngestReadersSeeACompleteAgentGeneration(t *testing.T) {
	oldAgent := rollingIngestAgent("echo-0", "old-uid", "10.0.0.9")
	newAgent := rollingIngestAgent("echo-0", "new-uid", "10.0.0.2")
	ig := &ingest{AgentInfo: oldAgent, ingestKey: ingestKey{workload: "echo", namespace: "default", container: "app"}}
	start := make(chan struct{})
	bad := make(chan string, 1)
	var workers sync.WaitGroup
	for range 3 {
		workers.Go(func() {
			<-start
			for range 150 {
				ai := ig.getAgentInfo()
				if ai.PodUid != "old-uid" && ai.PodUid != "new-uid" || ai.PodUid == "old-uid" && ai.PodIp != "10.0.0.9" || ai.PodUid == "new-uid" && ai.PodIp != "10.0.0.2" {
					select {
					case bad <- "agent identity did not belong to one generation":
					default:
					}
				}
				if response := ig.response(); response.PodIp != "10.0.0.9" && response.PodIp != "10.0.0.2" {
					select {
					case bad <- "response did not use a published generation":
					default:
					}
				}
			}
		})
	}
	workers.Go(func() {
		<-start
		for range 150 {
			ig.setAgentInfo(newAgent)
			ig.setAgentInfo(oldAgent)
		}
	})
	close(start)
	workers.Wait()
	select {
	case message := <-bad:
		t.Error(message)
	default:
	}
}

type rollingIngestWatchManager struct {
	manager.UnimplementedManagerServer
	deltas          chan *manager.SessionEventsDelta
	mu              sync.Mutex
	ensureAgents    []*manager.AgentInfo
	ensureResponses [][]*manager.AgentInfo
	ensureCalled    chan struct{}
}

func (f *rollingIngestWatchManager) EnsureAgent(context.Context, *manager.EnsureAgentRequest) (*manager.AgentInfoSnapshot, error) {
	f.mu.Lock()
	agents := f.ensureAgents
	if len(f.ensureResponses) > 0 {
		agents = f.ensureResponses[0]
		if len(f.ensureResponses) > 1 {
			f.ensureResponses = f.ensureResponses[1:]
		}
	}
	f.mu.Unlock()
	if f.ensureCalled != nil {
		f.ensureCalled <- struct{}{}
	}
	return &manager.AgentInfoSnapshot{Agents: agents}, nil
}

func (f *rollingIngestWatchManager) WatchSessionEvents(_ *manager.SessionEventsRequest, stream grpc.ServerStreamingServer[manager.SessionEventsDelta]) error {
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case delta := <-f.deltas:
			if err := stream.Send(delta); err != nil {
				return err
			}
		}
	}
}

type rollingIngestWatchRoot struct {
	rootdRpc.UnimplementedDaemonServer
	relayed chan *rootdRpc.AgentPodsDelta
	waiting chan string
	release chan struct{}
}

func (f *rollingIngestWatchRoot) TranslateEnvIPs(_ context.Context, env *rootdRpc.Environment) (*rootdRpc.Environment, error) {
	return env, nil
}

func (f *rollingIngestWatchRoot) WatchAgentPods(stream grpc.ClientStreamingServer[rootdRpc.AgentPodsDelta, emptypb.Empty]) error {
	for {
		delta, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return stream.SendAndClose(&emptypb.Empty{})
			}
			return err
		}
		select {
		case f.relayed <- delta:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (f *rollingIngestWatchRoot) WaitForAgentIP(ctx context.Context, request *rootdRpc.WaitForAgentIPRequest) (*rootdRpc.WaitForAgentIPResponse, error) {
	ip, _ := netip.AddrFromSlice(request.Ip)
	select {
	case f.waiting <- ip.String():
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-f.release:
		return &rootdRpc.WaitForAgentIPResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestCombinedWatchRelaysFollowingPodsWhileIngestReadinessIsBlocked(t *testing.T) {
	ctx, cancel := context.WithCancel(client.WithConfig(context.Background(), client.GetDefaultConfig()))
	defer cancel()
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	fakeManager := &rollingIngestWatchManager{deltas: make(chan *manager.SessionEventsDelta, 3)}
	s := newIngestTestSession(t, dialTestManager(t, fakeManager))
	s.Context = ctx
	s.podRelay = newPodRelay()
	s.agentPodWatchNamespacesValue = []string{"default"}
	s.agentPodWatchNamespacesOnce.Do(func() {})
	addRollingIngest(s, rollingIngestAgent("echo-0", "existing-uid", "10.0.0.9"))
	fakeRoot := &rollingIngestWatchRoot{
		relayed: make(chan *rootdRpc.AgentPodsDelta, 4), waiting: make(chan string, 4), release: make(chan struct{}),
	}
	rootConn, rootCleanup, err := dialTestRootDaemon(fakeRoot)
	require.NoError(t, err)
	t.Cleanup(rootCleanup)
	s.setRootDaemon(rootdRpc.NewDaemonClient(rootConn), rootConn, nil, false)
	relayDone := make(chan struct{})
	watchDone := make(chan struct{})
	go func() { defer close(relayDone); _ = s.podRelay.run(ctx, s) }()
	go func() { defer close(watchDone); _ = s.watchSessionEvents(watchCtx) }()
	t.Cleanup(func() {
		cancelWatch()
		cancel()
		select {
		case <-watchDone:
		case <-time.After(3 * time.Second):
			t.Error("combined watcher did not stop")
		}
		select {
		case <-relayDone:
		case <-time.After(3 * time.Second):
			t.Error("pod relay did not stop")
		}
	})
	fakeManager.deltas <- &manager.SessionEventsDelta{AgentPods: &manager.AgentPodInfoDelta{
		Upserts: map[string]*manager.AgentPodInfo{"existing": rollingIngestPod("echo-0", "existing-uid", "10.0.0.9")},
	}}
	select {
	case ip := <-fakeRoot.waiting:
		require.Equal(t, "10.0.0.9", ip)
	case <-time.After(3 * time.Second):
		t.Fatal("ingest did not start its root readiness wait")
	}
	fakeManager.deltas <- &manager.SessionEventsDelta{AgentPods: &manager.AgentPodInfoDelta{
		Upserts: map[string]*manager.AgentPodInfo{"unrelated": rollingIngestPod("echo-1", "unrelated-uid", "10.0.0.2")},
	}, Intercepts: &manager.InterceptInfoDelta{Upserts: map[string]*manager.InterceptInfo{
		"waiting-intercept": {Id: "waiting-intercept", Spec: &manager.InterceptSpec{Name: "waiting-intercept"}, Disposition: manager.InterceptDispositionType_WAITING},
	}}}
	seen := make(map[string]bool)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
receive:
	for len(seen) < 2 {
		select {
		case delta := <-fakeRoot.relayed:
			for key := range delta.Upserts {
				seen[key] = true
			}
		case <-timer.C:
			break receive
		}
	}
	assert.True(t, seen["existing"], "the initial pod must reach root while its own ingest is waiting")
	assert.True(t, seen["unrelated"], "a subsequent unrelated pod must reach root before ingest readiness is released")
	assert.Eventually(t, func() bool {
		for _, ap := range s.getCurrentAgentPods() {
			if ap.podName == "echo-1" {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "the pod cache must stay current while ingest readiness is blocked")
	assert.Eventually(t, func() bool {
		s.currentInterceptsLock.Lock()
		defer s.currentInterceptsLock.Unlock()
		return s.currentIntercepts["waiting-intercept"] != nil
	}, 2*time.Second, 10*time.Millisecond, "the unrelated intercept must be consumed while ingest readiness remains blocked")
	cancelWatch()
	select {
	case <-watchDone:
	case <-time.After(2 * time.Second):
		t.Error("the watcher must cancel blocked ingest readiness even while the session stays active")
	}
	require.NoError(t, ctx.Err())
	assert.Empty(t, s.getCurrentAgentPods(), "cleanup must run after ingest consumption stops")
	close(fakeRoot.release)
}

func TestCombinedWatchSingleIngestFollowsSameNamePodGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(client.WithConfig(context.Background(), client.GetDefaultConfig()))
	defer cancel()
	oldAgent := rollingIngestAgent("echo-0", "old-uid", "10.0.0.9")
	newAgent := rollingIngestAgent("echo-0", "new-uid", "10.0.0.2")
	fakeManager := &rollingIngestWatchManager{
		deltas: make(chan *manager.SessionEventsDelta, 4), ensureAgents: []*manager.AgentInfo{oldAgent, newAgent},
	}
	s := newIngestTestSession(t, dialTestManager(t, fakeManager))
	s.Context = ctx
	s.podRelay = newPodRelay()
	s.agentPodWatchNamespacesValue = []string{"default"}
	s.agentPodWatchNamespacesOnce.Do(func() {})
	ig := addRollingIngest(s, oldAgent)
	fakeRoot := &rollingIngestWatchRoot{waiting: make(chan string, 4), release: make(chan struct{})}
	close(fakeRoot.release)
	rootConn, rootCleanup, err := dialTestRootDaemon(fakeRoot)
	require.NoError(t, err)
	t.Cleanup(rootCleanup)
	s.setRootDaemon(rootdRpc.NewDaemonClient(rootConn), rootConn, nil, false)
	watchDone := make(chan struct{})
	go func() { defer close(watchDone); _ = s.watchSessionEvents(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-watchDone:
		case <-time.After(3 * time.Second):
			t.Error("combined watcher did not stop")
		}
	})
	oldPod := rollingIngestPod("echo-0", "old-uid", "10.0.0.9")
	newPod := rollingIngestPod("echo-0", "new-uid", "10.0.0.2")
	fakeManager.deltas <- &manager.SessionEventsDelta{AgentPods: &manager.AgentPodInfoDelta{
		Upserts: map[string]*manager.AgentPodInfo{"old-session": oldPod},
	}}
	select {
	case ip := <-fakeRoot.waiting:
		require.Equal(t, oldAgent.PodIp, ip)
	case <-time.After(3 * time.Second):
		t.Fatal("initial ingest pod was not processed")
	}
	fakeManager.deltas <- &manager.SessionEventsDelta{AgentPods: &manager.AgentPodInfoDelta{
		Upserts: map[string]*manager.AgentPodInfo{"new-session": newPod}, Removals: []string{"old-session"},
	}}
	select {
	case ip := <-fakeRoot.waiting:
		require.Equal(t, newAgent.PodIp, ip)
	case <-time.After(3 * time.Second):
		t.Fatal("ingest did not transfer root access to the same-named replacement pod")
	}
	assert.Equal(t, "new-uid", ig.getAgentInfo().PodUid)
	fakeManager.deltas <- &manager.SessionEventsDelta{AgentPods: &manager.AgentPodInfoDelta{
		Upserts: map[string]*manager.AgentPodInfo{"stale-session": oldPod},
	}}
	fakeManager.deltas <- &manager.SessionEventsDelta{AgentPods: &manager.AgentPodInfoDelta{Removals: []string{"stale-session"}}}
	assert.Eventually(t, func() bool {
		pods := s.getCurrentAgentPods()
		return len(pods) == 1 && pods[0].podUID == "new-uid" && ig.getAgentInfo().PodUid == "new-uid"
	}, time.Second, 10*time.Millisecond)
}

func addRollingTrackedPod(s *session) (podAccessKey, <-chan struct{}) {
	key := podAccessKey{namespace: "default", container: "app", podIP: "10.0.0.9"}
	deleted := make(chan struct{})
	s.ingestTracker.Lock()
	s.ingestTracker.alivePods[key] = &podAccessSync{workload: "echo", cancelPod: func() { close(deleted) }}
	s.ingestTracker.Unlock()
	return key, deleted
}

func rollingPodTracked(s *session, key podAccessKey) bool {
	s.ingestTracker.Lock()
	defer s.ingestTracker.Unlock()
	return s.ingestTracker.alivePods[key] != nil
}

func TestCombinedWatchRetriesStaleIngestAgentWithoutAnotherPodDelta(t *testing.T) {
	ctx, cancel := context.WithCancel(client.WithConfig(context.Background(), client.GetDefaultConfig()))
	defer cancel()
	oldAgent := rollingIngestAgent("echo-0", "old-uid", "10.0.0.9")
	newAgent := rollingIngestAgent("echo-0", "new-uid", "10.0.0.2")
	fakeManager := &rollingIngestWatchManager{
		deltas: make(chan *manager.SessionEventsDelta, 2), ensureResponses: [][]*manager.AgentInfo{{oldAgent}, {newAgent}},
		ensureCalled: make(chan struct{}, 16),
	}
	s := newIngestTestSession(t, dialTestManager(t, fakeManager))
	s.Context = ctx
	s.podRelay = newPodRelay()
	s.agentPodWatchNamespacesValue = []string{"default"}
	s.agentPodWatchNamespacesOnce.Do(func() {})
	ig := addRollingIngest(s, oldAgent)
	key, oldRemoved := addRollingTrackedPod(s)
	withRollingIngestReadyRoot(t, s)
	watchDone := make(chan struct{})
	go func() { defer close(watchDone); _ = s.watchSessionEvents(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-watchDone:
		case <-time.After(3 * time.Second):
			t.Error("combined watcher did not stop")
		}
	})
	fakeManager.deltas <- &manager.SessionEventsDelta{AgentPods: &manager.AgentPodInfoDelta{
		Upserts: map[string]*manager.AgentPodInfo{"new-session": rollingIngestPod("echo-0", "new-uid", "10.0.0.2")},
	}}
	select {
	case <-fakeManager.ensureCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("manager did not receive the first replacement request")
	}
	assert.True(t, rollingPodTracked(s, key), "old pod access must remain tracked while the manager still reports only the stale pod")
	assert.Eventually(t, func() bool { return ig.getAgentInfo().PodUid == "new-uid" }, 2*time.Second, 10*time.Millisecond,
		"the existing ingest must retry and adopt the replacement without receiving another watch delta")
	select {
	case <-fakeManager.ensureCalled:
	case <-time.After(2 * time.Second):
		t.Error("manager was never asked again after its stale-only reply")
	}
	select {
	case <-oldRemoved:
	case <-time.After(2 * time.Second):
		t.Error("old access must be removed once the watched replacement is adopted")
	}
}

func TestRollingIngestAllStaleRefetchCancellationPreservesOldAccessUntilCleanup(t *testing.T) {
	oldAgent := rollingIngestAgent("echo-0", "old-uid", "10.0.0.9")
	fakeManager := &rollingIngestWatchManager{ensureResponses: [][]*manager.AgentInfo{{oldAgent}}, ensureCalled: make(chan struct{}, 16)}
	s := newIngestTestSession(t, dialTestManager(t, fakeManager))
	withRollingIngestReadyRoot(t, s)
	ig := addRollingIngest(s, oldAgent)
	key, oldRemoved := addRollingTrackedPod(s)
	pods := []agentPod{agentPodFromPodInfo(rollingIngestPod("echo-0", "new-uid", "10.0.0.2"))}
	s.setCurrentAgentPods(pods)
	ctx, cancel := context.WithCancel(s)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); s.handleAgentPodSnapshot(ctx, pods, s.coveredCombined) }()
	select {
	case <-fakeManager.ensureCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("manager did not receive the first replacement request")
	}
	assert.True(t, rollingPodTracked(s, key), "old access must remain tracked while only a stale agent is returned")
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the refetch did not stop after caller cancellation")
	}
	assert.Equal(t, "old-uid", ig.getAgentInfo().PodUid, "a known stale physical pod must never be newly adopted")
	assert.True(t, rollingPodTracked(s, key), "an interrupted refetch must not discard the existing tracked access")
	select {
	case <-oldRemoved:
		t.Error("old access was removed before replacement or final cleanup")
	default:
	}
	s.setCurrentAgentPods(nil)
	s.handleAgentPodSnapshot(s, nil, s.coveredCombined)
	select {
	case <-oldRemoved:
	case <-time.After(2 * time.Second):
		t.Error("final watcher cleanup must release old tracked access")
	}
}

func TestRollingIngestRetryUsesTheLatestWatchedGeneration(t *testing.T) {
	oldAgent := rollingIngestAgent("echo-0", "old-uid", "10.0.0.9")
	firstAgent := rollingIngestAgent("echo-0", "first-uid", "10.0.0.2")
	latestAgent := rollingIngestAgent("echo-0", "latest-uid", "10.0.0.4")
	fakeManager := &rollingIngestWatchManager{
		ensureResponses: [][]*manager.AgentInfo{{oldAgent}, {firstAgent}, {latestAgent}}, ensureCalled: make(chan struct{}, 16),
	}
	s := newIngestTestSession(t, dialTestManager(t, fakeManager))
	withRollingIngestReadyRoot(t, s)
	ig := addRollingIngest(s, oldAgent)
	firstPods := []agentPod{agentPodFromPodInfo(rollingIngestPod("echo-0", "first-uid", "10.0.0.2"))}
	s.setCurrentAgentPods(firstPods)
	done := make(chan struct{})
	go func() { defer close(done); s.handleAgentPodSnapshot(s, firstPods, s.coveredCombined) }()
	select {
	case <-fakeManager.ensureCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("manager did not receive the first replacement request")
	}
	s.setCurrentAgentPods([]agentPod{agentPodFromPodInfo(rollingIngestPod("echo-0", "latest-uid", "10.0.0.4"))})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the refetch did not adopt the latest generation")
	}
	assert.Equal(t, "latest-uid", ig.getAgentInfo().PodUid)
	for range 2 {
		select {
		case <-fakeManager.ensureCalled:
		default:
			t.Error("the manager reply for the superseded watch generation should have been rejected")
		}
	}
}

func TestRollingIngestAllStaleStopsAtTheConfiguredManagerAPIDeadline(t *testing.T) {
	oldAgent := rollingIngestAgent("echo-0", "old-uid", "10.0.0.9")
	fakeManager := &rollingIngestWatchManager{ensureResponses: [][]*manager.AgentInfo{{oldAgent}}, ensureCalled: make(chan struct{}, 16)}
	s := newIngestTestSession(t, dialTestManager(t, fakeManager))
	config := client.GetDefaultConfig()
	config.Timeouts().PrivateTrafficManagerAPI = 350 * time.Millisecond
	s.Context = client.WithConfig(context.Background(), config)
	withRollingIngestReadyRoot(t, s)
	ig := addRollingIngest(s, oldAgent)
	key, _ := addRollingTrackedPod(s)
	pods := []agentPod{agentPodFromPodInfo(rollingIngestPod("echo-0", "new-uid", "10.0.0.2"))}
	s.setCurrentAgentPods(pods)
	done := make(chan struct{})
	go func() { defer close(done); s.handleAgentPodSnapshot(s, pods, s.coveredCombined) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the refetch did not stop at its configured manager API deadline")
	}
	assert.Equal(t, "old-uid", ig.getAgentInfo().PodUid)
	assert.True(t, rollingPodTracked(s, key), "a manager API deadline must not remove tracked access before a new generation adopts it")
	assert.GreaterOrEqual(t, len(fakeManager.ensureCalled), 2, "a stale reply should trigger a paced retry")
	assert.LessOrEqual(t, len(fakeManager.ensureCalled), 5, "the bounded retry should avoid a tight RPC loop")
	s.setCurrentAgentPods(nil)
	s.handleAgentPodSnapshot(s, nil, s.coveredCombined)
}
