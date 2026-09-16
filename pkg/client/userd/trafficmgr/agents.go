package trafficmgr

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/grpc/watcher"
	"github.com/telepresenceio/telepresence/v2/pkg/maps"
)

// agentPod is the client's internal record of one traffic-agent pod, holding
// exactly what its consumers read. Built from AgentPodInfo in combined mode
// and projected from AgentInfo in legacy fallback mode.
type agentPod struct {
	workload  string
	namespace string
	podName   string
	podUID    string
	podIP     netip.Addr
	version   string
	nodeAgent bool
}

// agentPodFromPodInfo builds an agentPod from the manager's AgentPodInfo
// projection (combined mode).
func agentPodFromPodInfo(ap *manager.AgentPodInfo) agentPod {
	ip, _ := netip.AddrFromSlice(ap.PodIp)
	return agentPod{
		workload:  ap.WorkloadName,
		namespace: ap.Namespace,
		podName:   ap.PodName,
		podUID:    ap.PodId,
		podIP:     ip.Unmap(),
		version:   ap.Version,
		nodeAgent: ap.NodeAgent,
	}
}

// agentPodFromAgentInfo builds an agentPod from the manager's AgentInfo
// (legacy fallback mode).
func agentPodFromAgentInfo(ai *manager.AgentInfo) agentPod {
	ip, _ := netip.ParseAddr(ai.PodIp)
	return agentPod{
		workload:  ai.Name,
		namespace: ai.Namespace,
		podName:   ai.PodName,
		podUID:    ai.PodUid,
		podIP:     ip.Unmap(),
		version:   ai.Version,
		nodeAgent: ai.NodeAgent,
	}
}

type ingestPodIdentity struct {
	namespace string
	name      string
	uid       string
	ip        netip.Addr
}

func (ap agentPod) identity() ingestPodIdentity {
	return ingestPodIdentity{namespace: ap.namespace, name: ap.podName, uid: ap.podUID, ip: ap.podIP}
}

func agentInfoPodIdentity(ai *manager.AgentInfo) ingestPodIdentity {
	ip, _ := netip.ParseAddr(ai.GetPodIp())
	return ingestPodIdentity{namespace: ai.GetNamespace(), name: ai.GetPodName(), uid: ai.GetPodUid(), ip: ip.Unmap()}
}

const (
	ingestPodConflict = iota - 1
	ingestPodUnknown
	ingestPodName
	ingestPodIP
	ingestPodUID
)

func (p ingestPodIdentity) sameNamespace(other ingestPodIdentity) bool {
	return p.namespace == "" || other.namespace == "" || p.namespace == other.namespace
}

// match ranks comparable identities; a conflict belongs to the same pod name.
func (p ingestPodIdentity) match(other ingestPodIdentity) int {
	if !p.sameNamespace(other) || p.name == "" || p.name != other.name {
		return ingestPodUnknown
	}
	if p.uid != "" && other.uid != "" {
		if p.uid == other.uid {
			return ingestPodUID
		}
		return ingestPodConflict
	}
	if p.ip.IsValid() && other.ip.IsValid() {
		if p.ip == other.ip {
			return ingestPodIP
		}
		return ingestPodConflict
	}
	return ingestPodName
}

// watchAgentsLoop drives the legacy WatchAgentsDelta/WatchAgents watcher, used
// only by watchSessionEventsLegacy. Its snapshots are scoped to the connected
// namespace, so the covered predicate it feeds handleAgentPodSnapshot only
// ever declares that one namespace covered.
func (s *session) watchAgentsLoop(ctx context.Context) error {
	covered := func(namespace string) bool { return namespace == s.Namespace }
	snapMap := make(map[string]*manager.AgentInfo)
	toPods := func() []agentPod {
		pods := make([]agentPod, 0, len(snapMap))
		for _, ai := range snapMap {
			pods = append(pods, agentPodFromAgentInfo(ai))
		}
		return pods
	}
	err := watcher.WatchWithRetry(ctx, "WatchAgentsDelta", client.GetConfig(ctx).Grpc().WatchRetryInterval,
		func(ctx context.Context) (grpc.ServerStreamingClient[manager.AgentInfoDelta], error) {
			return s.ManagerClient().WatchAgentsDelta(ctx, s.SessionInfo())
		},
		func(delta *manager.AgentInfoDelta) error {
			maps.DeltaUpdate(snapMap, delta.Upserts, delta.Removals)
			pods := toPods()
			s.setCurrentAgentPods(pods)
			s.handleAgentPodSnapshot(ctx, pods, covered)
			return nil
		}, func() error {
			clear(snapMap)
			return nil
		})

	if err != nil && status.Code(err) == codes.Unimplemented {
		clog.Warnf(ctx, "WatchAgentsDelta is not implemented by the traffic-manager, falling back to WatchAgents and full snapshots")
		err = watcher.WatchWithRetry(ctx, "WatchAgents", client.GetConfig(ctx).Grpc().WatchRetryInterval,
			func(ctx context.Context) (grpc.ServerStreamingClient[manager.AgentInfoSnapshot], error) {
				return s.ManagerClient().WatchAgents(ctx, s.SessionInfo())
			},
			func(snapshot *manager.AgentInfoSnapshot) error {
				pods := make([]agentPod, len(snapshot.Agents))
				for i, ai := range snapshot.Agents {
					pods[i] = agentPodFromAgentInfo(ai)
				}
				s.setCurrentAgentPods(pods)
				s.handleAgentPodSnapshot(ctx, pods, covered)
				return nil
			}, nil)
	}
	// Handle as if we had an empty snapshot. This will ensure that port forwards and volume mounts are canceled correctly.
	s.setCurrentAgentPods(nil)
	s.handleAgentPodSnapshot(ctx, nil, covered)
	return err
}

// ingestPodDecision is the outcome of matching one ingest's key and current
// physical pod against a fresh agent-pod snapshot.
type ingestPodDecision int

const (
	// ingestPodNoMatch: no pod in the snapshot matches the ingest's workload
	// and namespace. The ingest is left untouched this round; cancelUnwanted
	// decides its fate based on whether its namespace is covered.
	ingestPodNoMatch ingestPodDecision = iota
	// ingestPodKeepAlive: the ingest's current pod is still among the
	// matching pods; its mounts/port-forwards stay alive.
	ingestPodKeepAlive
	// ingestPodReplaced: matching pods exist, but not under the ingest's
	// current physical pod; its agent must be refetched.
	ingestPodReplaced
)

// decideIngestPod matches key (workload+namespace; container matching is not
// needed since membership was already validated when the ingest was
// created) and the ingest's current physical pod against pods, a fresh
// agent-pod snapshot. matching is the set of pods sharing key's workload and
// namespace -- non-empty exactly when the decision isn't ingestPodNoMatch.
func decideIngestPod(pods []agentPod, key ingestKey, current ingestPodIdentity) (decision ingestPodDecision, matching []agentPod) {
	weakMatch, conflict := false, false
	for _, ap := range pods {
		if ap.workload == key.workload && ap.namespace == key.namespace {
			matching = append(matching, ap)
			switch ap.identity().match(current) {
			case ingestPodUID, ingestPodIP:
				decision = ingestPodKeepAlive
			case ingestPodName:
				weakMatch = true
			case ingestPodConflict:
				conflict = true
			}
		}
	}
	if len(matching) == 0 {
		return ingestPodNoMatch, nil
	}
	if decision == ingestPodKeepAlive || weakMatch && !conflict {
		return ingestPodKeepAlive, matching
	}
	return ingestPodReplaced, matching
}

// selectReplacementAgent picks the AgentInfo to adopt for a replaced ingest
// pod. An unlisted candidate is usable only when the watch supplies no
// physical identifiers; a known conflicting generation is never adopted.
func selectReplacementAgent(candidates []*manager.AgentInfo, matching []agentPod) *manager.AgentInfo {
	var best, unlisted *manager.AgentInfo
	bestMatch := ingestPodUnknown
	snapshotHasPhysicalIdentity := false
	for _, ap := range matching {
		if ap.podUID != "" || ap.podIP.IsValid() {
			snapshotHasPhysicalIdentity = true
			break
		}
	}
	for _, cand := range candidates {
		candidate := agentInfoPodIdentity(cand)
		listed, sameNamespace := false, false
		for _, ap := range matching {
			pod := ap.identity()
			if !pod.sameNamespace(candidate) {
				continue
			}
			sameNamespace = true
			match := pod.match(candidate)
			if match != ingestPodUnknown {
				listed = true
			}
			if match > bestMatch {
				best, bestMatch = cand, match
			}
		}
		if unlisted == nil && !listed && sameNamespace && candidate.name != "" {
			unlisted = cand
		}
	}
	if best != nil {
		return best
	}
	if snapshotHasPhysicalIdentity {
		return nil
	}
	return unlisted
}

func (s *session) watchedIngestPods(key ingestKey) []agentPod {
	_, pods := decideIngestPod(s.getCurrentAgentPods(), key, ingestPodIdentity{})
	return pods
}

func (s *session) refetchIngestAgent(ctx context.Context, ig *ingest, nodeAgent bool) (*manager.AgentInfo, error) {
	ctx, cancel := client.GetConfig(ctx).Timeouts().TimeoutContext(ctx, client.TimeoutTrafficManagerAPI)
	stop := context.AfterFunc(ig.ctx, cancel)
	defer func() { stop(); cancel() }()
	rq := &manager.EnsureAgentRequest{
		Session: s.sessionInfo, Name: ig.workload, Namespace: ig.namespace, NodeAgent: nodeAgent,
	}
	backoff := 100 * time.Millisecond
	for {
		if len(s.watchedIngestPods(ig.ingestKey)) == 0 {
			return nil, nil
		}
		as, err := s.ManagerClient().EnsureAgent(ctx, rq)
		if err != nil {
			return nil, err
		}
		pods := s.watchedIngestPods(ig.ingestKey)
		if len(pods) == 0 {
			return nil, nil
		}
		if ai := selectReplacementAgent(as.Agents, pods); ai != nil {
			if _, ok := ai.Containers[ig.container]; !ok {
				return nil, fmt.Errorf("workload %s has no container named %s after replacement", ig.workload, ig.container)
			}
			if err = s.translateContainerEnv(ctx, ai, ig.container); err != nil {
				return nil, fmt.Errorf("failed to translate container env: %w", err)
			}
			pods = s.watchedIngestPods(ig.ingestKey)
			if len(pods) == 0 {
				return nil, nil
			}
			if selectReplacementAgent([]*manager.AgentInfo{ai}, pods) == ai {
				return ai, nil
			}
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		backoff = min(2*backoff, time.Second)
	}
}

func (s *session) keepIngestPodAccess(key ingestKey) {
	tracker := s.ingestTracker
	tracker.Lock()
	defer tracker.Unlock()
	for podKey, pod := range tracker.alivePods {
		if pod.workload == key.workload && podKey.namespace == key.namespace && podKey.container == key.container {
			tracker.snapshot[podKey] = struct{}{}
		}
	}
}

// handleAgentPodSnapshot reconciles ingest pod access within covered namespaces.
func (s *session) handleAgentPodSnapshot(ctx context.Context, pods []agentPod, covered func(namespace string) bool) {
	s.ingestTracker.initSnapshot()

	s.currentIngests.Range(func(key ingestKey, ig *ingest) bool {
		if ig.ctx.Err() != nil {
			return true
		}
		currentAgent := ig.getAgentInfo()
		decision, _ := decideIngestPod(pods, key, agentInfoPodIdentity(currentAgent))
		switch decision {
		case ingestPodNoMatch:
			// Not covered by this snapshot; cancelUnwanted below decides its fate.
			return true
		case ingestPodKeepAlive:
			s.startIngestPodAccess(ctx, ig, false)
			return true
		}

		ai, err := s.refetchIngestAgent(ctx, ig, currentAgent.GetNodeAgent())
		if err != nil {
			clog.Errorf(ctx, "failed to refetch agent for ingest %s: %v", key, err)
			if ig.ctx.Err() == nil {
				s.keepIngestPodAccess(key)
			}
			return true
		}
		if ai == nil || ig.ctx.Err() != nil {
			return true
		}
		ig.setAgentInfo(ai)
		s.startIngestPodAccess(ctx, ig, false)
		return true
	})
	s.ingestTracker.cancelUnwanted(ctx, covered)
}

func (s *session) getCurrentAgentPods() []agentPod {
	s.currentInterceptsLock.Lock()
	pods := s.currentAgentPods
	s.currentInterceptsLock.Unlock()
	return pods
}

func (s *session) setCurrentAgentPods(pods []agentPod) {
	s.currentInterceptsLock.Lock()
	s.currentAgentPods = pods
	s.currentInterceptsLock.Unlock()
}
