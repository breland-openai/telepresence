package trafficmgr

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/agentpf"
	"github.com/telepresenceio/telepresence/v2/pkg/grpc/watcher"
	"github.com/telepresenceio/telepresence/v2/pkg/log"
	"github.com/telepresenceio/telepresence/v2/pkg/maps"
)

// sessionEventsHandler drives the combined WatchSessionEvents watcher for as
// long as the session lives, degrading to watchSessionEventsLegacy the first
// time the traffic-manager reports it doesn't support the combined stream.
func (s *session) sessionEventsHandler(ctx context.Context) error {
	return runWithRetry(ctx, s.watchSessionEventsLoop)
}

func (s *session) watchSessionEventsLoop(ctx context.Context) error {
	err := s.watchSessionEvents(ctx)
	if err != nil && status.Code(err) == codes.Unimplemented {
		clog.Warnf(ctx, "WatchSessionEvents is not implemented by the traffic-manager, falling back to separate watchers")
		return s.watchSessionEventsLegacy(ctx)
	}
	return err
}

// coveredCombined reports whether namespace is one the combined stream keeps
// agent-pod state current for -- the namespaces requested in
// SessionEventsRequest plus the connected namespace.
func (s *session) coveredCombined(namespace string) bool {
	if namespace == s.Namespace {
		return true
	}
	for _, ns := range s.agentPodWatchNamespaces() {
		if ns == namespace {
			return true
		}
	}
	return false
}

// applyAgentPodsDelta folds delta into podMap and returns the resulting
// snapshot as []agentPod. Pulled out of watchSessionEvents so the
// accumulation step is exercisable without a stream.
func applyAgentPodsDelta(podMap map[string]*manager.AgentPodInfo, delta *manager.AgentPodInfoDelta) []agentPod {
	maps.DeltaUpdate(podMap, delta.Upserts, delta.Removals)
	pods := make([]agentPod, 0, len(podMap))
	for _, ap := range podMap {
		pods = append(pods, agentPodFromPodInfo(ap))
	}
	return pods
}

// applyInterceptsDelta folds delta into icMap and returns the resulting
// intercept snapshot. Pulled out of watchSessionEvents for the same reason as
// applyAgentPodsDelta.
func applyInterceptsDelta(icMap map[string]*manager.InterceptInfo, delta *manager.InterceptInfoDelta) []*manager.InterceptInfo {
	maps.DeltaUpdate(icMap, delta.Upserts, delta.Removals)
	return maps.Values(icMap)
}

// The relay and pod cache are published on receipt. Ingest and intercept
// reconciliation consume independent, coalesced full snapshots because both
// can wait for root-daemon readiness supplied by a later delta on this stream.
func (s *session) watchSessionEvents(ctx context.Context) error {
	pat := newPodAccessTracker()
	podMap := make(map[string]*manager.AgentPodInfo)
	icMap := make(map[string]*manager.InterceptInfo)
	relayEnabled := len(s.agentPodWatchNamespaces()) > 0
	everReceived := false

	ingestCtx, cancelIngest := context.WithCancel(ctx)
	defer cancelIngest()
	podSnapshots := make(chan []agentPod, 1)
	podDone := make(chan struct{})
	icSnapshots := make(chan []*manager.InterceptInfo, 1)
	icDone := make(chan struct{})
	var managerGeneration uint64
	go func() {
		defer close(podDone)
		for snap := range podSnapshots {
			s.handleAgentPodSnapshot(ingestCtx, snap, s.coveredCombined)
		}
	}()
	go func() {
		defer close(icDone)
		for snap := range icSnapshots {
			s.handleInterceptSnapshot(pat, snap)
		}
	}()

	err := watcher.WatchWithRetry(ctx, "WatchSessionEvents", client.GetConfig(ctx).Grpc().WatchRetryInterval,
		func(ctx context.Context) (grpc.ServerStreamingClient[manager.SessionEventsDelta], error) {
			mClient, generation := s.managerClient()
			managerGeneration = generation
			return mClient.WatchSessionEvents(ctx, &manager.SessionEventsRequest{
				Session:    s.SessionInfo(),
				Namespaces: s.agentPodWatchNamespaces(),
			})
		},
		func(delta *manager.SessionEventsDelta) error {
			everReceived = true
			if ad := delta.AgentPods; ad != nil {
				if relayEnabled {
					s.podRelay.apply(ad.Upserts, ad.Removals)
				}
				pods := applyAgentPodsDelta(podMap, ad)
				s.setCurrentAgentPods(pods)
				select {
				case <-podSnapshots:
				default:
				}
				podSnapshots <- pods
			}
			if id := delta.Intercepts; id != nil {
				snap := applyInterceptsDelta(icMap, id)
				select {
				case <-icSnapshots: // discard a superseded, unprocessed snapshot
				default:
				}
				icSnapshots <- snap
			}
			return nil
		}, func() error {
			clear(podMap)
			clear(icMap)
			if relayEnabled {
				s.podRelay.beginSync()
			}
			return s.reconnectManager(managerGeneration)
		})

	// Both consumers must stop before final cleanup touches their trackers.
	cancelIngest()
	close(podSnapshots)
	close(icSnapshots)
	<-podDone
	<-icDone

	if err != nil && status.Code(err) == codes.Unimplemented && !everReceived {
		// The probe failed immediately; watchSessionEventsLegacy owns the
		// consumers' lifecycle from here on.
		return err
	}
	s.setCurrentAgentPods(nil)
	s.handleAgentPodSnapshot(ctx, nil, s.coveredCombined)
	s.handleInterceptSnapshot(pat, nil)
	return err
}

// watchSessionEventsLegacy drives the three legacy manager watchers
// concurrently, for traffic-managers that don't implement
// WatchSessionEvents. It feeds the relay exactly as the combined loop does:
// the manager-computed AgentPodInfo, relayed as-is.
func (s *session) watchSessionEventsLegacy(ctx context.Context) error {
	relayEnabled := len(s.agentPodWatchNamespaces()) > 0

	g := log.NewGroup(ctx)
	g.Go("agents", s.watchAgentsLoop)
	g.Go("intercept-port-forward", s.watchInterceptsHandler)
	if relayEnabled {
		g.Go("agent-pods-legacy", func(ctx context.Context) error {
			return agentpf.WatchPodsWithClient(ctx, s.ManagerClient, s.SessionInfo(), s.agentPodWatchNamespaces(), s.Namespace,
				func(upserts map[string]*manager.AgentPodInfo, removals []string) error {
					s.podRelay.apply(upserts, removals)
					return nil
				},
				func() error {
					s.podRelay.beginSync()
					return nil
				},
				func(narrowed []string) {
					clog.Warnf(ctx, "agent-pod watch narrowed to namespaces %v", narrowed)
				})
		})
	}
	return g.Wait()
}
