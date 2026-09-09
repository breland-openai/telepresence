package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/telepresenceio/clog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/agent/fwd"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/grpc/watcher"
	"github.com/telepresenceio/telepresence/v2/pkg/matcher"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

type guardedInterceptState interface {
	InterceptState
	setRouteGuards(desired, removed []fwd.RouteGuard)
	supportsRouteGuards() bool
	markRouteSnapshotInstalled()
}

const localRoutingKeyHeader = "X-Local-Routing-Key"

const routeIntentAcknowledgmentInterval = 5 * time.Second

func provisionalRouteGuard(runtime *rpc.InterceptInfo) (fwd.RouteGuard, bool) {
	spec := runtime.GetSpec()
	if runtime.GetId() == "" || runtime.GetRouteIncarnation() == "" || spec == nil || spec.GetWiretap() ||
		len(spec.GetHeaderFilters()) != 1 || len(spec.GetPathFilters()) != 0 {
		return fwd.RouteGuard{}, false
	}
	for name, key := range spec.GetHeaderFilters() {
		if strings.EqualFold(name, localRoutingKeyHeader) && key != "" && matcher.NewValue(key).Op() == matcher.ValueOpEqual {
			return fwd.RouteGuard{RoutingKey: key, InterceptID: runtime.GetId(), Incarnation: runtime.GetRouteIncarnation()}, true
		}
	}
	return fwd.RouteGuard{}, false
}

func (s *state) HandleRouteIntents(ctx context.Context, snapshot *rpc.RouteIntentSnapshot) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if snapshot.GetSyncState() != rpc.RouteIntentSnapshot_AUTHORITATIVE {
		return false, nil
	}
	if snapshot.GetManagerEpoch() == "" {
		return false, fmt.Errorf("authoritative route snapshot has no manager epoch")
	}
	config := s.AgentConfig()
	byPort := make(map[uint16][]fwd.RouteGuard)
	removedByPort := make(map[uint16][]fwd.RouteGuard)
	for _, intent := range snapshot.GetIntents() {
		ports, err := routeIntentTargetPorts(config, intent)
		if err != nil {
			return false, err
		}
		guard := fwd.RouteGuard{
			RoutingKey: intent.GetRoutingKey(), InterceptID: intent.GetInterceptId(), Incarnation: intent.GetIncarnation(),
		}
		for _, port := range ports {
			if intent.GetState() == rpc.RouteIntent_DESIRED {
				byPort[port] = append(byPort[port], guard)
			} else {
				removedByPort[port] = append(removedByPort[port], guard)
			}
		}
	}

	// Check that each locally scoped guard can be installed before changing any
	// forwarder. Readiness must never advertise a guard that was silently dropped.
	ports := make(map[uint16]bool)
	for _, intercept := range s.interceptStates {
		if intercept.Target().Protocol() == types.ProtoTCP {
			if guarded, ok := intercept.(guardedInterceptState); ok {
				ports[intercept.Target().ContainerPort()] = guarded.supportsRouteGuards()
			}
		}
	}
	for port := range byPort {
		if _, ok := ports[port]; !ok {
			return false, fmt.Errorf("no traffic-agent forwarder for durable TCP route port %d", port)
		}
	}
	for _, intercept := range s.interceptStates {
		if intercept.Target().Protocol() != types.ProtoTCP {
			continue
		}
		if guarded, ok := intercept.(guardedInterceptState); ok && guarded.supportsRouteGuards() {
			port := intercept.Target().ContainerPort()
			guarded.setRouteGuards(byPort[port], removedByPort[port])
		}
	}
	for _, intercept := range s.interceptStates {
		if guarded, ok := intercept.(guardedInterceptState); ok && guarded.supportsRouteGuards() {
			guarded.markRouteSnapshotInstalled()
		}
	}
	return true, nil
}

func routeIntentTargetPorts(config *agentconfig.Sidecar, intent *rpc.RouteIntent) ([]uint16, error) {
	if intent.GetState() != rpc.RouteIntent_DESIRED && intent.GetState() != rpc.RouteIntent_REMOVED {
		return nil, fmt.Errorf("route intent has unsupported state %s", intent.GetState())
	}
	if intent.GetInterceptId() == "" || intent.GetIncarnation() == "" || intent.GetRevision() == 0 ||
		intent.GetRoutingKey() == "" || matcher.NewValue(intent.GetRoutingKey()).Op() != matcher.ValueOpEqual || len(intent.GetTargets()) == 0 {
		return nil, fmt.Errorf("route intent has incomplete or unsupported predicate")
	}
	seenPorts := make(map[uint16]struct{})
	var ports []uint16
	for _, target := range intent.GetTargets() {
		if target.GetNamespace() == "" || target.GetWorkloadName() == "" || target.GetWorkloadKind() == "" ||
			target.GetContainerPort() < 1 || target.GetContainerPort() > 65535 {
			return nil, fmt.Errorf("route intent has incomplete workload target")
		}
		if target.GetNamespace() != config.Namespace || target.GetWorkloadName() != config.WorkloadName ||
			!strings.EqualFold(target.GetWorkloadKind(), string(config.WorkloadKind)) {
			continue
		}
		port := uint16(target.GetContainerPort())
		if _, found := seenPorts[port]; found {
			continue
		}
		seenPorts[port] = struct{}{}
		ports = append(ports, port)
	}
	return ports, nil
}

// RouteIntentAcknowledgments reports only desired predicates that this agent
// can enforce from the first accepted connection. A raw or undeclared port may
// still serve ordinary traffic; it must never tell the manager to activate an
// exact HTTP-header route it cannot protect.
func (s *state) RouteIntentAcknowledgments(snapshot *rpc.RouteIntentSnapshot) []*rpc.RouteIntentAck {
	config := s.AgentConfig()
	ports := make(map[uint16]bool)
	for _, intercept := range s.interceptStates {
		if guarded, ok := intercept.(guardedInterceptState); ok && intercept.Target().Protocol() == types.ProtoTCP {
			ports[intercept.Target().ContainerPort()] = guarded.supportsRouteGuards()
		}
	}
	var result []*rpc.RouteIntentAck
	for _, intent := range snapshot.GetIntents() {
		scoped, enforceable := false, true
		for _, target := range intent.GetTargets() {
			if target.GetNamespace() != config.Namespace || target.GetWorkloadName() != config.WorkloadName ||
				!strings.EqualFold(target.GetWorkloadKind(), string(config.WorkloadKind)) {
				continue
			}
			scoped = true
			enforceable = enforceable && ports[uint16(target.GetContainerPort())]
		}
		if scoped && (enforceable || intent.GetState() == rpc.RouteIntent_REMOVED) {
			result = append(result, &rpc.RouteIntentAck{InterceptId: intent.GetInterceptId(), Incarnation: intent.GetIncarnation(), Revision: intent.GetRevision()})
		}
	}
	return result
}

func routeIntentWatchLoop(
	ctx context.Context,
	manager rpc.ManagerClient,
	session *rpc.SessionInfo,
	state State,
	retryInterval time.Duration,
	readiness *interceptReadiness,
) error {
	return routeIntentWatchLoopWithRefresh(ctx, manager, session, state, retryInterval, routeIntentAcknowledgmentInterval, readiness)
}

type routeIntentAcknowledgmentRefresh struct {
	mu      sync.Mutex
	current *rpc.RouteIntentAckRequest
}

func (r *routeIntentAcknowledgmentRefresh) set(request *rpc.RouteIntentAckRequest) {
	r.mu.Lock()
	r.current = request
	r.mu.Unlock()
}

func (r *routeIntentAcknowledgmentRefresh) get() *rpc.RouteIntentAckRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.current
}

// Reaffirm the installed versions while the watch is authoritative. Kubernetes
// can update this container's restart identity just after its first ACK; the
// manager needs a later proof for that status without waiting for a route change.
func (r *routeIntentAcknowledgmentRefresh) run(ctx context.Context, manager rpc.ManagerClient, refreshInterval time.Duration) {
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if request := r.get(); request != nil {
				ackCtx, cancel := context.WithTimeout(ctx, managerHeartbeatTimeout)
				_, err := manager.AcknowledgeRouteIntents(ackCtx, stampRouteIntentAck(request))
				cancel()
				if err != nil && ctx.Err() == nil {
					clog.Debugf(ctx, "Unable to refresh installed durable route acknowledgment: %v", err)
				}
			}
		}
	}
}

type routeIntentReadinessStream struct {
	*interceptReadinessStream[rpc.RouteIntentSnapshot]
	refresh *routeIntentAcknowledgmentRefresh
}

func (s *routeIntentReadinessStream) Recv() (*rpc.RouteIntentSnapshot, error) {
	value, err := s.interceptReadinessStream.Recv()
	if err != nil {
		s.refresh.set(nil)
	}
	return value, err
}

func routeIntentWatchLoopWithRefresh(
	ctx context.Context,
	manager rpc.ManagerClient,
	session *rpc.SessionInfo,
	state State,
	retryInterval, refreshInterval time.Duration,
	readiness *interceptReadiness,
) error {
	ctx, cancel := context.WithCancel(ctx)
	var refresh routeIntentAcknowledgmentRefresh
	refreshDone := make(chan struct{})
	go func() {
		defer close(refreshDone)
		refresh.run(ctx, manager, refreshInterval)
	}()
	defer func() {
		cancel()
		<-refreshDone
	}()
	return watcher.WatchWithRetry(ctx, "WatchRouteIntents", retryInterval,
		func(ctx context.Context) (grpc.ServerStreamingClient[rpc.RouteIntentSnapshot], error) {
			refresh.set(nil)
			stream, err := manager.WatchRouteIntents(ctx, session)
			if err != nil {
				return nil, err
			}
			return &routeIntentReadinessStream{
				interceptReadinessStream: &interceptReadinessStream[rpc.RouteIntentSnapshot]{ServerStreamingClient: stream, ctx: ctx, readiness: readiness},
				refresh:                  &refresh,
			}, nil
		},
		func(snapshot *rpc.RouteIntentSnapshot) error {
			refresh.set(nil)
			generation := readiness.currentGeneration()
			applied, err := state.HandleRouteIntents(ctx, snapshot)
			if err != nil || !applied {
				return err
			}
			request := &rpc.RouteIntentAckRequest{Session: session, Applied: state.RouteIntentAcknowledgments(snapshot), AgentGuardInstance: processRouteGuardInstance}
			if err = acknowledgeRouteIntents(ctx, manager, request, retryInterval); err != nil {
				return err
			}
			refresh.set(request)
			return readiness.setSynchronized(ctx, generation)
		}, nil)
}

func acknowledgeRouteIntents(ctx context.Context, manager rpc.ManagerClient, request *rpc.RouteIntentAckRequest, retryInterval time.Duration) error {
	for {
		ackCtx, cancel := context.WithTimeout(ctx, managerHeartbeatTimeout)
		_, err := manager.AcknowledgeRouteIntents(ackCtx, stampRouteIntentAck(request))
		cancel()
		if err == nil || ctx.Err() != nil {
			return err
		}
		switch status.Code(err) {
		case codes.Unimplemented, codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound:
			// The outer agent session must reconnect when ownership/session changed;
			// retrying with the same session cannot repair it.
			return err
		default:
			clog.Warnf(ctx, "Unable to acknowledge installed durable route intents; will retry: %v", err)
		}
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func stampRouteIntentAck(request *rpc.RouteIntentAckRequest) *rpc.RouteIntentAckRequest {
	return &rpc.RouteIntentAckRequest{Session: request.Session, Applied: request.Applied, AgentGuardInstance: request.AgentGuardInstance, AppliedAt: timestamppb.Now()}
}
