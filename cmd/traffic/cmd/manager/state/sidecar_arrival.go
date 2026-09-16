package state

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	events "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/mutator"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/cache"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

const (
	sidecarPodRetryInterval  = 500 * time.Millisecond
	sidecarPodLookupTimeout  = 2 * time.Second
	sidecarWorkLookupTimeout = 2 * time.Second
)

type sidecarAgentArrival struct {
	work    k8sapi.Workload
	config  string
	pending map[tunnel.SessionID]*AgentSession
	retryCh <-chan time.Time
}

type sidecarAgentWaitKey struct {
	name, namespace string
}

type sidecarAgentWaitGroup struct {
	sync.Mutex
	key        sidecarAgentWaitKey
	active     int
	sequence   uint64
	latest     *sidecarAgentWait
	successful map[sidecarAgentWaitConfig]struct{}
}

type sidecarAgentWaitConfig struct {
	workloadUID k8stypes.UID
	config      string
}

type sidecarAgentWaitOutcome uint8

const (
	sidecarAgentWaitUnqualified sidecarAgentWaitOutcome = iota
	sidecarAgentWaitPreserve
	sidecarAgentWaitFailed
	sidecarAgentWaitSucceeded
)

type sidecarAgentWait struct {
	group    *sidecarAgentWaitGroup
	ctx      context.Context
	work     k8sapi.Workload
	config   sidecarAgentWaitConfig
	outcome  sidecarAgentWaitOutcome
	sequence uint64
}

func (s *State) beginSidecarAgentWait(ctx context.Context, work k8sapi.Workload) *sidecarAgentWait {
	key := sidecarAgentWaitKey{name: work.GetName(), namespace: work.GetNamespace()}
	for {
		value, _ := s.sidecarAgentWaits.LoadOrStore(key, &sidecarAgentWaitGroup{key: key})
		group := value.(*sidecarAgentWaitGroup)
		group.Lock()
		if current, _ := s.sidecarAgentWaits.Load(key); current != group {
			group.Unlock()
			continue
		}
		group.sequence++
		wait := &sidecarAgentWait{group: group, ctx: ctx, work: work, sequence: group.sequence}
		group.active++
		group.Unlock()
		return wait
	}
}

func (s *State) finishSidecarAgentWait(wait *sidecarAgentWait, config string, outcome sidecarAgentWaitOutcome) {
	group := wait.group
	group.Lock()
	defer group.Unlock()
	if outcome != sidecarAgentWaitUnqualified {
		wait.config = sidecarAgentWaitConfig{workloadUID: wait.work.GetUID(), config: config}
		wait.outcome = outcome
		if group.latest == nil || group.latest.sequence < wait.sequence {
			group.latest = wait
		}
		if outcome == sidecarAgentWaitSucceeded {
			if group.successful == nil {
				group.successful = make(map[sidecarAgentWaitConfig]struct{})
			}
			group.successful[wait.config] = struct{}{}
		}
	}
	group.active--
	if group.active != 0 {
		return
	}
	latest := group.latest
	if latest != nil && latest.outcome == sidecarAgentWaitFailed {
		if _, successful := group.successful[latest.config]; !successful {
			// New callers for this workload cannot publish until its cleanup finishes.
			s.dropAgentConfig(latest.ctx, latest.work, latest.config.config)
		}
	}
	s.sidecarAgentWaits.CompareAndDelete(group.key, group)
}

func (s *State) waitForSidecarAgent(ctx context.Context, work k8sapi.Workload, name, namespace, config string, failedCreateCh <-chan *events.Event) ([]*AgentSession, error) {
	ticker := time.NewTicker(sidecarPodRetryInterval)
	defer ticker.Stop()
	return s.waitForAgentsMatching(ctx, name, namespace, false, 1, failedCreateCh, &sidecarAgentArrival{
		work: work, config: config, pending: make(map[tunnel.SessionID]*AgentSession), retryCh: ticker.C,
	})
}

func (s *sidecarAgentArrival) apply(ctx context.Context, mm mutator.Map, arrived map[string]*AgentSession, delta cache.Delta[tunnel.SessionID, *AgentSession]) error {
	for id := range delta.Removals {
		delete(s.pending, id)
	}
	var failure error
	for id, agent := range delta.Upserts {
		if err := s.check(ctx, mm, arrived, id, agent); err != nil {
			failure = errors.Join(failure, err)
		}
	}
	if len(arrived) != 0 {
		return nil
	}
	return failure
}

func (s *sidecarAgentArrival) retry(ctx context.Context, mm mutator.Map, arrived map[string]*AgentSession) error {
	var failure error
	for id, agent := range s.pending {
		if err := s.check(ctx, mm, arrived, id, agent); err != nil {
			failure = errors.Join(failure, err)
		}
	}
	if len(arrived) != 0 {
		return nil
	}
	return failure
}

func (s *sidecarAgentArrival) check(ctx context.Context, mm mutator.Map, arrived map[string]*AgentSession, id tunnel.SessionID, agent *AgentSession) error {
	delete(s.pending, id)
	if mm.IsInactive(k8stypes.UID(agent.PodUid)) {
		clog.Debugf(ctx, "Agent %s(%s) is blacklisted", agent.PodName, agent.PodIp)
		return nil
	}
	if agent.PodName == "" || agent.PodUid == "" {
		return nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, sidecarPodLookupTimeout)
	defer cancel()
	pod, err := k8sapi.GetK8sInterface(lookupCtx).CoreV1().Pods(agent.Namespace).Get(lookupCtx, agent.PodName, meta.GetOptions{})
	if err != nil {
		if ctx.Err() != nil || lookupCtx.Err() != nil || transientSidecarLookup(err) {
			s.pending[id] = agent
			return nil
		}
		return fmt.Errorf("checking traffic-agent pod %s.%s for the requested configuration: %w", agent.PodName, agent.Namespace, err)
	}
	if string(pod.UID) != agent.PodUid || pod.DeletionTimestamp != nil || pod.Annotations[annotation.Config] != s.config {
		clog.Debugf(ctx, "Agent %s(%s) does not carry the requested configuration on its current physical pod", agent.PodName, agent.PodIp)
		return nil
	}
	owned, err := mutator.PodOwnedByWorkload(lookupCtx, s.work, pod)
	if err != nil {
		if ctx.Err() != nil || lookupCtx.Err() != nil || transientSidecarLookup(err) {
			s.pending[id] = agent
			return nil
		}
		return fmt.Errorf("checking ownership of traffic-agent pod %s.%s for workload %s: %w", agent.PodName, agent.Namespace, s.work, err)
	}
	if !owned {
		s.pending[id] = agent
		clog.Debugf(ctx, "Agent %s(%s) does not belong to requested workload %s", agent.PodName, agent.PodIp, s.work)
		return nil
	}
	clog.Debugf(ctx, "Agent %s(%s) belongs to workload %s and carries the requested configuration", agent.PodName, agent.PodIp, s.work)
	arrived[agent.PodUid] = agent
	return nil
}

func transientSidecarLookup(err error) bool {
	var networkError net.Error
	return apierrors.IsNotFound(err) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err) || apierrors.IsInternalError(err) ||
		errors.As(err, &networkError) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func (s *State) dropAgentConfig(ctx context.Context, wl k8sapi.Workload, expectedConfig string) {
	if wl.GetUID() == "" {
		return
	}
	_, err := mutator.GetMap(ctx).Update(wl.GetName(), wl.GetNamespace(), func(current *agentconfig.Sidecar) (*agentconfig.Sidecar, error) {
		if current == nil {
			return nil, nil
		}
		actualConfig, err := agentconfig.MarshalTight(current)
		if err != nil || actualConfig != expectedConfig {
			return current, err
		}
		// Keep the key locked until the live workload identity is confirmed.
		lookupCtx, cancel := context.WithTimeout(ctx, sidecarWorkLookupTimeout)
		defer cancel()
		live, err := k8sapi.GetWorkload(lookupCtx, wl.GetName(), wl.GetNamespace(), wl.GetKind())
		if err != nil {
			if apierrors.IsNotFound(err) {
				return current, nil
			}
			return current, err
		}
		if live.GetUID() != wl.GetUID() {
			return current, nil
		}
		return nil, nil
	})
	if err != nil {
		clog.Warnf(ctx, "Keeping traffic-agent configuration after a failed agent wait for %s: %v", wl, err)
	}
}
