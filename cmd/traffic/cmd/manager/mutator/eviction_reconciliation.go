package mutator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"

	core "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

const (
	evictionRequeueBase = time.Second
	evictionRequeueMax  = 30 * time.Second
	evictionMaxFailures = 10
	evictionWorkers     = 2

	// evictionDeferralNotice is how long an agent update may be deferred before that is logged at
	// info level.
	evictionDeferralNotice = time.Minute

	// evictionDeferralCeiling is how long an agent update may be deferred before the workload is
	// restarted through its controller instead.
	evictionDeferralCeiling = 5 * time.Minute

	// evictionBudgetGrace is how long a PodDisruptionBudget may keep refusing evictions before the
	// workload is restarted through its controller instead. The budget's status trails pod readiness,
	// so a refusal right after a batch is normally transient.
	evictionBudgetGrace = 30 * time.Second
)

type evictionQueue = workqueue.TypedRateLimitingInterface[WorkloadKey]

func newEvictionQueue(base, maxDelay time.Duration) evictionQueue {
	return workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[WorkloadKey](base, maxDelay))
}

// requestEviction records the agent config the pods of wl must carry and schedules a reconcile pass.
// It also hides the agents of the pods that must be replaced.
func (c *configWatcher) requestEviction(ctx context.Context, wl k8sapi.Workload, wanted *agentconfig.Sidecar) {
	if wl.GetUID() == "" {
		return
	}
	key := workloadKeyOf(wl)
	clog.Debugf(ctx, "Requesting replacement of pods with agent config mismatch for %s", wl)
	state := c.lockEvictionState(key)
	if state.requested && state.uid != wl.GetUID() {
		// A delayed request for an older incarnation cannot replace a current request.
		current, err := evictionWorkload(ctx, key)
		if err != nil || current.GetUID() != wl.GetUID() {
			state.Unlock()
			return
		}
		c.finishRequest(state)
	}
	if !state.requested {
		state.requested = true
		c.pendingRequests.Add(1)
	}
	state.denied = nil
	state.uid = wl.GetUID()
	state.wanted = wanted
	state.tracksConfig = c.Get(key.Name, key.Namespace) != nil || wanted == nil
	if pods, err := workloadPods(ctx, wl); err == nil {
		c.markPendingEviction(pods, podsWithAgentConfigMismatch(ctx, pods, wanted))
	} else {
		clog.Debugf(ctx, "Unable to list pods of %s: %v", wl, err)
	}
	state.Unlock()
	c.evictionQueue.Add(key)
}

// markPendingEviction hides the agents of the mismatched pods until a pass evicts them, and shows
// the agents of the other pods again if an earlier pass hid them.
func (c *configWatcher) markPendingEviction(pods, mismatched []*core.Pod) {
	for _, pod := range pods {
		pending := slices.Contains(mismatched, pod)
		c.inactivePods.Compute(pod.UID, func(v inactivation, loaded bool) (inactivation, xsync.ComputeOp) {
			switch {
			case loaded && v.deleted:
				return v, xsync.CancelOp
			case pending:
				return inactivation{Time: time.Now(), pending: true}, xsync.UpdateOp
			case loaded && v.pending:
				return v, xsync.DeleteOp
			default:
				return v, xsync.CancelOp
			}
		})
	}
}

// finishRequest clears the request. The caller holds the state's lock.
func (c *configWatcher) finishRequest(state *workloadEvictionState) {
	if state.requested {
		state.requested = false
		c.pendingRequests.Add(-1)
	}
	state.accepted = nil
	state.denied = nil
	state.uid = ""
	state.wanted = nil
	state.rolloutGeneration = 0
	state.failures = 0
	state.resetDeferral()
}

func (c *configWatcher) startEvictionWorkers(ctx context.Context) {
	var workers sync.WaitGroup
	c.evictionQueueDone = make(chan struct{})
	for range evictionWorkers {
		workers.Go(func() { c.runEvictionWorker(ctx) })
	}
	go func() {
		<-ctx.Done()
		c.evictionQueue.ShutDown()
		workers.Wait()
		close(c.evictionQueueDone)
	}()
}

func (c *configWatcher) runEvictionWorker(ctx context.Context) {
	q := c.evictionQueue
	for {
		key, shutdown := q.Get()
		if shutdown {
			return
		}
		pending, err := c.reconcile(ctx, key)
		switch {
		case permanentEvictionError(err):
			clog.Warnf(ctx, "Agent pod eviction for %s awaits a new event: %v", key, err)
			q.Forget(key)
		case err != nil && ctx.Err() == nil:
			clog.Warnf(ctx, "Agent pod eviction for %s will be retried: %v", key, err)
			q.AddRateLimited(key)
		case err == nil && pending:
			q.AddRateLimited(key)
		default:
			q.Forget(key)
		}
		q.Done(key)
	}
}

// reconcile runs one eviction pass for the workload and reports whether more passes are needed.
func (c *configWatcher) reconcile(ctx context.Context, key WorkloadKey) (pending bool, err error) {
	state := c.lockEvictionState(key)
	defer state.Unlock()
	if !state.requested {
		return false, nil
	}
	if state.denied != nil {
		return false, state.denied
	}
	pending, err = c.reconcileLocked(ctx, key, state)
	switch {
	case err != nil:
		if ctx.Err() != nil {
			return false, err
		}
		if permanentEvictionError(err) {
			state.denied = err
			return false, err
		}
		state.failures++
		if state.failures >= evictionMaxFailures {
			clog.Errorf(ctx, "Giving up on agent pod eviction for %s: %v", key, err)
			c.finishRequest(state)
			return false, nil
		}
	case !pending:
		c.finishRequest(state)
	default:
		state.failures = 0
	}
	return pending, err
}

func (c *configWatcher) reconcileLocked(ctx context.Context, key WorkloadKey, state *workloadEvictionState) (bool, error) {
	wl, err := k8sapi.GetWorkload(ctx, key.Name, key.Namespace, key.Kind)
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			clog.Debugf(ctx, "Agent pod eviction for %s ends because the workload is gone", key)
			return false, nil
		}
		return true, err
	}
	if state.uid == "" || wl.GetUID() != state.uid {
		return false, nil
	}
	if state.tracksConfig {
		current := c.Get(key.Name, key.Namespace)
		if state.wanted != nil && current == nil {
			return false, nil
		}
		state.wanted = current
	}
	pods, err := liveWorkloadPods(ctx, wl)
	if err != nil {
		return true, err
	}
	if state.accepted != nil {
		if !state.accepted.recovered(wl, pods) {
			return true, nil
		}
		state.accepted = nil
	}
	mismatched := podsWithAgentConfigMismatch(ctx, pods, state.wanted)
	c.markPendingEviction(pods, mismatched)
	if len(mismatched) == 0 {
		clog.Debugf(ctx, "All pods of %s carry the wanted agent config", wl)
		return false, nil
	}
	reason, ownRollout, budget := state.pacing(wl, pods)
	if reason != "" {
		if !c.deferEviction(ctx, state, wl, reason, !ownRollout) {
			return true, nil
		}
		if err = triggerRollout(ctx, wl, mismatched[0]); err != nil {
			return true, err
		}
		state.rolloutGeneration = wl.GetGeneration()
		state.resetDeferral()
		return true, nil
	}
	accepted, refused, err := c.evictPodBatch(ctx, mismatched, budget)
	evicted := len(accepted)
	if evicted > 0 {
		state.accepted = newEvictionBatch(wl, pods, accepted)
	}
	if err != nil {
		return true, err
	}
	if evicted > 0 {
		state.resetDeferral()
		clog.Debugf(ctx, "Evicted %d of %d pods with agent config mismatch for %s", evicted, len(mismatched), wl)
	}
	if refused == nil || evicted > 0 {
		return true, nil
	}
	if !c.budgetRefused(ctx, state, wl, refused) {
		return true, nil
	}
	if err = triggerRollout(ctx, wl, refused); err != nil {
		return true, err
	}
	state.rolloutGeneration = wl.GetGeneration()
	state.resetDeferral()
	return true, nil
}

// budgetRefused records that the disruption budget refused to evict pod and reports whether it has
// done so for long enough for the workload to be restarted through its controller instead.
func (c *configWatcher) budgetRefused(ctx context.Context, state *workloadEvictionState, wl k8sapi.Workload, pod *core.Pod) bool {
	now := time.Now()
	if state.budgetRefusedSince.IsZero() {
		state.budgetRefusedSince = now
	}
	refusing := now.Sub(state.budgetRefusedSince)
	if refusing >= c.budgetGrace {
		clog.Infof(ctx, "The disruption budget of %s has refused evictions for %s; restarting it through its controller",
			wl, refusing.Round(time.Second))
		return true
	}
	c.deferEviction(ctx, state, wl, fmt.Sprintf("the disruption budget refused to evict pod %s", pod.Name), false)
	return false
}

// pacing reports why the pass must wait, or how many pods it may evict. ownRollout is true when the
// wait is for a rollout that an earlier pass triggered.
func (s *workloadEvictionState) pacing(wl k8sapi.Workload, pods []*core.Pod) (reason string, ownRollout bool, budget int) {
	if s.rolloutGeneration != 0 {
		if wl.GetGeneration() <= s.rolloutGeneration || workloadRolloutInProgress(wl) {
			return "the rollout it triggered has not completed", true, 0
		}
		s.rolloutGeneration = 0
	}
	if workloadRolloutInProgress(wl) {
		return "a rollout is in progress", false, 0
	}
	unavailable := unavailablePods(wl, pods)
	budget = allowedUnavailable(wl) - unavailable
	if budget <= 0 {
		return fmt.Sprintf("%d of %d replicas are unavailable", unavailable, k8sapi.DesiredReplicas(wl)), false, 0
	}
	return "", false, budget
}

func (s *workloadEvictionState) resetDeferral() {
	s.deferredSince = time.Time{}
	s.deferralNoticed = false
	s.budgetRefusedSince = time.Time{}
}

// deferEviction records that the pass made no progress and reports whether the deferral has lasted
// long enough for the workload to be restarted through its controller instead.
func (c *configWatcher) deferEviction(ctx context.Context, state *workloadEvictionState, wl k8sapi.Workload, reason string, allowRestart bool) bool {
	now := time.Now()
	if state.deferredSince.IsZero() {
		state.deferredSince = now
	}
	waited := now.Sub(state.deferredSince)
	switch {
	case allowRestart && waited >= c.deferralCeiling:
		clog.Infof(ctx, "Agent update for %s was deferred for %s because %s; restarting it through its controller",
			wl, waited.Round(time.Second), reason)
		return true
	case waited >= c.deferralNotice && !state.deferralNoticed:
		state.deferralNoticed = true
		clog.Infof(ctx, "Deferring agent update for %s because %s", wl, reason)
	default:
		clog.Debugf(ctx, "Deferring agent update for %s because %s", wl, reason)
	}
	return false
}

func (c *configWatcher) watchPods(ix cache.SharedIndexInformer) (cache.ResourceEventHandlerRegistration, error) {
	if ix == nil {
		return nil, nil
	}
	return ix.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueEvictionPod,
		UpdateFunc: c.enqueueChangedEvictionPod,
		DeleteFunc: c.enqueueEvictionPod,
	})
}

func (c *configWatcher) enqueueChangedEvictionPod(oldObject, newObject any) {
	old, oldOK := oldObject.(*core.Pod)
	newPod, newOK := newObject.(*core.Pod)
	if !oldOK || !newOK || old.UID != newPod.UID || old.Status.Phase != newPod.Status.Phase ||
		old.DeletionTimestamp.IsZero() != newPod.DeletionTimestamp.IsZero() || podIsReady(old) != podIsReady(newPod) {
		c.enqueueEvictionPod(newObject)
	}
}

func (c *configWatcher) enqueueEvictionPod(object any) {
	if c.pendingRequests.Load() == 0 {
		return
	}
	if tombstone, ok := object.(cache.DeletedFinalStateUnknown); ok {
		object = tombstone.Obj
	}
	pod, ok := object.(*core.Pod)
	if !ok {
		return
	}
	owner := meta.GetControllerOf(pod)
	if owner == nil {
		return
	}
	kind, valid := liveControllerKind(*owner)
	if !valid {
		return
	}
	q := c.evictionQueue
	if q == nil {
		return
	}
	keys := make(map[WorkloadKey]struct{}, 2)
	if kind, name := pod.Labels[agentconfig.WorkloadKindLabel], pod.Labels[agentconfig.WorkloadNameLabel]; kind != "" && name != "" {
		keys[WorkloadKey{Kind: k8sapi.Kind(kind), Name: name, Namespace: pod.Namespace}] = struct{}{}
	}
	switch kind {
	case k8sapi.StatefulSetKind, k8sapi.ReplicaSetKind, k8sapi.DeploymentKind, k8sapi.RolloutKind:
		keys[WorkloadKey{Kind: kind, Name: owner.Name, Namespace: pod.Namespace}] = struct{}{}
	}
	for key := range keys {
		if request, active := c.evictionStates.Load(key); active {
			request.Lock()
			if request.requested && evictionPodMatchesRequest(pod, key, request.uid) {
				request.denied = nil
				q.Forget(key)
				q.Add(key)
			}
			request.Unlock()
		}
	}
}

func evictionPodMatchesRequest(pod *core.Pod, key WorkloadKey, uid types.UID) bool {
	owner := meta.GetControllerOf(pod)
	if owner == nil || k8sapi.Kind(owner.Kind) != key.Kind {
		return true
	}
	return owner.Name == key.Name && (uid == "" || owner.UID == uid)
}

// evictionBatch tracks the physical pods present when Kubernetes accepted a batch.
type evictionBatch struct {
	before   map[types.UID]struct{}
	accepted map[types.UID]struct{}
	desired  int32
}

func newEvictionBatch(wl k8sapi.Workload, pods, accepted []*core.Pod) *evictionBatch {
	b := &evictionBatch{desired: k8sapi.DesiredReplicas(wl), before: make(map[types.UID]struct{}, len(pods)), accepted: make(map[types.UID]struct{}, len(accepted))}
	for _, pod := range pods {
		b.before[pod.UID] = struct{}{}
	}
	for _, pod := range accepted {
		b.accepted[pod.UID] = struct{}{}
	}
	return b
}

func (b *evictionBatch) recovered(wl k8sapi.Workload, pods []*core.Pod) bool {
	var ready, fresh int
	for _, pod := range pods {
		if _, retiring := b.accepted[pod.UID]; retiring {
			if pod.DeletionTimestamp == nil {
				return false
			}
			continue
		}
		if !podIsReady(pod) {
			continue
		}
		ready++
		if _, old := b.before[pod.UID]; !old {
			fresh++
		}
	}
	desired := k8sapi.DesiredReplicas(wl)
	return ready >= int(desired) && fresh >= len(b.accepted)-int(max(0, b.desired-desired))
}

func (c *configWatcher) removeAgentsAndWait(ctx context.Context, namespace string) error {
	c.deleteNamespaceAgentConfigs(namespace)
	workloads, err := liveAgentPodList(ctx, namespace)
	if err != nil {
		return err
	}
	keys := make(map[WorkloadKey]struct{}, len(workloads))
	for key, wp := range workloads {
		c.requestEviction(ctx, wp.wl, nil)
		keys[key] = struct{}{}
	}
	c.evictionStates.Range(func(key WorkloadKey, state *workloadEvictionState) bool {
		if namespace == "" || key.Namespace == namespace {
			state.Lock()
			if state.requested {
				state.wanted = nil
				state.tracksConfig = true
				keys[key] = struct{}{}
			}
			state.Unlock()
		}
		return true
	})
	timeout := managerutil.GetEnv(ctx).AgentArrivalTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	recoveryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var errs error
	for key := range keys {
		err = c.waitForEvictionRecovery(recoveryCtx, key)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("removing agents from %s: %w", key, err))
		}
	}
	return errs
}

func permanentEvictionError(err error) bool {
	return k8sErrors.IsForbidden(err) || k8sErrors.IsUnauthorized(err) || k8sErrors.IsBadRequest(err) || k8sErrors.IsInvalid(err)
}

func (c *configWatcher) waitForEvictionRecovery(ctx context.Context, key WorkloadKey) error {
	return wait.PollUntilContextCancel(ctx, 300*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		pending, err := c.reconcile(ctx, key)
		return !pending, err
	})
}
