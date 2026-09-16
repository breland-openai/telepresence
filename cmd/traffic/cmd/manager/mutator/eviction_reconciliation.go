package mutator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

const evictionPendingDelay = time.Second

type workloadEvictionRequest struct {
	sync.Mutex
	uid        types.UID
	configJSON string
	recorded   bool
}

func evictionKey(wl k8sapi.Workload) WorkloadKey {
	return WorkloadKey{Kind: wl.GetKind(), Name: wl.GetName(), Namespace: wl.GetNamespace()}
}

func (c *configWatcher) currentEvictionConfig(key WorkloadKey) (string, error) {
	config := c.Get(key.Name, key.Namespace)
	if config == nil {
		return "", nil
	}
	return agentconfig.MarshalTight(config)
}

func (c *configWatcher) lockEvictionRequest(key WorkloadKey) *workloadEvictionRequest {
	for {
		request, _ := c.evictionRequests.LoadOrCompute(key, func() (*workloadEvictionRequest, bool) {
			return &workloadEvictionRequest{}, false
		})
		request.Lock()
		if current, ok := c.evictionRequests.Load(key); ok && current == request {
			return request
		}
		request.Unlock()
	}
}

func (c *configWatcher) deleteEvictionRequest(key WorkloadKey) {
	for {
		request, ok := c.evictionRequests.Load(key)
		if !ok {
			return
		}
		request.Lock()
		if current, ok := c.evictionRequests.Load(key); ok && current == request {
			c.evictionRequests.Delete(key)
			request.Unlock()
			return
		}
		request.Unlock()
	}
}

func (c *configWatcher) requestWorkloadEviction(ctx context.Context, wl k8sapi.Workload, requested string) error {
	key := evictionKey(wl)
	live, err := k8sapi.GetWorkload(ctx, key.Name, key.Namespace, key.Kind)
	if err != nil {
		if uid := wl.GetUID(); uid != "" && !evictionErrorIsPermanent(err) && c.recordUnverifiedEviction(key, uid, requested) {
			if q := c.getEvictionQueue(); q != nil {
				q.AddRateLimited(key)
			}
		}
		return err
	}
	if uid := wl.GetUID(); uid == "" {
		return c.evictUnidentifiedWorkload(ctx, live, requested)
	} else if uid != live.GetUID() {
		return nil
	}
	request := c.lockEvictionRequest(key)
	current, err := c.currentEvictionConfig(key)
	if err != nil || requested != "" && current == "" {
		if !request.recorded {
			c.evictionRequests.Delete(key)
		}
		request.Unlock()
		return err
	}
	request.uid = live.GetUID()
	request.configJSON = current
	request.recorded = true
	request.Unlock()
	q := c.getEvictionQueue()
	if q != nil {
		q.Forget(key)
	}
	pending, err := c.reconcileWorkloadEviction(ctx, key)
	if q != nil && !evictionErrorIsPermanent(err) && (err != nil || pending) {
		if err != nil {
			q.AddRateLimited(key)
		} else {
			q.AddAfter(key, evictionPendingDelay)
		}
	}
	return err
}

func (c *configWatcher) recordUnverifiedEviction(key WorkloadKey, uid types.UID, requested string) bool {
	request := c.lockEvictionRequest(key)
	defer request.Unlock()
	if request.recorded && request.uid != uid {
		return false
	}
	current, err := c.currentEvictionConfig(key)
	if err != nil || requested != "" && current == "" {
		if !request.recorded {
			c.evictionRequests.Delete(key)
		}
		return false
	}
	request.uid, request.configJSON, request.recorded = uid, current, true
	return true
}

func (c *configWatcher) evictUnidentifiedWorkload(ctx context.Context, wl k8sapi.Workload, requested string) error {
	key := evictionKey(wl)
	current, err := c.currentEvictionConfig(key)
	if err != nil || requested != "" && current == "" {
		return err
	}
	if err = c.evictPods(ctx, wl, nil); err != nil || c.workloadEvictionPending(key) || workloadRolloutInProgress(wl) {
		return err
	}
	pods, err := liveWorkloadPods(ctx, wl)
	if err != nil {
		return err
	}
	return c.evictPods(ctx, wl, podsWithAgentConfigMismatch(ctx, pods, current))
}

func (c *configWatcher) reconcileWorkloadEviction(ctx context.Context, key WorkloadKey) (bool, error) {
	request, ok := c.evictionRequests.Load(key)
	if !ok {
		return false, nil
	}
	request.Lock()
	defer request.Unlock()
	if current, ok := c.evictionRequests.Load(key); !ok || current != request || !request.recorded {
		return false, nil
	}
	wl, err := k8sapi.GetWorkload(ctx, key.Name, key.Namespace, key.Kind)
	if k8sErrors.IsNotFound(err) || err == nil && request.uid != "" && wl.GetUID() != request.uid {
		c.evictionRequests.Delete(key)
		return false, nil
	}
	if err != nil {
		return true, fmt.Errorf("unable to read workload %s for agent pod eviction: %w", key, err)
	}
	current, err := c.currentEvictionConfig(key)
	if err != nil {
		return true, err
	}
	if request.configJSON != "" && current == "" {
		c.evictionRequests.Delete(key)
		return false, nil
	}
	request.configJSON = current
	if err = c.evictPods(ctx, wl, nil); err != nil {
		return true, err
	}
	if c.workloadEvictionPending(key) || workloadRolloutInProgress(wl) {
		return true, nil
	}
	pods, err := liveWorkloadPods(ctx, wl)
	if err != nil {
		return true, err
	}
	mismatches := podsWithAgentConfigMismatch(ctx, pods, current)
	if err = c.evictPods(ctx, wl, mismatches); err != nil {
		return true, err
	}
	if len(mismatches) != 0 || c.workloadEvictionPending(key) {
		return true, nil
	}
	c.evictionRequests.Delete(key)
	return false, nil
}

func (c *configWatcher) workloadEvictionPending(key WorkloadKey) bool {
	state := c.lockEvictionState(key)
	defer state.Unlock()
	return state.acceptedReplacement != nil || state.replacementPending
}

func (c *configWatcher) getEvictionQueue() workqueue.TypedRateLimitingInterface[WorkloadKey] {
	c.evictionQueueMutex.Lock()
	defer c.evictionQueueMutex.Unlock()
	return c.evictionQueue
}

func (c *configWatcher) startEvictionQueue(ctx context.Context) {
	c.evictionQueueMutex.Lock()
	defer c.evictionQueueMutex.Unlock()
	if c.evictionQueue != nil {
		return
	}
	q := workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[WorkloadKey](time.Second, 30*time.Second))
	done := make(chan struct{})
	c.evictionQueue = q
	c.evictionQueueDone = done
	c.evictionRequests.Range(func(key WorkloadKey, _ *workloadEvictionRequest) bool {
		q.Add(key)
		return true
	})
	var workers sync.WaitGroup
	workers.Add(2)
	for range 2 {
		go func() {
			defer workers.Done()
			c.runEvictionWorker(ctx, q)
		}()
	}
	go func() {
		<-ctx.Done()
		q.ShutDown()
		workers.Wait()
		close(done)
	}()
}

func (c *configWatcher) runEvictionWorker(ctx context.Context, q workqueue.TypedRateLimitingInterface[WorkloadKey]) {
	for {
		key, shutdown := q.Get()
		if shutdown {
			return
		}
		func() {
			defer q.Done(key)
			pending, err := c.reconcileWorkloadEviction(ctx, key)
			switch {
			case ctx.Err() != nil:
				q.Forget(key)
			case err != nil && evictionErrorIsPermanent(err):
				q.Forget(key)
				clog.Errorf(ctx, "Unable to continue agent pod eviction for %s: %v", key, err)
			case err != nil:
				clog.Warnf(ctx, "Retrying agent pod eviction for %s: %v", key, err)
				q.AddRateLimited(key)
			case pending:
				q.Forget(key)
				q.AddAfter(key, evictionPendingDelay)
			default:
				q.Forget(key)
			}
		}()
	}
}

func evictionErrorIsPermanent(err error) bool {
	return err != nil && (workloadRefreshErrorIsTerminal(err) || errors.Is(err, context.Canceled))
}

func (c *configWatcher) watchEvictionPods(ix cache.SharedIndexInformer) (cache.ResourceEventHandlerRegistration, error) {
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
	q := c.getEvictionQueue()
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
		if request, active := c.evictionRequests.Load(key); active {
			request.Lock()
			if request.recorded && evictionPodMatchesRequest(pod, key, request.uid) {
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
