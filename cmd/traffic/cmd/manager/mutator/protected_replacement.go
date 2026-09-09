package mutator

import (
	"context"
	"errors"
	"time"

	core "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/agentmap"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

// A Service can change again while our first replacement is starting. If the
// stale agent configuration prevents that replacement becoming Ready, waiting
// for workload recovery before retrying would prevent recovery indefinitely.
// This exception is restricted to our pending replacement and never uses a
// workload restart/scale fallback when the normal policy eviction is denied.
func (c *configWatcher) catchUpProtectedReplacement(ctx context.Context, wl k8sapi.Workload) (bool, error) {
	key := WorkloadKey{Kind: wl.GetKind(), Name: wl.GetName(), Namespace: wl.GetNamespace()}
	if c.protectedReplacementConfig(key) == "" {
		return false, nil
	}
	st, known := c.evictionStates.Load(key)
	if !known {
		return false, nil
	}
	st.Lock()
	defer st.Unlock()
	if current, ok := c.evictionStates.Load(key); !ok || current != st || !st.replacementPending {
		return false, nil
	}
	wl, err := refreshWorkload(ctx, wl)
	if err != nil {
		return true, err
	}
	if !workloadUpdateInProgress(wl) {
		st.replacementPending = false
		return false, nil
	}
	if workloadRolloutInProgress(wl) {
		return false, nil // normal reconciliation handles unrelated workload rollouts
	}
	pods, err := workloadPods(ctx, wl)
	if err != nil {
		return true, err
	}
	for _, candidate := range pods {
		if candidate.Annotations[annotation.Config] == "" || candidate.Annotations[annotation.Config] == c.protectedReplacementConfig(key) || c.isEvicted(candidate.UID) {
			continue
		}
		if handled, err := c.evictNonservingProtectedReplacement(ctx, wl, key, candidate); handled || err != nil {
			return true, err // one policy eviction per workload/reconciliation
		}
	}
	return false, nil
}

func (c *configWatcher) protectedReplacementConfig(key WorkloadKey) string {
	sc := c.Get(key.Name, key.Namespace)
	if sc == nil || !sc.RequireAuthoritativeRoutes || sc.Manual || sc.WorkloadKind != key.Kind {
		return ""
	}
	current, err := agentconfig.MarshalTight(sc)
	if err != nil {
		return ""
	}
	return current
}

func (c *configWatcher) evictNonservingProtectedReplacement(ctx context.Context, wl k8sapi.Workload, key WorkloadKey, candidate *core.Pod) (bool, error) {
	client := k8sapi.GetK8sInterface(ctx)
	pod, err := client.CoreV1().Pods(candidate.Namespace).Get(ctx, candidate.Name, meta.GetOptions{})
	if k8sErrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if pod.UID != candidate.UID || pod.ResourceVersion == "" || !explicitlyNonservingReplacement(pod) || podIsManuallyInjected(ctx, pod) {
		return false, nil
	}
	owned, err := protectedReplacementOwnedBy(ctx, pod, wl)
	if err != nil {
		return false, err
	}
	if !owned {
		return false, nil
	}
	slices, err := client.DiscoveryV1().EndpointSlices(pod.Namespace).List(ctx, meta.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, slice := range slices.Items {
		for _, endpoint := range slice.Endpoints {
			ref := endpoint.TargetRef
			if ref != nil && ref.UID == pod.UID && (ref.Namespace == "" || ref.Namespace == pod.Namespace) &&
				(endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready || endpoint.Conditions.Serving != nil && *endpoint.Conditions.Serving) {
				return false, nil
			}
		}
	}
	// Re-read the cached desired configuration after all API checks. A queued Pod
	// or Service event must not replay a previous version across another change.
	latest := c.protectedReplacementConfig(key)
	if latest == "" || pod.Annotations[annotation.Config] == "" || pod.Annotations[annotation.Config] == latest {
		return false, nil
	}
	evicted, err := evictPodWithVersion(ctx, pod, &pod.ResourceVersion)
	if err != nil {
		if errors.As(err, &disruptionBudgetError{}) || k8sErrors.IsTooManyRequests(err) {
			clog.Debugf(ctx, "Waiting for disruption budget before replacing stale nonserving protected pod %s/%s", pod.Namespace, pod.Name)
			return true, nil
		}
		return true, err
	}
	if evicted {
		c.inactivePods.Store(pod.UID, inactivation{Time: time.Now(), deleted: true})
		clog.Infof(ctx, "Evicted stale nonserving protected pod %s/%s to catch up its agent configuration", pod.Namespace, pod.Name)
	}
	return true, nil
}

func protectedReplacementOwnedBy(ctx context.Context, pod *core.Pod, wl k8sapi.Workload) (bool, error) {
	owner := meta.GetControllerOf(pod)
	if owner == nil || owner.UID == "" || wl.GetUID() == "" || pod.Namespace != wl.GetNamespace() {
		return false, nil
	}
	matches := func(ref *meta.OwnerReference) bool {
		return ref != nil && ref.Kind == string(wl.GetKind()) && ref.Name == wl.GetName() && ref.UID == wl.GetUID()
	}
	if matches(owner) {
		return true, nil
	}
	if owner.Kind != string(k8sapi.ReplicaSetKind) || (wl.GetKind() != k8sapi.DeploymentKind && wl.GetKind() != k8sapi.RolloutKind) {
		return false, nil
	}
	rs, err := k8sapi.GetK8sInterface(ctx).AppsV1().ReplicaSets(pod.Namespace).Get(ctx, owner.Name, meta.GetOptions{})
	if k8sErrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return rs.UID == owner.UID && matches(meta.GetControllerOf(rs)), nil
}

func explicitlyNonservingReplacement(pod *core.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != core.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == core.PodReady {
			return condition.Status == core.ConditionFalse
		}
	}
	return false
}

func (c *configWatcher) watchProtectedReplacementPods(ctx context.Context, ix cache.SharedIndexInformer) (cache.ResourceEventHandlerRegistration, error) {
	reconcile := func(obj any) {
		pod, ok := obj.(*core.Pod)
		if ok {
			c.reconcileProtectedReplacementPod(ctx, pod)
		}
	}
	return ix.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: reconcile, UpdateFunc: func(_, current any) { reconcile(current) }})
}

func (c *configWatcher) reconcileProtectedReplacementPod(ctx context.Context, pod *core.Pod) {
	if !managerutil.GetEnv(ctx).AgentRequireAuthoritativeRoutes || pod.Annotations[annotation.Config] == "" {
		return
	}
	annotationConfig, err := agentconfig.UnmarshalJSON(pod.Annotations[annotation.Config])
	if err != nil || annotationConfig.WorkloadName == "" || annotationConfig.Namespace != pod.Namespace {
		return
	}
	key := WorkloadKey{Kind: annotationConfig.WorkloadKind, Name: annotationConfig.WorkloadName, Namespace: pod.Namespace}
	if current := c.protectedReplacementConfig(key); current != "" && current != pod.Annotations[annotation.Config] {
		c.reconcilePendingProtectedReplacement(ctx, key)
	}
}

func (c *configWatcher) reconcilePendingProtectedReplacement(ctx context.Context, key WorkloadKey) {
	if c.protectedReplacementConfig(key) == "" {
		return
	}
	st, known := c.evictionStates.Load(key)
	if !known {
		return
	}
	st.Lock()
	pending := st.replacementPending
	st.Unlock()
	if !pending {
		return
	}
	wl, err := agentmap.GetWorkload(ctx, key.Name, key.Namespace, key.Kind)
	if err == nil {
		_, err = c.catchUpProtectedReplacement(ctx, wl)
	}
	if err != nil && ctx.Err() == nil && !k8sErrors.IsNotFound(err) {
		clog.Debugf(ctx, "Unable to catch up protected replacement for %s/%s: %v", key.Namespace, key.Name, err)
	}
}

func (c *configWatcher) retryPendingProtectedReplacements(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.evictionStates.Range(func(key WorkloadKey, _ *workloadEvictionState) bool {
				c.reconcilePendingProtectedReplacement(ctx, key)
				return ctx.Err() == nil
			})
		}
	}
}
