package mutator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	argoRolloutsAPI "github.com/datawire/argo-rollouts-go-client/pkg/apis/rollouts"
	argoRollouts "github.com/datawire/argo-rollouts-go-client/pkg/apis/rollouts/v1alpha1"
	"github.com/puzpuzpuz/xsync/v4"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	v1 "k8s.io/api/policy/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/agentmap"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

type wlPods struct {
	wl   k8sapi.Workload
	pods []*core.Pod
}

type wlPodMap map[WorkloadKey]*wlPods

func (em wlPodMap) add(wl k8sapi.Workload, pod *core.Pod) {
	k := WorkloadKey{Kind: wl.GetKind(), Name: wl.GetName(), Namespace: wl.GetNamespace()}
	if v, ok := em[k]; ok {
		v.pods = append(v.pods, pod)
	} else {
		em[k] = &wlPods{wl: wl, pods: []*core.Pod{pod}}
	}
}

func (c *configWatcher) EvictPodsWithAgentConfigMismatch(ctx context.Context, wl k8sapi.Workload, sc *agentconfig.Sidecar) error {
	cfgJSON, err := agentconfig.MarshalTight(sc)
	if err != nil {
		return err
	}
	return c.requestWorkloadEviction(ctx, wl, cfgJSON)
}

func (c *configWatcher) EvictPodsWithAgentConfig(ctx context.Context, wl k8sapi.Workload) error {
	return c.requestWorkloadEviction(ctx, wl, "")
}

func (c *configWatcher) EvictAllPodsWithAgentConfig(ctx context.Context, namespace string) error {
	c.deleteNamespaceAgentConfigs(namespace)
	evictMap, err := liveAgentPodList(ctx, namespace)
	if err != nil {
		return err
	}
	var errs error
	for _, wp := range evictMap {
		err = c.EvictPodsWithAgentConfig(ctx, wp.wl)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func (c *configWatcher) evictAllPodsWithAgentConfigAndWait(ctx context.Context, namespace string) error {
	c.agentConfigs.Delete(namespace)
	failedWorkloads := make(map[WorkloadKey]struct{})
	var errs error
	for {
		evictMap, err := podList(ctx, namespace)
		if err != nil {
			return errors.Join(errs, err)
		}

		foundAgent := false
		for key, wp := range evictMap {
			if _, failed := failedWorkloads[key]; failed {
				continue
			}
			pods := podsWithAgentConfigMismatch(ctx, wp.pods, "")
			if len(pods) == 0 {
				continue
			}
			foundAgent = true
			if err = c.evictPods(ctx, wp.wl, pods); err != nil {
				errs = errors.Join(errs, fmt.Errorf("unable to evict agents from %s: %w", wp.wl, err))
				failedWorkloads[key] = struct{}{}
				continue
			}

			recoveryCtx := ctx
			cancel := func() {}
			if timeout := managerutil.GetEnv(ctx).AgentArrivalTimeout; timeout > 0 {
				recoveryCtx, cancel = context.WithTimeout(ctx, timeout)
			}
			err = waitForWorkloadRecovery(recoveryCtx, wp.wl)
			cancel()
			if err != nil {
				errs = errors.Join(errs, err)
				failedWorkloads[key] = struct{}{}
			}
		}
		if !foundAgent {
			return errs
		}
	}
}

func podsWithAgentConfigMismatch(ctx context.Context, pods []*core.Pod, cfgJSON string) []*core.Pod {
	return slices.DeleteFunc(slices.Clone(pods), func(pod *core.Pod) bool {
		if podIsManuallyInjected(ctx, pod) {
			clog.Tracef(ctx, "Keeping pod %s because it is managed manually", pod.Name)
			return true
		}
		if pod.Annotations[annotation.Config] == cfgJSON {
			clog.Tracef(ctx, "Keeping pod %s because its config is still valid", pod.Name)
			return true
		}
		return false
	})
}

func podIsManuallyInjected(ctx context.Context, pod *core.Pod) bool {
	v := annotation.GetAnnotation(ctx, pod.Annotations, annotation.ManuallyInjected, annotation.LegacyManuallyInjected)
	return v == "true"
}

type podEvictionResult uint8

const (
	podEvictionGone podEvictionResult = iota
	podEvictionStarted
	podEvictionDeferred
	podEvictionAccepted
)

func (c *configWatcher) evictPods(ctx context.Context, wl k8sapi.Workload, pods []*core.Pod) (err error) {
	var evictionState *workloadEvictionState
	if wl != nil {
		key := WorkloadKey{Kind: wl.GetKind(), Name: wl.GetName(), Namespace: wl.GetNamespace()}
		evictionState = c.lockEvictionState(key)
		defer evictionState.Unlock()

		previousUID := wl.GetUID()
		wl, err = refreshWorkload(ctx, wl)
		if err != nil {
			return err
		}
		if previousUID != "" && wl.GetUID() != previousUID {
			return nil
		}
		if pending := evictionState.acceptedReplacement; pending != nil {
			var recovered bool
			if recovered, err = pending.recovered(ctx, wl); err != nil {
				return err
			}
			if !recovered {
				clog.Debugf(ctx, "Deferring pod eviction because %s is replacing pod %s", wl, pending.name)
				return nil
			}
			evictionState.acceptedReplacement = nil
		}
		if !workloadUpdateInProgress(wl) {
			evictionState.replacementPending = false
		}
		if k8sapi.DesiredReplicas(wl) == 0 {
			clog.Debugf(ctx, "Deferring pod eviction because %s has no desired replicas", wl)
			return nil
		}
		if evictionState.replacementPending || workloadRolloutInProgress(wl) {
			clog.Debugf(ctx, "Deferring pod eviction because %s is already updating", wl)
			return nil
		}
	}

	var ownedPods []*core.Pod
	listedPods := false
	for _, pod := range pods {
		podID := pod.UID
		if c.isEvicted(podID) {
			clog.Debugf(ctx, "Skipping pod %s because it is already deleted", pod.Name)
			continue
		}
		if podIsManuallyInjected(ctx, pod) {
			clog.Tracef(ctx, "Skipping pod %s because it is managed manually", pod.Name)
			continue
		}
		if evictionState != nil && !listedPods {
			if ownedPods, err = liveWorkloadPods(ctx, wl); err != nil {
				return err
			}
			listedPods = true
		}
		result := podEvictionGone
		c.inactivePods.Compute(podID, func(v inactivation, loaded bool) (inactivation, xsync.ComputeOp) {
			if loaded && v.deleted {
				return v, xsync.CancelOp
			}
			result, err = evictOrRollout(ctx, wl, pod)
			if err != nil || result == podEvictionDeferred {
				return v, xsync.CancelOp
			}
			return inactivation{Time: time.Now(), deleted: true}, xsync.UpdateOp
		})
		if err != nil {
			return err
		}
		switch result {
		case podEvictionDeferred:
			return nil
		case podEvictionAccepted:
			if evictionState != nil {
				evictionState.acceptedReplacement = newPodReplacement(wl, pod, ownedPods)
				return nil
			}
		case podEvictionStarted:
			if evictionState != nil {
				evictionState.replacementPending = true
				return nil
			}
		}
	}
	return nil
}

type podReplacement struct {
	name            string
	uid             types.UID
	workloadUID     types.UID
	desiredReplicas int32
	previousUIDs    map[types.UID]struct{}
}

func newPodReplacement(wl k8sapi.Workload, pod *core.Pod, ownedPods []*core.Pod) *podReplacement {
	previous := make(map[types.UID]struct{}, len(ownedPods)+1)
	previous[pod.UID] = struct{}{}
	for _, p := range ownedPods {
		previous[p.UID] = struct{}{}
	}
	return &podReplacement{
		name: pod.Name, uid: pod.UID, workloadUID: wl.GetUID(),
		desiredReplicas: k8sapi.DesiredReplicas(wl), previousUIDs: previous,
	}
}

func (p *podReplacement) recovered(ctx context.Context, wl k8sapi.Workload) (bool, error) {
	if p.workloadUID != "" && wl.GetUID() != p.workloadUID {
		return true, nil
	}
	old, err := k8sapi.GetK8sInterface(ctx).CoreV1().Pods(wl.GetNamespace()).Get(ctx, p.name, meta.GetOptions{})
	if err != nil && !k8sErrors.IsNotFound(err) {
		return false, err
	}
	if err == nil && old.UID == p.uid && old.DeletionTimestamp == nil {
		return false, nil
	}
	pods, err := liveWorkloadPods(ctx, wl)
	if err != nil {
		return false, err
	}
	var ready int32
	freshReady := false
	for _, pod := range pods {
		if pod.UID == "" || pod.UID == p.uid || !podIsReady(pod) {
			continue
		}
		ready++
		if _, seen := p.previousUIDs[pod.UID]; !seen {
			freshReady = true
		}
	}
	desired := k8sapi.DesiredReplicas(wl)
	return ready >= desired && (freshReady || desired < p.desiredReplicas), nil
}

func podIsReady(pod *core.Pod) bool {
	return pod.DeletionTimestamp == nil && pod.Status.Phase == core.PodRunning &&
		slices.ContainsFunc(pod.Status.Conditions, func(condition core.PodCondition) bool {
			return condition.Type == core.PodReady && condition.Status == core.ConditionTrue
		})
}

type disruptionBudgetError struct {
	error
	podName string
}

func (e disruptionBudgetError) Unwrap() error { return e.error }

func (e disruptionBudgetError) Error() string {
	return fmt.Sprintf("cannot evict pod %s as it would violate the pod's disruption budget", e.podName)
}

func evictOrRollout(ctx context.Context, wl k8sapi.Workload, pod *core.Pod) (podEvictionResult, error) {
	if wl != nil {
		previousUID := wl.GetUID()
		refreshed, err := refreshWorkload(ctx, wl)
		if err != nil {
			return podEvictionGone, err
		}
		wl = refreshed
		if (previousUID != "" && wl.GetUID() != previousUID) || k8sapi.DesiredReplicas(wl) == 0 || workloadRolloutInProgress(wl) {
			// Do not consume disruption budget while another rollout is already replacing pods.
			clog.Debugf(ctx, "Deferring eviction of %s because %s is already updating", pod.Name, wl)
			return podEvictionDeferred, nil
		}
	}

	evicted, err := evictPod(ctx, pod)
	if err == nil {
		if !evicted {
			return podEvictionGone, nil
		}
		return podEvictionAccepted, nil
	}
	if wl == nil || !errors.As(err, &disruptionBudgetError{}) {
		return podEvictionGone, fmt.Errorf("failed to evict pod %s: %w", pod.Name, err)
	}
	clog.Debug(ctx, err.Error())
	refreshedWorkload, refreshErr := refreshWorkload(ctx, wl)
	if refreshErr != nil {
		return podEvictionGone, refreshErr
	}
	if wl.GetUID() != "" && refreshedWorkload.GetUID() != wl.GetUID() {
		return podEvictionDeferred, nil
	}
	wl = refreshedWorkload
	if workloadRolloutInProgress(wl) {
		// A previous restart patch or an unrelated workload update is already replacing these pods.
		// Patching restartedAt again resets slow rollouts and can keep them permanently below their
		// disruption budget. Informer reconciliation will retry once the update finishes.
		clog.Debugf(ctx, "Deferring eviction of %s because %s is already updating", pod.Name, wl)
		return podEvictionDeferred, nil
	}
	switch wl.GetKind() {
	case k8sapi.StatefulSetKind, k8sapi.ReplicaSetKind:
		if err := triggerScalingEviction(ctx, wl, pod); err != nil {
			return podEvictionGone, err
		}
	default:
		clog.Debugf(ctx, "Patching %s to trigger pod recreation", wl)
		restartAnnotation := generateRestartAnnotationPatch(wl.GetPodTemplate().Annotations)
		if err := wl.Patch(ctx, types.JSONPatchType, []byte(restartAnnotation)); err != nil {
			return podEvictionGone, fmt.Errorf("unable to patch %s: %w", wl, err)
		}
		clog.Debugf(ctx, "Successfully patched %s", wl)
	}
	// Rollout applies to all pods for the workload, so we're done here
	return podEvictionStarted, nil
}

func workloadUpdateInProgress(wl k8sapi.Workload) bool {
	return !wl.Updated(wl.GetGeneration())
}

func workloadRolloutInProgress(wl k8sapi.Workload) bool {
	desiredReplicas := k8sapi.DesiredReplicas(wl)
	switch wl.GetKind() {
	case k8sapi.DeploymentKind:
		deployment, ok := k8sapi.DeploymentImpl(wl)
		if !ok {
			break
		}
		if deployment.Status.ObservedGeneration != deployment.Generation ||
			deployment.Status.Replicas != desiredReplicas ||
			deployment.Status.UpdatedReplicas != desiredReplicas {
			return true
		}
		for _, condition := range deployment.Status.Conditions {
			if condition.Type == apps.DeploymentProgressing && condition.Status == core.ConditionTrue {
				return condition.Reason != "NewReplicaSetAvailable"
			}
		}
		return false
	case k8sapi.RolloutKind:
		rollout, ok := k8sapi.RolloutImpl(wl)
		if !ok {
			break
		}
		return rollout.Status.ObservedGeneration != strconv.FormatInt(rollout.Generation, 10) ||
			rollout.Status.Replicas != desiredReplicas ||
			rollout.Status.UpdatedReplicas != desiredReplicas ||
			rollout.Status.ReadyReplicas != desiredReplicas ||
			rollout.Status.AvailableReplicas != desiredReplicas ||
			rollout.Status.Phase != argoRollouts.RolloutPhaseHealthy
	case k8sapi.ReplicaSetKind:
		replicaSet, ok := k8sapi.ReplicaSetImpl(wl)
		if !ok {
			break
		}
		return replicaSet.Status.ObservedGeneration != replicaSet.Generation ||
			replicaSet.Status.Replicas != desiredReplicas
	case k8sapi.StatefulSetKind:
		statefulSet, ok := k8sapi.StatefulSetImpl(wl)
		if !ok {
			break
		}
		if statefulSet.Status.ObservedGeneration != statefulSet.Generation ||
			statefulSet.Status.Replicas != desiredReplicas {
			return true
		}
		if statefulSet.Spec.UpdateStrategy.Type == apps.OnDeleteStatefulSetStrategyType {
			return false
		}
		expectedUpdatedReplicas := desiredReplicas
		if rollingUpdate := statefulSet.Spec.UpdateStrategy.RollingUpdate; rollingUpdate != nil && rollingUpdate.Partition != nil {
			expectedUpdatedReplicas = max(0, desiredReplicas-*rollingUpdate.Partition)
		}
		return statefulSet.Status.UpdatedReplicas < expectedUpdatedReplicas
	}
	return workloadUpdateInProgress(wl)
}

func refreshWorkload(ctx context.Context, wl k8sapi.Workload) (k8sapi.Workload, error) {
	var refreshed k8sapi.Workload
	err := backoff.Retry(func() error {
		var refreshErr error
		refreshed, refreshErr = k8sapi.GetWorkload(ctx, wl.GetName(), wl.GetNamespace(), wl.GetKind())
		if refreshErr != nil && workloadRefreshErrorIsTerminal(refreshErr) {
			return backoff.Permanent(refreshErr)
		}
		return refreshErr
	}, backoff.WithContext(backoff.WithMaxRetries(backoff.NewConstantBackOff(200*time.Millisecond), 10), ctx))
	if err != nil {
		return nil, fmt.Errorf("unable to refresh %s before pod eviction: %w", wl, err)
	}
	return refreshed, nil
}

func workloadRefreshErrorIsTerminal(err error) bool {
	return k8sErrors.IsNotFound(err) ||
		k8sErrors.IsForbidden(err) ||
		k8sErrors.IsUnauthorized(err) ||
		k8sErrors.IsBadRequest(err) ||
		k8sErrors.IsInvalid(err) ||
		k8sErrors.IsMethodNotSupported(err)
}

func retryEvictPod(ctx context.Context, wl k8sapi.Workload, pod *core.Pod, replicas int) error {
	err := waitForReplicaCount(ctx, wl, replicas)
	if err != nil {
		clog.Error(ctx, err)
		return err
	}
	for {
		_, err = evictPod(ctx, pod)
		if err == nil || !errors.As(err, &disruptionBudgetError{}) {
			clog.Error(ctx, err)
			return err
		}
		delay := 2 * time.Second
		clog.Debugf(ctx, "%v. Will retry in %s", err, delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// generateRestartAnnotationPatch generates a JSON patch that adds or updates the annotation
// We need to use this particular patch type because argo-rollouts do not support strategic merge patches.
func generateRestartAnnotationPatch(anns map[string]string) string {
	basePointer := "/spec/template/metadata/annotations"
	pointer := fmt.Sprintf(
		basePointer+"/%s",
		strings.ReplaceAll(annotation.RestartedAt, "/", "~1"),
	)

	if _, ok := anns[annotation.RestartedAt]; ok {
		return fmt.Sprintf(
			`[{"op": "replace", "path": "%s", "value": "%s"}]`, pointer, time.Now().Format(time.RFC3339),
		)
	}

	if len(anns) == 0 {
		return fmt.Sprintf(
			`[{"op": "add", "path": "%s", "value": {}}, {"op": "add", "path": "%s", "value": "%s"}]`, basePointer, pointer, time.Now().Format(time.RFC3339),
		)
	}

	return fmt.Sprintf(
		`[{"op": "add", "path": "%s", "value": "%s"}]`, pointer, time.Now().Format(time.RFC3339),
	)
}

func waitForReplicaCount(ctx context.Context, wl k8sapi.Workload, count int) error {
	for {
		pods, err := workloadPods(ctx, wl)
		if err != nil {
			return err
		}
		if len(pods) == count && !slices.ContainsFunc(pods, func(pod *core.Pod) bool { return pod.Status.Phase != core.PodRunning }) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s never scaled to %d", wl, count)
		case <-time.After(2 * time.Second):
		}
	}
}

func waitForWorkloadRecovery(ctx context.Context, wl k8sapi.Workload) error {
	for {
		refreshed, err := refreshWorkload(ctx, wl)
		if err != nil {
			return err
		}
		desiredReplicas := k8sapi.DesiredReplicas(refreshed)
		if k8sapi.ReadyReplicas(refreshed) >= desiredReplicas &&
			!workloadRolloutInProgress(refreshed) {
			return nil
		}
		wl = refreshed
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s did not recover %d ready replicas: %w", wl, desiredReplicas, ctx.Err())
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func scaleIt(ctx context.Context, wl k8sapi.Workload, replicas int) error {
	clog.Debugf(ctx, "Scaling %s to %d replicas", wl, replicas)
	patch := fmt.Sprintf(`{"spec": {"replicas": %d}}`, replicas)
	err := wl.Patch(ctx, types.StrategicMergePatchType, []byte(patch))
	if err != nil {
		err = fmt.Errorf("unable to scale %s to %d: %v", wl, replicas, err)
	}
	return err
}

func triggerScalingEviction(ctx context.Context, wl k8sapi.Workload, pod *core.Pod) (err error) {
	// Rollout of a replicatset/statefulset will not recreate the pods. For that to happen, the
	// set must be scaled up to replicas+1 and then the pod must be evicted before the replicas
	// are scaled back down.
	replicas := wl.Replicas()
	if err := scaleIt(ctx, wl, replicas+1); err != nil {
		return err
	}
	defer func() {
		// Restore the desired count before recovery follows live replica changes. This keeps the
		// temporary scale-up from being mistaken for an externally requested target.
		if restoreErr := scaleIt(context.WithoutCancel(ctx), wl, replicas); restoreErr != nil {
			err = errors.Join(err, restoreErr)
		}
	}()
	return retryEvictPod(ctx, wl, pod, replicas+1)
}

func evictPod(ctx context.Context, pod *core.Pod) (bool, error) {
	clog.Debugf(ctx, "Attempting to evict pod %s", pod.Name)
	uid := pod.UID
	err := k8sapi.GetK8sInterface(ctx).CoreV1().Pods(pod.Namespace).EvictV1(ctx, &v1.Eviction{
		ObjectMeta: meta.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
		DeleteOptions: &meta.DeleteOptions{
			Preconditions: &meta.Preconditions{UID: &uid},
		},
	})
	if err == nil || k8sErrors.IsNotFound(err) {
		store := informer.GetK8sFactory(ctx, pod.Namespace).Core().V1().Pods().Informer().GetStore()
		if cached, exists, getErr := store.Get(pod); getErr == nil && exists {
			if cachedPod, ok := cached.(*core.Pod); ok && cachedPod.UID == pod.UID {
				_ = store.Delete(cachedPod)
			}
		}
		clog.Debugf(ctx, "Pod %s was evicted or already replaced", pod.Name)
		return err == nil, nil
	}
	if k8sErrors.IsConflict(err) {
		// The name now belongs to a different UID. Leave the informer entry intact so the
		// replacement remains visible to subsequent reconciliation.
		clog.Debugf(ctx, "Pod %s was already replaced", pod.Name)
		return false, nil
	}
	if strings.Contains(err.Error(), "disruption budget") {
		err = disruptionBudgetError{error: err, podName: pod.Name}
	}
	return false, err
}

func podIsPendingOrRunning(pod *core.Pod) bool {
	switch pod.Status.Phase {
	case core.PodPending, core.PodRunning:
		return true
	default:
		return false
	}
}

type podLister interface {
	List(selector labels.Selector) (ret []*core.Pod, err error)
}

func getPodLister(ctx context.Context, namespace string) (lister podLister) {
	api := informer.GetK8sFactory(ctx, namespace).Core().V1().Pods().Lister()
	if namespace == "" {
		lister = api
	} else {
		lister = api.Pods(namespace)
	}
	return lister
}

func podList(ctx context.Context, namespace string) (wlPodMap, error) {
	lister := getPodLister(ctx, namespace)
	pods, err := lister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("error listing pods %s: %v", whereWeWatch(namespace), err)
	}
	enabledWorkloads := managerutil.GetEnv(ctx).EnabledWorkloadKinds
	podMap := make(wlPodMap)
	for _, pod := range pods {
		if !podIsPendingOrRunning(pod) {
			continue
		}
		wl, err := podOwnerWorkload(ctx, pod, enabledWorkloads)
		if err != nil {
			if k8sErrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		podMap.add(wl, pod)
	}
	return podMap, nil
}

func liveAgentPodList(ctx context.Context, namespace string) (wlPodMap, error) {
	list, err := k8sapi.GetK8sInterface(ctx).CoreV1().Pods(namespace).List(ctx, meta.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("unable to list physical agent pods %s: %w", whereWeWatch(namespace), err)
	}
	resolver := &liveOwnerResolver{workloads: make(map[WorkloadKey]k8sapi.Workload)}
	enabled := managerutil.GetEnv(ctx).EnabledWorkloadKinds
	podMap := make(wlPodMap)
	for i := range list.Items {
		pod := &list.Items[i]
		if !podIsPendingOrRunning(pod) || pod.Annotations[annotation.Config] == "" || podIsManuallyInjected(ctx, pod) {
			continue
		}
		kinds := enabled
		if kind := k8sapi.Kind(pod.Labels[agentconfig.WorkloadKindLabel]); kind != "" && !kinds.Contains(kind) {
			kinds = append(slices.Clone(kinds), kind)
		}
		wl, err := resolver.owner(ctx, k8sapi.Pod(pod), kinds)
		if k8sErrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		podMap.add(wl, pod)
	}
	return podMap, nil
}

func podOwnerWorkload(ctx context.Context, pod *core.Pod, enabledWorkloads k8sapi.Kinds) (k8sapi.Workload, error) {
	if podKind, ok := pod.Labels[agentconfig.WorkloadKindLabel]; ok {
		if !enabledWorkloads.Contains(k8sapi.Kind(podKind)) {
			// Pod's label indicates a workload kind that has been disabled. As such, it will not be present in the
			// shared informer cache.
			return k8sapi.GetWorkload(ctx, pod.Labels[agentconfig.WorkloadNameLabel], pod.Namespace, k8sapi.Kind(podKind))
		}
		return agentmap.FindOwnerWorkload(ctx, k8sapi.Pod(pod), enabledWorkloads)
	}
	if enabledWorkloads.Contains(k8sapi.RolloutKind) && !enabledWorkloads.Contains(k8sapi.ReplicaSetKind) {
		// Rollout pods are owned by ReplicaSets even when ReplicaSets are not enabled as workloads. Resolve
		// through that intermediary without treating standalone ReplicaSets as eligible.
		for _, ref := range pod.OwnerReferences {
			if ref.Controller == nil || !*ref.Controller || ref.Kind != string(k8sapi.ReplicaSetKind) {
				continue
			}
			rs, err := k8sapi.GetWorkload(ctx, ref.Name, pod.Namespace, k8sapi.ReplicaSetKind)
			if err == nil {
				if owner, err := agentmap.FindOwnerWorkload(ctx, rs, enabledWorkloads); err == nil &&
					enabledWorkloads.Contains(owner.GetKind()) {
					return owner, nil
				}
			}
			break
		}
	}
	return agentmap.FindOwnerWorkload(ctx, k8sapi.Pod(pod), enabledWorkloads)
}

func workloadPods(ctx context.Context, wl k8sapi.Workload) ([]*core.Pod, error) {
	lister := getPodLister(ctx, wl.GetNamespace())
	selector, err := wl.Selector()
	if err != nil {
		return nil, err
	}
	pods, err := lister.List(selector)
	if err != nil {
		return nil, err
	}
	return ownedWorkloadPods(ctx, wl, pods, podOwnerWorkload)
}

func liveWorkloadPods(ctx context.Context, wl k8sapi.Workload) ([]*core.Pod, error) {
	selector, err := wl.Selector()
	if err != nil {
		return nil, err
	}
	if _, selectable := selector.Requirements(); !selectable {
		return nil, nil
	}
	list, err := k8sapi.GetK8sInterface(ctx).CoreV1().Pods(wl.GetNamespace()).List(ctx, meta.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, err
	}
	pods := make([]*core.Pod, len(list.Items))
	for i := range list.Items {
		pods[i] = &list.Items[i]
	}
	resolver := &liveOwnerResolver{workloads: make(map[WorkloadKey]k8sapi.Workload)}
	return ownedWorkloadPods(ctx, wl, pods, func(ctx context.Context, pod *core.Pod, enabledWorkloads k8sapi.Kinds) (k8sapi.Workload, error) {
		if !enabledWorkloads.Contains(wl.GetKind()) {
			enabledWorkloads = append(slices.Clone(enabledWorkloads), wl.GetKind())
		}
		return resolver.ownerFor(ctx, k8sapi.Pod(pod), enabledWorkloads, wl)
	})
}

// PodOwnedByWorkload verifies the current workload through native controller references.
func PodOwnedByWorkload(ctx context.Context, wl k8sapi.Workload, pod *core.Pod) (bool, error) {
	if pod.Namespace != wl.GetNamespace() || wl.GetUID() == "" {
		return false, nil
	}
	kinds := managerutil.GetEnv(ctx).EnabledWorkloadKinds
	if !kinds.Contains(wl.GetKind()) {
		kinds = append(slices.Clone(kinds), wl.GetKind())
	}
	resolver := &liveOwnerResolver{workloads: make(map[WorkloadKey]k8sapi.Workload)}
	owner, err := resolver.ownerFor(ctx, k8sapi.Pod(pod), kinds, wl)
	if err != nil {
		return false, err
	}
	return sameLiveWorkload(owner, wl), nil
}

type liveOwnerResolver struct {
	workloads map[WorkloadKey]k8sapi.Workload
}

type liveOwnerIdentity struct {
	group string
	key   WorkloadKey
	uid   types.UID
}

func liveOwnerGroup(kind k8sapi.Kind) string {
	switch kind {
	case k8sapi.DeploymentKind, k8sapi.ReplicaSetKind, k8sapi.StatefulSetKind:
		return apps.GroupName
	case k8sapi.RolloutKind:
		return argoRolloutsAPI.Group
	default:
		return ""
	}
}

func liveControllerKind(ref meta.OwnerReference) (k8sapi.Kind, bool) {
	if ref.UID == "" {
		return "", false
	}
	version, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil || version.Version == "" {
		return "", false
	}
	kind := k8sapi.Kind(ref.Kind)
	group := liveOwnerGroup(kind)
	return kind, group != "" && version.Group == group
}

func liveCustomController(ref meta.OwnerReference) bool {
	version, err := schema.ParseGroupVersion(ref.APIVersion)
	return ref.UID != "" && err == nil && version.Version != "" && version.Group != "" &&
		version.Group != apps.GroupName && version.Group != argoRolloutsAPI.Group
}

func sameLiveWorkload(actual, wanted k8sapi.Workload) bool {
	return wanted.GetUID() != "" && actual.GetName() == wanted.GetName() && actual.GetNamespace() == wanted.GetNamespace() &&
		actual.GetKind() == wanted.GetKind() && actual.GetUID() == wanted.GetUID()
}

func (r *liveOwnerResolver) workload(ctx context.Context, key WorkloadKey) (k8sapi.Workload, error) {
	if wl := r.workloads[key]; wl != nil {
		return wl, nil
	}
	wl, err := k8sapi.GetWorkload(ctx, key.Name, key.Namespace, key.Kind)
	if err == nil {
		r.workloads[key] = wl
	}
	return wl, err
}

func (r *liveOwnerResolver) owner(ctx context.Context, obj k8sapi.Object, enabledWorkloads k8sapi.Kinds) (k8sapi.Workload, error) {
	return r.ownerFor(ctx, obj, enabledWorkloads, nil)
}

func (r *liveOwnerResolver) ownerFor(ctx context.Context, obj k8sapi.Object, kinds k8sapi.Kinds, wanted k8sapi.Workload) (k8sapi.Workload, error) {
	visited := make(map[liveOwnerIdentity]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		wl, isWorkload := obj.(k8sapi.Workload)
		if isWorkload && wanted != nil && sameLiveWorkload(wl, wanted) {
			return wl, nil
		}
		key := WorkloadKey{Kind: obj.GetKind(), Name: obj.GetName(), Namespace: obj.GetNamespace()}
		identity := liveOwnerIdentity{group: liveOwnerGroup(key.Kind), key: key, uid: obj.GetUID()}
		if _, seen := visited[identity]; seen {
			return nil, fmt.Errorf("cyclic workload controller ownership at %s: %w", key, k8sErrors.NewNotFound(obj.GetGroupResource(), key.Name))
		}
		visited[identity] = struct{}{}
		ref := meta.GetControllerOf(obj)
		if ref == nil {
			if wanted == nil && isWorkload && kinds.Contains(wl.GetKind()) {
				return wl, nil
			}
			return nil, k8sErrors.NewNotFound(obj.GetGroupResource(), obj.GetName())
		}
		kind, valid := liveControllerKind(*ref)
		if !valid {
			if wanted == nil && isWorkload && kinds.Contains(wl.GetKind()) && liveCustomController(*ref) {
				return wl, nil
			}
			return nil, k8sErrors.NewNotFound(obj.GetGroupResource(), obj.GetName())
		}
		if !kinds.Contains(kind) && (kind != k8sapi.ReplicaSetKind || (!kinds.Contains(k8sapi.DeploymentKind) && !kinds.Contains(k8sapi.RolloutKind))) {
			return nil, k8sErrors.NewNotFound(obj.GetGroupResource(), obj.GetName())
		}
		owner, err := r.workload(ctx, WorkloadKey{Kind: kind, Name: ref.Name, Namespace: obj.GetNamespace()})
		if err != nil {
			return nil, err
		}
		if owner.GetUID() != ref.UID {
			return nil, k8sErrors.NewNotFound(owner.GetGroupResource(), ref.Name)
		}
		obj = owner
	}
}

func ownedWorkloadPods(ctx context.Context, wl k8sapi.Workload, pods []*core.Pod,
	resolveOwner func(context.Context, *core.Pod, k8sapi.Kinds) (k8sapi.Workload, error),
) ([]*core.Pod, error) {
	if len(pods) == 0 {
		return nil, nil
	}
	// A selector is not always unique to one workload. Stable and canary
	// Deployments commonly share one and distinguish their pods only by
	// template labels, so verify ownership before evicting a candidate pod.
	enabledWorkloads := managerutil.GetEnv(ctx).EnabledWorkloadKinds
	workloadKey := WorkloadKey{Kind: wl.GetKind(), Name: wl.GetName(), Namespace: wl.GetNamespace()}
	ownedPods := make([]*core.Pod, 0, len(pods))
	for _, pod := range pods {
		if !podIsPendingOrRunning(pod) {
			continue
		}
		owner, err := resolveOwner(ctx, pod, enabledWorkloads)
		if err != nil {
			if k8sErrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		if (WorkloadKey{Kind: owner.GetKind(), Name: owner.GetName(), Namespace: owner.GetNamespace()}) == workloadKey &&
			(wl.GetUID() == "" || owner.GetUID() == wl.GetUID()) {
			ownedPods = append(ownedPods, pod)
		}
	}
	return ownedPods, nil
}
