package mutator

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	argorolloutsfake "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/fake"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

type evictionIntentFixture struct {
	t         *testing.T
	ctx       context.Context
	wl        k8sapi.Workload
	client    *fake.Clientset
	watcher   *configWatcher
	pods      cache.Store
	workloads cache.Store
	evictions chan types.UID
}

func newEvictionIntentFixture(t *testing.T) *evictionIntentFixture {
	t.Helper()
	const namespace = "default"
	wl, obj := reconciliationTestWorkload(k8sapi.StatefulSetKind, "echo", namespace)
	client := fake.NewSimpleClientset(obj)
	evictions := make(chan types.UID, 10)
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		eviction := action.(k8stesting.CreateAction).GetObject().(*policy.Eviction)
		if eviction.DeleteOptions != nil && eviction.DeleteOptions.Preconditions != nil && eviction.DeleteOptions.Preconditions.UID != nil {
			evictions <- *eviction.DeleteOptions.Preconditions.UID
		}
		return true, nil, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	ctx = k8sapi.WithJoinedClientSetInterface(ctx, client, argorolloutsfake.NewSimpleClientset())
	ctx = informer.WithFactory(ctx, "")
	ctx = managerutil.WithEnv(ctx, &managerutil.Env{EnabledWorkloadKinds: k8sapi.Kinds{k8sapi.StatefulSetKind}})
	factory := informer.GetK8sFactory(ctx, namespace)
	workloads := factory.Apps().V1().StatefulSets().Informer().GetStore()
	require.NoError(t, workloads.Add(obj))
	watcher := NewWatcher().(*configWatcher)
	require.NoError(t, watcher.StartWatchers(ctx))
	return &evictionIntentFixture{
		t: t, ctx: ctx, wl: wl, client: client, watcher: watcher,
		pods: factory.Core().V1().Pods().Informer().GetStore(), workloads: workloads, evictions: evictions,
	}
}

func (f *evictionIntentFixture) config(policy agentconfig.ReplacePolicy) (*agentconfig.Sidecar, string) {
	f.t.Helper()
	config := reconciliationTestConfig(f.wl, policy)
	encoded, err := agentconfig.MarshalTight(config)
	require.NoError(f.t, err)
	return config, encoded
}

func (f *evictionIntentFixture) prependReactor(verb, resource string, reactor k8stesting.ReactionFunc) {
	f.t.Helper()
	f.client.Lock()
	f.client.PrependReactor(verb, resource, reactor)
	f.client.Unlock()
}

func (f *evictionIntentFixture) create(pod *core.Pod) {
	f.t.Helper()
	_, err := f.client.CoreV1().Pods(pod.Namespace).Create(f.ctx, pod, meta.CreateOptions{})
	require.NoError(f.t, err)
	require.NoError(f.t, f.pods.Add(pod))
}

func (f *evictionIntentFixture) delete(pod *core.Pod) {
	f.t.Helper()
	require.NoError(f.t, f.client.CoreV1().Pods(pod.Namespace).Delete(f.ctx, pod.Name, meta.DeleteOptions{}))
	require.NoError(f.t, f.pods.Delete(pod))
}

func (f *evictionIntentFixture) ready(pod *core.Pod) {
	f.t.Helper()
	pod = pod.DeepCopy()
	pod.Status.Conditions[0].Status = core.ConditionTrue
	_, err := f.client.CoreV1().Pods(pod.Namespace).Update(f.ctx, pod, meta.UpdateOptions{})
	require.NoError(f.t, err)
	require.NoError(f.t, f.pods.Update(pod))
}

func (f *evictionIntentFixture) prime() *core.Pod {
	f.t.Helper()
	old := reconciliationTestPod(f.wl, "old", "", true)
	f.create(old)
	initial, encoded := f.config(agentconfig.ReplacePolicyIntercept)
	f.watcher.Store(initial)
	require.NoError(f.t, f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, initial))
	require.Equal(f.t, old.UID, readReconciliationEviction(f.t, f.evictions))
	f.delete(old)
	fresh := reconciliationTestPod(f.wl, "fresh", encoded, false)
	f.create(fresh)
	return fresh
}

func (f *evictionIntentFixture) noEviction(duration time.Duration) {
	f.t.Helper()
	select {
	case uid := <-f.evictions:
		f.t.Fatalf("unexpected eviction of %s", uid)
	case <-time.After(duration):
	}
}

func TestDeferredAgentEvictionUsesLatestConfiguration(t *testing.T) {
	for _, uninstallFirst := range []bool{false, true} {
		name := "replace"
		if uninstallFirst {
			name = "uninstall"
		}
		t.Run(name, func(t *testing.T) {
			f := newEvictionIntentFixture(t)
			fresh := f.prime()
			if uninstallFirst {
				f.watcher.Delete(f.wl.GetName(), f.wl.GetNamespace())
				require.NoError(t, f.watcher.EvictPodsWithAgentConfig(f.ctx, f.wl))
			} else {
				intermediate, _ := f.config(agentconfig.ReplacePolicyInactive)
				f.watcher.Store(intermediate)
				require.NoError(t, f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, intermediate))
			}
			latest, encoded := f.config(agentconfig.ReplacePolicyContainer)
			f.watcher.Store(latest)
			f.noEviction(75 * time.Millisecond)
			f.ready(fresh)
			require.Equal(t, fresh.UID, readReconciliationEviction(t, f.evictions))
			f.delete(fresh)
			correct := reconciliationTestPod(f.wl, "latest", encoded, true)
			f.create(correct)
			f.noEviction(1500 * time.Millisecond)
			require.Eventually(t, func() bool {
				_, pending := f.watcher.evictionRequests.Load(evictionKey(f.wl))
				return !pending
			}, 3*time.Second, 10*time.Millisecond)
		})
	}
}

func TestDeferredAgentEvictionDoesNotCrossWorkloadUID(t *testing.T) {
	f := newEvictionIntentFixture(t)
	fresh := f.prime()
	replacement, encoded := f.config(agentconfig.ReplacePolicyContainer)
	f.watcher.Store(replacement)
	require.NoError(t, f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, replacement))
	f.delete(fresh)
	oldObject, ok := k8sapi.StatefulSetImpl(f.wl)
	require.True(t, ok)
	newObject := oldObject.DeepCopy()
	newObject.UID = "new-workload-uid"
	_, err := f.client.AppsV1().StatefulSets(newObject.Namespace).Update(f.ctx, newObject, meta.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, f.workloads.Update(newObject))
	newWL := k8sapi.StatefulSet(newObject)
	newPod := reconciliationTestPod(newWL, "recreated", encoded, true)
	f.create(newPod)
	require.NoError(t, f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, newWL, replacement))
	require.NoError(t, f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, replacement), "a stale older RPC must not change the recreated workload's current request")
	f.noEviction(1500 * time.Millisecond)
	request, pending := f.watcher.evictionRequests.Load(evictionKey(newWL))
	if pending {
		request.Lock()
		require.Equal(t, newWL.GetUID(), request.uid)
		request.Unlock()
	}
}

func TestEvictionPodEventsRequireRecordedOwnedIdentity(t *testing.T) {
	watcher := NewWatcher().(*configWatcher)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[WorkloadKey](time.Second, time.Minute))
	t.Cleanup(queue.ShutDown)
	watcher.evictionQueue = queue
	wl, _ := reconciliationTestWorkload(k8sapi.StatefulSetKind, "echo", "default")
	key := evictionKey(wl)
	watcher.evictionRequests.Store(key, &workloadEvictionRequest{uid: wl.GetUID(), recorded: true})
	pod := reconciliationTestPod(wl, "pod", "", false)
	foreign, _ := reconciliationTestWorkload(k8sapi.StatefulSetKind, "foreign", "default")
	watcher.enqueueEvictionPod(reconciliationTestPod(foreign, "pod", "", true))
	watcher.enqueueEvictionPod(&apps.StatefulSet{})
	wrongUID := pod.DeepCopy()
	wrongUID.OwnerReferences[0].UID = "stale-workload-uid"
	watcher.enqueueEvictionPod(wrongUID)
	watcher.enqueueChangedEvictionPod(pod, pod.DeepCopy())
	require.Zero(t, queue.Len())

	ready := pod.DeepCopy()
	ready.Status.Conditions[0].Status = core.ConditionTrue
	watcher.enqueueChangedEvictionPod(pod, ready)
	dequeueEvictionIntent(t, queue, key)
	terminating := ready.DeepCopy()
	now := meta.Now()
	terminating.DeletionTimestamp = &now
	watcher.enqueueChangedEvictionPod(ready, terminating)
	dequeueEvictionIntent(t, queue, key)
	watcher.enqueueEvictionPod(cache.DeletedFinalStateUnknown{Key: "default/echo-0", Obj: terminating})
	dequeueEvictionIntent(t, queue, key)
	require.Zero(t, queue.Len())
}

func TestDeferredAgentEvictionRetriesTransientWorkloadErrors(t *testing.T) {
	f := newEvictionIntentFixture(t)
	fresh := f.prime()
	replacement, _ := f.config(agentconfig.ReplacePolicyContainer)
	f.watcher.Store(replacement)
	require.NoError(t, f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, replacement))
	var failures atomic.Int32
	f.prependReactor("get", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
		if failures.Add(1) <= 2 {
			return true, nil, k8sErrors.NewServiceUnavailable("temporary API failure")
		}
		return false, nil, nil
	})
	f.ready(fresh)
	select {
	case uid := <-f.evictions:
		require.Equal(t, fresh.UID, uid)
	case <-time.After(8 * time.Second):
		t.Fatal("the deferred eviction did not recover from transient workload errors")
	}
	require.GreaterOrEqual(t, failures.Load(), int32(3))
}

func TestFirstAgentEvictionRequestSurvivesTransientPhysicalWorkloadLookup(t *testing.T) {
	for _, uninstall := range []bool{false, true} {
		name := "replace"
		if uninstall {
			name = "uninstall"
		}
		t.Run(name, func(t *testing.T) {
			f := newEvictionIntentFixture(t)
			initial, encoded := f.config(agentconfig.ReplacePolicyIntercept)
			old := reconciliationTestPod(f.wl, "old", encoded, true)
			f.create(old)
			f.watcher.Store(initial)
			var attempts atomic.Int32
			f.prependReactor("get", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
				if attempts.Add(1) == 1 {
					return true, nil, k8sErrors.NewServiceUnavailable("first workload lookup failed")
				}
				return false, nil, nil
			})
			var err error
			if uninstall {
				f.watcher.Delete(f.wl.GetName(), f.wl.GetNamespace())
				err = f.watcher.EvictPodsWithAgentConfig(f.ctx, f.wl)
			} else {
				desired, _ := f.config(agentconfig.ReplacePolicyContainer)
				f.watcher.Store(desired)
				err = f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, desired)
			}
			require.True(t, k8sErrors.IsServiceUnavailable(err), "the original caller still receives its immediate API error: %v", err)
			require.Equal(t, old.UID, readReconciliationEviction(t, f.evictions),
				"the first known-UID intent must retry without another request or Pod event")
			require.GreaterOrEqual(t, attempts.Load(), int32(2))
		})
	}
}

func TestFirstAgentEvictionRequestDoesNotRetryPermanentOrUnknownIdentity(t *testing.T) {
	for _, known := range []bool{false, true} {
		name := "unknown UID"
		if known {
			name = "known UID forbidden"
		}
		t.Run(name, func(t *testing.T) {
			f := newEvictionIntentFixture(t)
			old := reconciliationTestPod(f.wl, "old", "earlier", true)
			f.create(old)
			desired, _ := f.config(agentconfig.ReplacePolicyContainer)
			f.watcher.Store(desired)
			requestWorkload := f.wl
			if !known {
				object, ok := k8sapi.StatefulSetImpl(f.wl)
				require.True(t, ok)
				unidentified := object.DeepCopy()
				unidentified.UID = ""
				requestWorkload = k8sapi.StatefulSet(unidentified)
			}
			var attempts atomic.Int32
			f.prependReactor("get", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
				attempts.Add(1)
				if known {
					return true, nil, k8sErrors.NewForbidden(apps.Resource("statefulsets"), "echo", fmt.Errorf("denied"))
				}
				return true, nil, k8sErrors.NewServiceUnavailable("unknown workload read failed")
			})
			err := f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, requestWorkload, desired)
			if known {
				require.True(t, k8sErrors.IsForbidden(err), "%v", err)
			} else {
				require.True(t, k8sErrors.IsServiceUnavailable(err), "%v", err)
			}
			f.noEviction(1250 * time.Millisecond)
			require.Equal(t, int32(1), attempts.Load())
			_, recorded := f.watcher.evictionRequests.Load(evictionKey(f.wl))
			require.False(t, recorded)
			require.Same(t, desired, f.watcher.Get(f.wl.GetName(), f.wl.GetNamespace()))
		})
	}
}

func TestFirstAgentEvictionRequestWithStaleUIDCannotEvictRecreatedWorkload(t *testing.T) {
	f := newEvictionIntentFixture(t)
	object, ok := k8sapi.StatefulSetImpl(f.wl)
	require.True(t, ok)
	recreated := object.DeepCopy()
	recreated.UID = "new-native-workload"
	_, err := f.client.AppsV1().StatefulSets(recreated.Namespace).Update(f.ctx, recreated, meta.UpdateOptions{})
	require.NoError(t, err)
	newWorkload := k8sapi.StatefulSet(recreated)
	pod := reconciliationTestPod(newWorkload, "new", "earlier", true)
	f.create(pod)
	desired, _ := f.config(agentconfig.ReplacePolicyContainer)
	f.watcher.Store(desired)
	var attempts atomic.Int32
	f.prependReactor("get", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
		if attempts.Add(1) == 1 {
			return true, nil, k8sErrors.NewServiceUnavailable("first workload lookup failed")
		}
		return false, nil, nil
	})
	err = f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, desired)
	require.True(t, k8sErrors.IsServiceUnavailable(err), "%v", err)
	require.Eventually(t, func() bool { return attempts.Load() >= 2 }, 3*time.Second, 10*time.Millisecond)
	f.noEviction(250 * time.Millisecond)
	_, recorded := f.watcher.evictionRequests.Load(evictionKey(f.wl))
	require.False(t, recorded)
}

func TestFirstStaleAgentEvictionErrorCannotOverwriteNewerRecordedWorkload(t *testing.T) {
	f := newEvictionIntentFixture(t)
	desired, encoded := f.config(agentconfig.ReplacePolicyContainer)
	f.watcher.Store(desired)
	key := evictionKey(f.wl)
	newer := &workloadEvictionRequest{uid: "new-native-workload", configJSON: encoded, recorded: true}
	f.watcher.evictionRequests.Store(key, newer)
	f.prependReactor("get", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8sErrors.NewServiceUnavailable("stale caller first workload lookup failed")
	})
	err := f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, desired)
	require.True(t, k8sErrors.IsServiceUnavailable(err), "%v", err)
	retained, exists := f.watcher.evictionRequests.Load(key)
	require.True(t, exists)
	require.Same(t, newer, retained)
	retained.Lock()
	uid, config := retained.uid, retained.configJSON
	retained.Unlock()
	require.Equal(t, types.UID("new-native-workload"), uid)
	require.Equal(t, encoded, config)
	f.noEviction(75 * time.Millisecond)
}

func TestDeferredAgentEvictionDoesNotLoopOnForbiddenWorkload(t *testing.T) {
	f := newEvictionIntentFixture(t)
	fresh := f.prime()
	replacement, _ := f.config(agentconfig.ReplacePolicyContainer)
	f.watcher.Store(replacement)
	require.NoError(t, f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, replacement))
	var forbidden atomic.Bool
	var deniedReads atomic.Int32
	forbidden.Store(true)
	f.prependReactor("get", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
		if forbidden.Load() {
			deniedReads.Add(1)
			return true, nil, k8sErrors.NewForbidden(apps.Resource("statefulsets"), "echo", fmt.Errorf("permission denied"))
		}
		return false, nil, nil
	})
	f.ready(fresh)
	err := f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, replacement)
	require.True(t, k8sErrors.IsForbidden(err), "the synchronous caller must see its permission error: %v", err)
	require.Eventually(t, func() bool { return deniedReads.Load() >= 2 }, 3*time.Second, 10*time.Millisecond)
	reads := deniedReads.Load()
	f.noEviction(1500 * time.Millisecond)
	require.Equal(t, reads, deniedReads.Load(), "permanent denial must not run an automatic error retry loop")
	forbidden.Store(false)
	f.watcher.enqueueEvictionPod(fresh)
	require.Equal(t, fresh.UID, readReconciliationEviction(t, f.evictions), "a later relevant physical pod event may retry the recorded intent")
}

func TestDeferredAgentEvictionDoesNotDisruptManuallyInjectedPod(t *testing.T) {
	f := newEvictionIntentFixture(t)
	fresh := f.prime()
	manual := fresh.DeepCopy()
	manual.Annotations[annotation.ManuallyInjected] = "true"
	manual.Status.Conditions[0].Status = core.ConditionTrue
	_, err := f.client.CoreV1().Pods(manual.Namespace).Update(f.ctx, manual, meta.UpdateOptions{})
	require.NoError(t, err)
	require.NoError(t, f.pods.Update(manual))
	replacement, _ := f.config(agentconfig.ReplacePolicyContainer)
	f.watcher.Store(replacement)
	require.NoError(t, f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, replacement))
	f.noEviction(1500 * time.Millisecond)
	require.Eventually(t, func() bool {
		_, pending := f.watcher.evictionRequests.Load(evictionKey(f.wl))
		return !pending
	}, 3*time.Second, 10*time.Millisecond)
}

func TestAgentEvictionSurvivesEmptyInformerWhenOwnedPhysicalPodExists(t *testing.T) {
	f := newEvictionIntentFixture(t)
	foreignWL, foreignObject := reconciliationTestWorkload(k8sapi.StatefulSetKind, "foreign", "default")
	foreignNative, ok := foreignObject.(*apps.StatefulSet)
	require.True(t, ok)
	_, err := f.client.AppsV1().StatefulSets("default").Create(f.ctx, foreignNative, meta.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, f.workloads.Add(foreignObject))
	for _, pod := range []*core.Pod{
		reconciliationTestPod(f.wl, "owned-native-only", "", true),
		reconciliationTestPod(foreignWL, "foreign-native-only", "", true),
	} {
		_, err = f.client.CoreV1().Pods("default").Create(f.ctx, pod, meta.CreateOptions{})
		require.NoError(t, err)
	}
	desired, _ := f.config(agentconfig.ReplacePolicyContainer)
	f.watcher.Store(desired)
	require.NoError(t, f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, desired))
	require.Equal(t, types.UID("echo-owned-native-only"), readReconciliationEviction(t, f.evictions),
		"a physical owned pod omitted from the informer must be evicted without another request or event")
	f.noEviction(75 * time.Millisecond)
}

func TestAgentEvictionDoesNotClaimCompletionWhenPhysicalListIsUnknown(t *testing.T) {
	f := newEvictionIntentFixture(t)
	desired, _ := f.config(agentconfig.ReplacePolicyContainer)
	f.watcher.Store(desired)
	f.prependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8sErrors.NewServiceUnavailable("physical Pod list unavailable")
	})
	err := f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, desired)
	require.True(t, k8sErrors.IsServiceUnavailable(err), "informer absence and a physical list error are not completion: %v", err)
	_, recorded := f.watcher.evictionRequests.Load(evictionKey(f.wl))
	require.True(t, recorded, "uncertain ownership state must preserve the request")
	f.noEviction(75 * time.Millisecond)
}

func TestAllAgentsUninstallFindsOwnedPhysicalPodsWithEmptyInformer(t *testing.T) {
	f := newEvictionIntentFixture(t)
	initial, encoded := f.config(agentconfig.ReplacePolicyIntercept)
	f.watcher.Store(initial)
	owned := reconciliationTestPod(f.wl, "owned-native-only", encoded, true)
	_, err := f.client.CoreV1().Pods(owned.Namespace).Create(f.ctx, owned, meta.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, f.watcher.EvictAllPodsWithAgentConfig(f.ctx, owned.Namespace))
	require.Equal(t, owned.UID, readReconciliationEviction(t, f.evictions), "all-agents uninstall must enumerate native physical pods if the informer has no entries")
}

func TestAgentEvictionIgnoresControllerlessPhysicalPodWithWorkloadLabels(t *testing.T) {
	for _, allAgents := range []bool{false, true} {
		name := "replace"
		if allAgents {
			name = "uninstall all agents"
		}
		t.Run(name, func(t *testing.T) {
			f := newEvictionIntentFixture(t)
			old, encoded := f.config(agentconfig.ReplacePolicyIntercept)
			orphan := reconciliationTestPod(f.wl, "orphan", encoded, true)
			orphan.OwnerReferences = nil
			_, err := f.client.CoreV1().Pods(orphan.Namespace).Create(f.ctx, orphan, meta.CreateOptions{})
			require.NoError(t, err)
			if allAgents {
				f.watcher.Store(old)
				require.NoError(t, f.watcher.EvictAllPodsWithAgentConfig(f.ctx, orphan.Namespace))
			} else {
				desired, _ := f.config(agentconfig.ReplacePolicyContainer)
				f.watcher.Store(desired)
				require.NoError(t, f.watcher.EvictPodsWithAgentConfigMismatch(f.ctx, f.wl, desired))
			}
			f.noEviction(100 * time.Millisecond)
		})
	}
}

func TestEvictionPodEventIgnoresControllerlessPodWithWorkloadLabels(t *testing.T) {
	watcher := NewWatcher().(*configWatcher)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[WorkloadKey](time.Second, time.Minute))
	t.Cleanup(queue.ShutDown)
	watcher.evictionQueue = queue
	wl, _ := reconciliationTestWorkload(k8sapi.StatefulSetKind, "echo", "default")
	key := evictionKey(wl)
	watcher.evictionRequests.Store(key, &workloadEvictionRequest{uid: wl.GetUID(), recorded: true})
	orphan := reconciliationTestPod(wl, "orphan", "", true)
	orphan.OwnerReferences = nil
	watcher.enqueueEvictionPod(orphan)
	require.Zero(t, queue.Len(), "Pod labels without a native controller cannot establish workload ownership")
}

func TestEvictionPodEventIgnoresForeignGroupAndMissingControllerUID(t *testing.T) {
	for _, apiVersion := range []string{"foreign.example/v1", "", "apps/v1"} {
		t.Run(apiVersion, func(t *testing.T) {
			watcher := NewWatcher().(*configWatcher)
			queue := workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[WorkloadKey](time.Second, time.Minute))
			t.Cleanup(queue.ShutDown)
			watcher.evictionQueue = queue
			wl, _ := reconciliationTestWorkload(k8sapi.StatefulSetKind, "echo", "default")
			key := evictionKey(wl)
			watcher.evictionRequests.Store(key, &workloadEvictionRequest{uid: wl.GetUID(), recorded: true})
			pod := reconciliationTestPod(wl, "pod", "", true)
			pod.OwnerReferences[0].APIVersion = apiVersion
			if apiVersion == "apps/v1" {
				pod.OwnerReferences[0].UID = ""
			}
			watcher.enqueueEvictionPod(pod)
			require.Zero(t, queue.Len())
		})
	}
}

func dequeueEvictionIntent(t *testing.T, queue workqueue.TypedRateLimitingInterface[WorkloadKey], want WorkloadKey) {
	t.Helper()
	require.Equal(t, 1, queue.Len())
	key, shutdown := queue.Get()
	require.False(t, shutdown)
	require.Equal(t, want, key)
	queue.Forget(key)
	queue.Done(key)
}
