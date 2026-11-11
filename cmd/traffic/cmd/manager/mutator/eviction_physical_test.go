package mutator

import (
	"context"
	"testing"
	"time"

	argorolloutsfake "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/fake"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func TestUnannotatedAgentRequestSurvivesAcceptedInjection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		kind      k8sapi.Kind
		uninstall bool
	}{
		{name: "statefulset-replace", kind: k8sapi.StatefulSetKind},
		{name: "headless-statefulset-replace", kind: k8sapi.StatefulSetKind},
		{name: "deployment-uninstall", kind: k8sapi.DeploymentKind, uninstall: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const namespace = "default"
			wl, obj := reconciliationTestWorkload(tc.kind, "echo", namespace)
			foreignWl, foreignObj := reconciliationTestWorkload(tc.kind, "foreign", namespace)
			initial := reconciliationTestConfig(wl, agentconfig.ReplacePolicyIntercept)
			initialJSON, err := agentconfig.MarshalTight(initial)
			require.NoError(t, err)
			old := reconciliationTestPod(wl, "old", "", true)
			foreign := reconciliationTestPod(foreignWl, "foreign", initialJSON, true)
			client := fake.NewSimpleClientset(obj, foreignObj, old, foreign)
			evicted := make(chan types.UID, 8)
			client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "eviction" {
					return false, nil, nil
				}
				eviction := action.(k8stesting.CreateAction).GetObject().(*policy.Eviction)
				if eviction.DeleteOptions == nil || eviction.DeleteOptions.Preconditions == nil || eviction.DeleteOptions.Preconditions.UID == nil {
					return true, nil, nil
				}
				evicted <- *eviction.DeleteOptions.Preconditions.UID
				return true, nil, nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = k8sapi.WithJoinedClientSetInterface(ctx, client, argorolloutsfake.NewSimpleClientset())
			ctx = informer.WithFactory(ctx, "")
			ctx = managerutil.WithEnv(ctx, &managerutil.Env{EnabledWorkloadKinds: k8sapi.Kinds{tc.kind}})
			factory := informer.GetK8sFactory(ctx, namespace)
			var workloadStore cache.Store
			if tc.kind == k8sapi.StatefulSetKind {
				workloadStore = factory.Apps().V1().StatefulSets().Informer().GetStore()
			} else {
				workloadStore = factory.Apps().V1().Deployments().Informer().GetStore()
			}
			require.NoError(t, workloadStore.Add(obj))
			require.NoError(t, workloadStore.Add(foreignObj))
			podStore := factory.Core().V1().Pods().Informer().GetStore()
			require.NoError(t, podStore.Add(old))
			require.NoError(t, podStore.Add(foreign))
			cw := NewWatcher().(*configWatcher)
			require.NoError(t, cw.StartWatchers(ctx))
			cw.Store(initial)
			require.NoError(t, cw.EvictPodsWithAgentConfigMismatch(ctx, wl, initial))
			require.Equal(t, old.UID, readReconciliationEviction(t, evicted))
			pods := client.CoreV1().Pods(namespace)
			require.NoError(t, pods.Delete(ctx, old.Name, meta.DeleteOptions{}))
			fresh := reconciliationTestPod(wl, "fresh", initialJSON, false)
			_, err = pods.Create(ctx, fresh, meta.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, podStore.Add(fresh))
			if tc.uninstall {
				cw.Delete(wl.GetName(), wl.GetNamespace())
				require.NoError(t, cw.EvictPodsWithAgentConfig(ctx, wl))
			} else {
				updated := reconciliationTestConfig(wl, agentconfig.ReplacePolicyContainer)
				cw.Store(updated)
				require.NoError(t, cw.EvictPodsWithAgentConfigMismatch(ctx, wl, updated))
			}
			select {
			case uid := <-evicted:
				t.Fatalf("evicted %s before the initially injected pod was Kubernetes Ready", uid)
			case <-time.After(75 * time.Millisecond):
			}
			fresh = fresh.DeepCopy()
			fresh.Status.Conditions[0].Status = core.ConditionTrue
			_, err = pods.Update(ctx, fresh, meta.UpdateOptions{})
			require.NoError(t, err)
			require.NoError(t, podStore.Update(fresh))
			require.Equal(t, fresh.UID, readReconciliationEviction(t, evicted), "the original request must resume without a second CLI/RPC call or Pod event")
			select {
			case uid := <-evicted:
				t.Fatalf("unexpected eviction of %s, including foreign workload", uid)
			case <-time.After(75 * time.Millisecond):
			}
		})
	}
}

func readReconciliationEviction(t *testing.T, evicted <-chan types.UID) types.UID {
	t.Helper()
	select {
	case uid := <-evicted:
		return uid
	case <-time.After(3 * time.Second):
		t.Fatal("no physical pod eviction received")
		return ""
	}
}

func reconciliationTestWorkload(kind k8sapi.Kind, name, namespace string) (k8sapi.Workload, runtime.Object) {
	replicas := int32(1)
	selector := &meta.LabelSelector{MatchLabels: map[string]string{"app": "echo"}}
	objectMeta := meta.ObjectMeta{Name: name, Namespace: namespace, Generation: 1, UID: types.UID(name + "-workload")}
	if kind == k8sapi.StatefulSetKind {
		sts := &apps.StatefulSet{
			ObjectMeta: objectMeta,
			Spec:       apps.StatefulSetSpec{Replicas: &replicas, Selector: selector, ServiceName: name, UpdateStrategy: apps.StatefulSetUpdateStrategy{Type: apps.RollingUpdateStatefulSetStrategyType}},
			Status:     apps.StatefulSetStatus{ObservedGeneration: 1, Replicas: 1, CurrentReplicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, CurrentRevision: "v1", UpdateRevision: "v1"},
		}
		return k8sapi.StatefulSet(sts), sts
	}
	deploy := &apps.Deployment{
		ObjectMeta: objectMeta,
		Spec:       apps.DeploymentSpec{Replicas: &replicas, Selector: selector},
		Status:     apps.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
	}
	return k8sapi.Deployment(deploy), deploy
}

func reconciliationTestPod(wl k8sapi.Workload, identity, config string, ready bool) *core.Pod {
	controller := true
	name := wl.GetName() + "-" + identity
	if wl.GetKind() == k8sapi.StatefulSetKind {
		name = wl.GetName() + "-0"
	}
	status := core.ConditionFalse
	if ready {
		status = core.ConditionTrue
	}
	return &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name: name, Namespace: wl.GetNamespace(), UID: types.UID(wl.GetName() + "-" + identity),
			Labels:          map[string]string{"app": "echo", agentconfig.WorkloadNameLabel: wl.GetName(), agentconfig.WorkloadKindLabel: string(wl.GetKind())},
			Annotations:     map[string]string{annotation.Config: config},
			OwnerReferences: []meta.OwnerReference{{APIVersion: "apps/v1", Kind: string(wl.GetKind()), Name: wl.GetName(), UID: wl.GetUID(), Controller: &controller}},
		},
		Status: core.PodStatus{Phase: core.PodRunning, Conditions: []core.PodCondition{{Type: core.PodReady, Status: status}}},
	}
}

func reconciliationTestConfig(wl k8sapi.Workload, policy agentconfig.ReplacePolicy) *agentconfig.Sidecar {
	return &agentconfig.Sidecar{
		AgentName: wl.GetName(), Namespace: wl.GetNamespace(), WorkloadName: wl.GetName(), WorkloadKind: wl.GetKind(),
		Containers: []*agentconfig.Container{{Name: wl.GetName(), Replace: policy}},
	}
}
