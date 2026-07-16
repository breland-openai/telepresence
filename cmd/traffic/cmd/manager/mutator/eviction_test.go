package mutator

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	argorolloutsfake "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8sTesting "k8s.io/client-go/testing"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func TestWorkloadUpdateInProgress(t *testing.T) {
	replicas := int32(6)
	deployment := &apps.Deployment{
		ObjectMeta: meta.ObjectMeta{Generation: 12},
		Spec:       apps.DeploymentSpec{Replicas: &replicas},
		Status: apps.DeploymentStatus{
			ObservedGeneration: 12,
			Replicas:           6,
			UpdatedReplicas:    6,
			AvailableReplicas:  6,
		},
	}

	assert.False(t, workloadUpdateInProgress(k8sapi.Deployment(deployment)))

	deployment.Status.Replicas = 8
	deployment.Status.UpdatedReplicas = 3
	deployment.Status.AvailableReplicas = 5
	assert.True(t, workloadUpdateInProgress(k8sapi.Deployment(deployment)))

	deployment.Status.ObservedGeneration = 11
	deployment.Status.Replicas = 6
	deployment.Status.UpdatedReplicas = 6
	deployment.Status.AvailableReplicas = 6
	assert.True(t, workloadUpdateInProgress(k8sapi.Deployment(deployment)))
}

func TestWorkloadRolloutInProgress(t *testing.T) {
	replicas := int32(6)
	deployment := &apps.Deployment{
		ObjectMeta: meta.ObjectMeta{Generation: 12},
		Spec:       apps.DeploymentSpec{Replicas: &replicas},
		Status: apps.DeploymentStatus{
			ObservedGeneration: 12,
			Replicas:           6,
			UpdatedReplicas:    6,
			AvailableReplicas:  5,
			Conditions: []apps.DeploymentCondition{{
				Type:   apps.DeploymentProgressing,
				Status: core.ConditionTrue,
				Reason: "NewReplicaSetAvailable",
			}},
		},
	}
	assert.False(t, workloadRolloutInProgress(k8sapi.Deployment(deployment)), "steady unavailable replica")

	deployment.Status.Conditions[0].Reason = "ReplicaSetUpdated"
	assert.True(t, workloadRolloutInProgress(k8sapi.Deployment(deployment)), "rollout awaiting availability")

	replicaSet := &apps.ReplicaSet{
		ObjectMeta: meta.ObjectMeta{Generation: 4},
		Spec:       apps.ReplicaSetSpec{Replicas: &replicas},
		Status: apps.ReplicaSetStatus{
			ObservedGeneration: 4,
			Replicas:           6,
			AvailableReplicas:  5,
		},
	}
	assert.False(t, workloadRolloutInProgress(k8sapi.ReplicaSet(replicaSet)), "steady unavailable replica")
	replicaSet.Status.Replicas = 5
	assert.True(t, workloadRolloutInProgress(k8sapi.ReplicaSet(replicaSet)), "scale in progress")

	statefulSet := &apps.StatefulSet{
		ObjectMeta: meta.ObjectMeta{Generation: 8},
		Spec: apps.StatefulSetSpec{
			Replicas: &replicas,
			UpdateStrategy: apps.StatefulSetUpdateStrategy{
				Type: apps.OnDeleteStatefulSetStrategyType,
			},
		},
		Status: apps.StatefulSetStatus{
			ObservedGeneration: 8,
			Replicas:           6,
			UpdatedReplicas:    0,
		},
	}
	assert.False(t, workloadRolloutInProgress(k8sapi.StatefulSet(statefulSet)), "OnDelete revision mismatch")
	statefulSet.Status.Replicas = 5
	assert.True(t, workloadRolloutInProgress(k8sapi.StatefulSet(statefulSet)), "OnDelete scale in progress")

	partition := int32(3)
	statefulSet.Status.Replicas = 6
	statefulSet.Spec.UpdateStrategy = apps.StatefulSetUpdateStrategy{
		Type:          apps.RollingUpdateStatefulSetStrategyType,
		RollingUpdate: &apps.RollingUpdateStatefulSetStrategy{Partition: &partition},
	}
	statefulSet.Status.UpdatedReplicas = 3
	assert.False(t, workloadRolloutInProgress(k8sapi.StatefulSet(statefulSet)), "completed partitioned rollout")
	statefulSet.Status.UpdatedReplicas = 2
	assert.True(t, workloadRolloutInProgress(k8sapi.StatefulSet(statefulSet)), "partitioned rollout in progress")
}

func TestEvictOrRolloutDoesNotRestartUpdatingWorkload(t *testing.T) {
	for _, tc := range []struct {
		name              string
		status            apps.DeploymentStatus
		wantEvictionCount int
		wantPatchCount    int
		wantDidRollout    bool
	}{
		{
			name: "stable workload",
			status: apps.DeploymentStatus{
				ObservedGeneration: 12,
				Replicas:           6,
				UpdatedReplicas:    6,
				AvailableReplicas:  6,
			},
			wantEvictionCount: 1,
			wantPatchCount:    1,
			wantDidRollout:    true,
		},
		{
			name: "stable workload with unavailable replica",
			status: apps.DeploymentStatus{
				ObservedGeneration: 12,
				Replicas:           6,
				UpdatedReplicas:    6,
				AvailableReplicas:  5,
				Conditions: []apps.DeploymentCondition{{
					Type:   apps.DeploymentProgressing,
					Status: core.ConditionTrue,
					Reason: "NewReplicaSetAvailable",
				}},
			},
			wantEvictionCount: 1,
			wantPatchCount:    1,
			wantDidRollout:    true,
		},
		{
			name: "update in progress",
			status: apps.DeploymentStatus{
				ObservedGeneration: 12,
				Replicas:           8,
				UpdatedReplicas:    3,
				AvailableReplicas:  5,
			},
			wantDidRollout: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replicas := int32(6)
			deployment := &apps.Deployment{
				ObjectMeta: meta.ObjectMeta{Name: "echo", Namespace: "default", Generation: 12},
				Spec:       apps.DeploymentSpec{Replicas: &replicas},
				Status:     tc.status,
			}
			pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "echo-old", Namespace: "default", UID: types.UID("echo-old")}}
			client := fake.NewSimpleClientset(deployment.DeepCopy(), pod.DeepCopy())
			evictionCount := 0
			client.PrependReactor("create", "pods", func(action k8sTesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "eviction" {
					return false, nil, nil
				}
				evictionCount++
				return true, nil, apierrors.NewTooManyRequests("eviction would violate the pod's disruption budget", 0)
			})

			ctx := k8sapi.WithK8sInterface(context.Background(), client)
			ctx = managerutil.WithEnv(ctx, &managerutil.Env{})
			didRollout, err := evictOrRollout(ctx, k8sapi.Deployment(deployment), pod, 0)
			require.NoError(t, err)
			assert.Equal(t, tc.wantDidRollout, didRollout)
			assert.Equal(t, tc.wantEvictionCount, evictionCount)

			patchCount := 0
			for _, action := range client.Actions() {
				if action.GetVerb() == "patch" && action.GetResource().Resource == "deployments" {
					patchCount++
				}
			}
			assert.Equal(t, tc.wantPatchCount, patchCount)
		})
	}
}

func TestRefreshWorkloadRetriesTransientError(t *testing.T) {
	replicas := int32(1)
	deployment := &apps.Deployment{
		ObjectMeta: meta.ObjectMeta{Name: "echo", Namespace: "default"},
		Spec:       apps.DeploymentSpec{Replicas: &replicas},
	}
	client := fake.NewSimpleClientset(deployment.DeepCopy())
	getCount := 0
	client.PrependReactor("get", "deployments", func(k8sTesting.Action) (bool, runtime.Object, error) {
		getCount++
		if getCount == 1 {
			return true, nil, apierrors.NewServiceUnavailable("temporary API server failure")
		}
		return false, nil, nil
	})

	ctx := k8sapi.WithK8sInterface(context.Background(), client)
	refreshed, err := refreshWorkload(ctx, k8sapi.Deployment(deployment))
	require.NoError(t, err)
	assert.Equal(t, "echo", refreshed.GetName())
	assert.Equal(t, 2, getCount)
}

func TestDeleteMapsAndRolloutNamespaceWaitsForAllEvictions(t *testing.T) {
	const (
		namespace = "default"
		name      = "echo"
	)
	replicas := int32(3)
	selector := map[string]string{"app": name}
	replicaSet := &apps.ReplicaSet{
		ObjectMeta: meta.ObjectMeta{Name: name, Namespace: namespace, Generation: 1},
		Spec: apps.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &meta.LabelSelector{MatchLabels: selector},
		},
		Status: apps.ReplicaSetStatus{
			ObservedGeneration:   1,
			Replicas:             replicas,
			ReadyReplicas:        replicas,
			AvailableReplicas:    replicas,
			FullyLabeledReplicas: replicas,
		},
	}
	objects := []runtime.Object{replicaSet}
	for i := range int(replicas) {
		objects = append(objects, &core.Pod{
			ObjectMeta: meta.ObjectMeta{
				Name:      fmt.Sprintf("echo-old-%d", i),
				Namespace: namespace,
				UID:       types.UID(fmt.Sprintf("echo-old-%d", i)),
				Labels: map[string]string{
					"app":                         name,
					agentconfig.WorkloadNameLabel: name,
					agentconfig.WorkloadKindLabel: string(k8sapi.ReplicaSetKind),
				},
				Annotations: map[string]string{annotation.Config: "stale"},
			},
			Status: core.PodStatus{Phase: core.PodRunning},
		})
	}

	client := fake.NewSimpleClientset(objects...)
	tracker := client.Tracker()
	podsResource := core.SchemeGroupVersion.WithResource("pods")
	replicaSetsResource := apps.SchemeGroupVersion.WithResource("replicasets")
	replacementName := ""
	evictionCount := 0
	var informerCancelled atomic.Bool
	var cancelledBeforeEviction atomic.Bool
	client.PrependReactor("create", "pods", func(action k8sTesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		if informerCancelled.Load() {
			cancelledBeforeEviction.Store(true)
		}
		eviction := action.(k8sTesting.CreateAction).GetObject().(*policy.Eviction)
		require.NoError(t, tracker.Delete(podsResource, namespace, eviction.Name))
		evictionCount++
		replacementName = fmt.Sprintf("echo-new-%d", evictionCount)
		obj, err := tracker.Get(replicaSetsResource, namespace, name)
		require.NoError(t, err)
		updated := obj.(*apps.ReplicaSet).DeepCopy()
		updated.Status.Replicas = replicas - 1
		updated.Status.ReadyReplicas = replicas - 1
		updated.Status.AvailableReplicas = replicas - 1
		updated.Status.FullyLabeledReplicas = replicas - 1
		require.NoError(t, tracker.Update(replicaSetsResource, updated, namespace))
		return true, nil, nil
	})
	client.PrependReactor("get", "replicasets", func(k8sTesting.Action) (bool, runtime.Object, error) {
		if replacementName == "" {
			return false, nil, nil
		}
		obj, err := tracker.Get(replicaSetsResource, namespace, name)
		require.NoError(t, err)
		updating := obj.(*apps.ReplicaSet).DeepCopy()
		replacement := &core.Pod{
			ObjectMeta: meta.ObjectMeta{
				Name:      replacementName,
				Namespace: namespace,
				UID:       types.UID(replacementName),
				Labels: map[string]string{
					"app":                         name,
					agentconfig.WorkloadNameLabel: name,
					agentconfig.WorkloadKindLabel: string(k8sapi.ReplicaSetKind),
				},
			},
			Status: core.PodStatus{Phase: core.PodRunning},
		}
		require.NoError(t, tracker.Create(podsResource, replacement, namespace))
		recovered := updating.DeepCopy()
		recovered.Status.Replicas = replicas
		recovered.Status.ReadyReplicas = replicas
		recovered.Status.AvailableReplicas = replicas
		recovered.Status.FullyLabeledReplicas = replicas
		require.NoError(t, tracker.Update(replicaSetsResource, recovered, namespace))
		replacementName = ""
		return true, updating, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = k8sapi.WithJoinedClientSetInterface(ctx, client, argorolloutsfake.NewSimpleClientset())
	ctx = informer.WithFactory(ctx, "")
	ctx = managerutil.WithEnv(ctx, &managerutil.Env{
		AgentArrivalTimeout:  2 * time.Second,
		EnabledWorkloadKinds: k8sapi.Kinds{k8sapi.ReplicaSetKind},
	})
	factory := informer.GetK8sFactory(ctx, namespace)
	podStore := factory.Core().V1().Pods().Informer().GetStore()
	replicaSetStore := factory.Apps().V1().ReplicaSets().Informer().GetStore()
	for _, object := range objects {
		switch object := object.(type) {
		case *core.Pod:
			require.NoError(t, podStore.Add(object.DeepCopy()))
		case *apps.ReplicaSet:
			require.NoError(t, replicaSetStore.Add(object.DeepCopy()))
		}
	}
	evictMap, err := podList(ctx, namespace)
	require.NoError(t, err)
	require.Len(t, evictMap, 1)

	cw := NewWatcher().(*configWatcher)
	iwc := &informersWithCancel{cancel: func() { informerCancelled.Store(true) }}
	cw.informers.Store(namespace, iwc)
	cw.deleteMapsAndRolloutNS(ctx, namespace, iwc)

	assert.Equal(t, int(replicas), evictionCount)
	assert.True(t, informerCancelled.Load())
	assert.False(t, cancelledBeforeEviction.Load())
}
