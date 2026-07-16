package mutator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8sTesting "k8s.io/client-go/testing"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
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

func TestEvictOrRolloutDoesNotRestartUpdatingWorkload(t *testing.T) {
	for _, tc := range []struct {
		name           string
		status         apps.DeploymentStatus
		wantPatchCount int
		wantDidRollout bool
	}{
		{
			name: "stable workload",
			status: apps.DeploymentStatus{
				ObservedGeneration: 12,
				Replicas:           6,
				UpdatedReplicas:    6,
				AvailableReplicas:  6,
			},
			wantPatchCount: 1,
			wantDidRollout: true,
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
			client.PrependReactor("create", "pods", func(action k8sTesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "eviction" {
					return false, nil, nil
				}
				return true, nil, apierrors.NewTooManyRequests("eviction would violate the pod's disruption budget", 0)
			})

			ctx := k8sapi.WithK8sInterface(context.Background(), client)
			ctx = managerutil.WithEnv(ctx, &managerutil.Env{})
			didRollout, err := evictOrRollout(ctx, k8sapi.Deployment(deployment), pod, 0)
			require.NoError(t, err)
			assert.Equal(t, tc.wantDidRollout, didRollout)

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
