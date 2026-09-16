package mutator

import (
	"context"
	"testing"

	argofake "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/fake"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func nativeOwnerReference(groupVersion string, kind k8sapi.Kind, name string, uid types.UID) meta.OwnerReference {
	return meta.OwnerReference{APIVersion: groupVersion, Kind: string(kind), Name: name, UID: uid, Controller: ptr.To(true)}
}

func nativeOwnerTestContext(t *testing.T, kinds k8sapi.Kinds, objects ...runtime.Object) (context.Context, *fake.Clientset) {
	t.Helper()
	client := fake.NewSimpleClientset(objects...)
	ctx := k8sapi.WithJoinedClientSetInterface(t.Context(), client, argofake.NewSimpleClientset())
	return managerutil.WithEnv(ctx, &managerutil.Env{EnabledWorkloadKinds: kinds}), client
}

func TestNativeAgentOwnershipStopsAtOperatorManagedWorkloadForEverySibling(t *testing.T) {
	const namespace = "default"
	deployment := replacementTestDeployment("echo", namespace, 2)
	deployment.OwnerReferences = []meta.OwnerReference{nativeOwnerReference("operator.example/v1", "Operator", "outside", "operator-uid")}
	replicaSet := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{
		Name: "echo-rs", Namespace: namespace, UID: "replica-set-uid",
		OwnerReferences: []meta.OwnerReference{nativeOwnerReference("apps/v1beta2", k8sapi.DeploymentKind, deployment.Name, deployment.UID)},
	}}
	pods := []*core.Pod{replacementTestPod("echo-one", namespace, deployment.Name), replacementTestPod("echo-two", namespace, deployment.Name)}
	for _, pod := range pods {
		pod.OwnerReferences = []meta.OwnerReference{nativeOwnerReference("apps/v1", k8sapi.ReplicaSetKind, replicaSet.Name, replicaSet.UID)}
	}
	ctx, client := nativeOwnerTestContext(t, k8sapi.Kinds{k8sapi.DeploymentKind}, deployment, replicaSet, pods[0], pods[1])
	wl := k8sapi.Deployment(deployment)
	for _, pod := range pods {
		owned, err := PodOwnedByWorkload(ctx, wl, pod)
		require.NoError(t, err)
		require.True(t, owned)
	}
	live, err := liveWorkloadPods(ctx, wl)
	require.NoError(t, err)
	require.ElementsMatch(t, pods, live)
	bulk, err := liveAgentPodList(ctx, namespace)
	require.NoError(t, err)
	require.Len(t, bulk, 1)
	require.ElementsMatch(t, pods, bulk[evictionKey(wl)].pods)
	require.Zero(t, countActions(client.Actions(), "get", "operators"))
}

func TestNativeAgentOwnershipRejectsControllerCyclesPerTraversal(t *testing.T) {
	const namespace = "default"
	for _, selfCycle := range []bool{false, true} {
		name := "two node cycle"
		if selfCycle {
			name = "self cycle"
		}
		t.Run(name, func(t *testing.T) {
			deployment := replacementTestDeployment("echo", namespace, 1)
			a := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{Name: "a", Namespace: namespace, UID: "a-uid"}}
			b := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{Name: "b", Namespace: namespace, UID: "b-uid"}}
			a.OwnerReferences = []meta.OwnerReference{nativeOwnerReference("apps/v1", k8sapi.ReplicaSetKind, b.Name, b.UID)}
			b.OwnerReferences = []meta.OwnerReference{nativeOwnerReference("apps/v1", k8sapi.ReplicaSetKind, a.Name, a.UID)}
			if selfCycle {
				a.OwnerReferences = b.OwnerReferences
			}
			pod := replacementTestPod("echo-one", namespace, deployment.Name)
			pod.OwnerReferences = []meta.OwnerReference{nativeOwnerReference("apps/v1", k8sapi.ReplicaSetKind, a.Name, a.UID)}
			ctx, client := nativeOwnerTestContext(t, k8sapi.Kinds{k8sapi.DeploymentKind}, deployment, a, b, pod)
			wl := k8sapi.Deployment(deployment)
			for range 2 {
				owned, err := PodOwnedByWorkload(ctx, wl, pod)
				require.True(t, apierrors.IsNotFound(err), "%v", err)
				require.False(t, owned)
			}
			live, err := liveWorkloadPods(ctx, wl)
			require.NoError(t, err)
			require.Empty(t, live)
			bulk, err := liveAgentPodList(ctx, namespace)
			require.NoError(t, err)
			require.Empty(t, bulk)
			require.LessOrEqual(t, countActions(client.Actions(), "get", "replicasets"), 8)
			require.Zero(t, countActions(client.Actions(), "create", "pods"))
		})
	}
}

func TestNativeAgentOwnershipSupportsEnabledStandaloneReplicaSetAndIgnoresDisabledOrphanLabels(t *testing.T) {
	const namespace = "default"
	deployment := replacementTestDeployment("echo", namespace, 1)
	replicaSet := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{
		Name: "echo-rs", Namespace: namespace, UID: "replica-set-uid",
		Labels: map[string]string{agentconfig.WorkloadNameLabel: deployment.Name, agentconfig.WorkloadKindLabel: string(k8sapi.DeploymentKind)},
	}}
	pod := replacementTestPod("echo-one", namespace, deployment.Name)
	pod.OwnerReferences = []meta.OwnerReference{nativeOwnerReference("apps/v1", k8sapi.ReplicaSetKind, replicaSet.Name, replicaSet.UID)}
	enabledCtx, _ := nativeOwnerTestContext(t, k8sapi.Kinds{k8sapi.ReplicaSetKind}, deployment, replicaSet, pod)
	standalone := k8sapi.ReplicaSet(replicaSet)
	owned, err := PodOwnedByWorkload(enabledCtx, standalone, pod)
	require.NoError(t, err)
	require.True(t, owned)
	bulk, err := liveAgentPodList(enabledCtx, namespace)
	require.NoError(t, err)
	require.Len(t, bulk, 1)
	require.Contains(t, bulk, evictionKey(standalone))
	require.Len(t, bulk[evictionKey(standalone)].pods, 1)
	require.Equal(t, pod.Name, bulk[evictionKey(standalone)].pods[0].Name)

	disabledCtx, _ := nativeOwnerTestContext(t, k8sapi.Kinds{k8sapi.DeploymentKind}, deployment, replicaSet, pod)
	owned, err = PodOwnedByWorkload(disabledCtx, k8sapi.Deployment(deployment), pod)
	require.True(t, apierrors.IsNotFound(err), "%v", err)
	require.False(t, owned)
	bulk, err = liveAgentPodList(disabledCtx, namespace)
	require.NoError(t, err)
	require.Empty(t, bulk)
}
