package mutator

import (
	"encoding/json/v2"
	"slices"
	"testing"

	argofake "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/fake"
	"github.com/stretchr/testify/require"
	admission "k8s.io/api/admission/v1"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func TestAdmissionAfterRestoringPodAnnotation(t *testing.T) {
	const namespace = "app-space"
	wl, obj := reconciliationTestWorkload(k8sapi.DeploymentKind, "echo", namespace)
	deployment := obj.(*apps.Deployment)
	deployment.Spec.Template.Spec.Containers = []core.Container{{Name: "echo", Image: "example/echo:1"}}
	rs := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{
		Name: "echo-6f7db9fd49", Namespace: namespace, UID: "echo-rs",
		OwnerReferences: []meta.OwnerReference{*meta.NewControllerRef(deployment, apps.SchemeGroupVersion.WithKind("Deployment"))},
	}}
	previous := reconciliationTestConfig(wl, agentconfig.ReplacePolicyIntercept)
	previous.Containers[0].Intercepts = []*agentconfig.Intercept{{
		ServiceName: "echo", ContainerPort: 8080, AgentPort: 9900, Protocol: types.ProtoTCP, TargetPortNumeric: true,
	}}
	previous.AgentImage = "example/agent:old"
	encoded, err := agentconfig.MarshalTight(previous)
	require.NoError(t, err)
	oldPod := reconciliationTestPod(wl, "previous", encoded, true)
	oldPod.OwnerReferences = []meta.OwnerReference{*meta.NewControllerRef(rs, apps.SchemeGroupVersion.WithKind("ReplicaSet"))}

	client := fake.NewSimpleClientset(deployment, rs, oldPod)
	ctx := k8sapi.WithJoinedClientSetInterface(t.Context(), client, argofake.NewSimpleClientset())
	ctx = informer.WithFactory(ctx, "")
	env := &managerutil.Env{
		AgentInjectPolicy:         agentconfig.OnDemand,
		EnabledWorkloadKinds:      k8sapi.Kinds{k8sapi.DeploymentKind},
		AgentInitContainerEnabled: true,
		AgentImagePullPolicy:      string(core.PullAlways),
		AgentImagePullSecrets:     []core.LocalObjectReference{{Name: "agent-registry"}},
		AgentInitResources:        &core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("50m")}},
		AgentSecurityContext:      &core.SecurityContext{AllowPrivilegeEscalation: ptr.To(false), RunAsGroup: ptr.To[int64](3000)},
		AgentInitSecurityContext:  &core.SecurityContext{Capabilities: &core.Capabilities{Drop: []core.Capability{"ALL"}, Add: []core.Capability{"NET_ADMIN"}}},
	}
	ctx = managerutil.WithEnv(ctx, env)
	const image = "example/agent:current"
	ctx = managerutil.WithResolvedAgentImageRetriever(ctx, managerutil.ImageFromEnv(image))
	factory := informer.GetK8sFactory(ctx, namespace)
	require.NoError(t, factory.Apps().V1().Deployments().Informer().GetStore().Add(deployment))
	require.NoError(t, factory.Apps().V1().ReplicaSets().Informer().GetStore().Add(rs))
	require.NoError(t, factory.Core().V1().Pods().Informer().GetStore().Add(oldPod))

	watcher := NewWatcher().(*configWatcher)
	gc, err := env.GeneratorConfig(image)
	require.NoError(t, err)
	require.NoError(t, watcher.regenerateAgentConfigs(ctx, namespace, gc))
	watcher.SetConfigured()
	config := watcher.Get(wl.GetName(), namespace)
	require.NotNil(t, config)
	require.Equal(t, previous.Containers, config.Containers)

	injector := NewAgentInjector(ctx, watcher)
	for _, mode := range []string{"on-demand", "enabled"} {
		t.Run(mode, func(t *testing.T) {
			pod := &core.Pod{TypeMeta: meta.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: meta.ObjectMeta{
				Name: "echo-new", Namespace: namespace, OwnerReferences: oldPod.OwnerReferences,
			}, Spec: deployment.Spec.Template.Spec}
			if mode == "enabled" {
				pod.Annotations = map[string]string{annotation.InjectTrafficAgent: "enabled"}
			}
			data, err := json.Marshal(pod)
			require.NoError(t, err)
			patches, err := injector.Inject(ctx, &admission.AdmissionRequest{
				Operation: admission.Create, Resource: meta.GroupVersionResource{Version: "v1", Resource: "pods"},
				Namespace: namespace, Name: pod.Name, Object: runtime.RawExtension{Raw: data},
			})
			require.NoError(t, err)
			i := slices.IndexFunc(patches, func(op PatchOperation) bool { return op.Op == "add" && op.Path == "/spec/containers/-" })
			require.NotEqual(t, -1, i)
			agent := patches[i].Value.(*core.Container)
			require.Equal(t, agentconfig.ContainerName, agent.Name)
			require.Equal(t, image, agent.Image)
			require.Equal(t, core.PullAlways, agent.ImagePullPolicy)
			require.Equal(t, env.AgentSecurityContext, agent.SecurityContext)
			require.Contains(t, patches, PatchOperation{Op: "replace", Path: "/spec/imagePullSecrets", Value: env.AgentImagePullSecrets})

			i = slices.IndexFunc(patches, func(op PatchOperation) bool { return op.Path == "/spec/initContainers" })
			require.NotEqual(t, -1, i)
			initContainers := patches[i].Value.([]core.Container)
			require.Len(t, initContainers, 1)
			require.Equal(t, image, initContainers[0].Image)
			require.Equal(t, env.AgentInitSecurityContext, initContainers[0].SecurityContext)
			require.Equal(t, *env.AgentInitResources, initContainers[0].Resources)
		})
	}
	for _, action := range client.Actions() {
		require.Contains(t, []string{"get", "list", "watch"}, action.GetVerb())
	}
}
