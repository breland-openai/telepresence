package mutator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func TestGetOrGenerateReplacesScopedStatefulSetImageFromOldPodAnnotation(t *testing.T) {
	const standard, candidate = "registry.example/tel2:stable", "registry.example/tel2@sha256:abc123"
	env := &managerutil.Env{
		RouteIntentEnabled: true, RouteIntentNamespaces: []string{"ambassador"}, AgentRouteIntentImage: candidate,
		AgentRequireAuthoritativeRoutes: true, AgentPort: 9900,
	}
	ctx := k8sapi.WithK8sInterface(context.Background(), fake.NewClientset())
	ctx = managerutil.WithResolvedAgentImageRetriever(managerutil.WithEnv(ctx, env), managerutil.ImageFromEnv(standard))
	workload := func(namespace, name string) k8sapi.Workload {
		return k8sapi.StatefulSet(&apps.StatefulSet{
			ObjectMeta: meta.ObjectMeta{Name: name, Namespace: namespace},
			Spec: apps.StatefulSetSpec{Template: core.PodTemplateSpec{
				ObjectMeta: meta.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec:       core.PodSpec{Containers: []core.Container{{Name: "app"}}},
			}},
		})
	}
	initial := func(namespace, name string, manual bool) *agentconfig.Sidecar {
		return &agentconfig.Sidecar{AgentName: name, Namespace: namespace, WorkloadName: name, WorkloadKind: k8sapi.StatefulSetKind, AgentImage: standard, Manual: manual}
	}
	watcher := NewWatcher().(*configWatcher)
	stale := initial("ambassador", "web", false)
	watcher.Store(stale) // same path as startup reading a previous Pod annotation
	generated, err := watcher.GetOrGenerate(ctx, workload("ambassador", "web"))
	require.NoError(t, err)
	require.Equal(t, candidate, generated.AgentImage)
	require.True(t, generated.RequireAuthoritativeRoutes)
	require.Equal(t, standard, stale.AgentImage, "a shared cached annotation must not be changed in place")
	require.Same(t, generated, watcher.Get("web", "ambassador"))
	again, err := watcher.GetOrGenerate(ctx, workload("ambassador", "web"))
	require.NoError(t, err)
	require.Same(t, generated, again, "replacement StatefulSet admissions must keep the same staged selection")

	outside := initial("application", "ordinary", false)
	watcher.Store(outside)
	generated, err = watcher.GetOrGenerate(ctx, workload("application", "ordinary"))
	require.NoError(t, err)
	require.Same(t, outside, generated)
	require.Equal(t, standard, generated.AgentImage)
	require.False(t, generated.RequireAuthoritativeRoutes)
	generated, err = watcher.GetOrGenerate(ctx, workload("application", "fresh"))
	require.NoError(t, err)
	require.Equal(t, standard, generated.AgentImage)
	require.False(t, generated.RequireAuthoritativeRoutes)

	manual := initial("ambassador", "manual", true)
	watcher.Store(manual)
	generated, err = watcher.GetOrGenerate(ctx, workload("ambassador", "manual"))
	require.NoError(t, err)
	require.Same(t, manual, generated)
}
