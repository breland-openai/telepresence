package mutator

import (
	"context"
	"testing"

	argorolloutsfake "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/fake"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	policy "k8s.io/api/policy/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

type replacementFixture struct {
	t       *testing.T
	ctx     context.Context
	client  *fake.Clientset
	watcher *configWatcher
	dep     *apps.Deployment
	evicted []*policy.Eviction
	denyPDB bool
}

func newReplacementFixture(t *testing.T) *replacementFixture {
	t.Helper()
	dep := &apps.Deployment{
		ObjectMeta: meta.ObjectMeta{Name: "api", Namespace: "default", UID: "api-deployment", Generation: 1},
		Spec:       apps.DeploymentSpec{Replicas: ptr.To(int32(1)), Selector: &meta.LabelSelector{MatchLabels: map[string]string{"app": "api"}}},
		Status: apps.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, Conditions: []apps.DeploymentCondition{{
			Type: apps.DeploymentProgressing, Status: core.ConditionTrue, Reason: "NewReplicaSetAvailable",
		}}},
	}
	client := fake.NewClientset(dep.DeepCopy())
	ctx := k8sapi.WithJoinedClientSetInterface(context.Background(), client, argorolloutsfake.NewSimpleClientset())
	ctx = managerutil.WithEnv(ctx, &managerutil.Env{AgentRequireAuthoritativeRoutes: true, EnabledWorkloadKinds: k8sapi.Kinds{k8sapi.DeploymentKind}})
	ctx = informer.WithFactory(ctx, "")
	f := informer.GetK8sFactory(ctx, "default")
	require.NoError(t, f.Apps().V1().Deployments().Informer().GetStore().Add(dep.DeepCopy()))
	fixture := &replacementFixture{t: t, ctx: ctx, client: client, watcher: NewWatcher().(*configWatcher), dep: dep}
	fixture.watcher.Store(replacementConfig("http", true))
	st := fixture.watcher.lockEvictionState(WorkloadKey{Kind: k8sapi.DeploymentKind, Name: "api", Namespace: "default"})
	st.replacementPending = true
	st.Unlock()
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		eviction := action.(k8stesting.CreateAction).GetObject().(*policy.Eviction)
		fixture.evicted = append(fixture.evicted, eviction.DeepCopy())
		if fixture.denyPDB {
			return true, nil, k8sErrors.NewTooManyRequests("would violate the pod's disruption budget", 0)
		}
		return true, nil, nil
	})
	return fixture
}

func replacementConfig(protocol string, strict bool) *agentconfig.Sidecar {
	return &agentconfig.Sidecar{
		AgentName: "api", Namespace: "default", WorkloadName: "api", WorkloadKind: k8sapi.DeploymentKind,
		RequireAuthoritativeRoutes: strict, Containers: []*agentconfig.Container{{Name: "app", Intercepts: []*agentconfig.Intercept{{AppProtocol: protocol}}}},
	}
}

func (f *replacementFixture) pod(name, protocol string) *core.Pod {
	f.t.Helper()
	raw, err := agentconfig.MarshalTight(replacementConfig(protocol, true))
	require.NoError(f.t, err)
	return &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name: name, Namespace: "default", UID: types.UID("uid-" + name), ResourceVersion: "7",
			Labels:          map[string]string{"app": "api", agentconfig.WorkloadNameLabel: "api", agentconfig.WorkloadKindLabel: string(k8sapi.DeploymentKind)},
			Annotations:     map[string]string{annotation.Config: raw},
			OwnerReferences: []meta.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "api", UID: "api-deployment", Controller: ptr.To(true)}},
		},
		Status: core.PodStatus{Phase: core.PodRunning, Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionFalse}}},
	}
}

func (f *replacementFixture) addPod(pod *core.Pod) *core.Pod {
	f.t.Helper()
	_, err := f.client.CoreV1().Pods(pod.Namespace).Create(f.ctx, pod, meta.CreateOptions{})
	require.NoError(f.t, err)
	cached := pod.DeepCopy()
	cached.Status.Conditions = nil // production informer transform cannot establish readiness
	factory := informer.GetK8sFactory(f.ctx, pod.Namespace)
	require.NoError(f.t, factory.Core().V1().Pods().Informer().GetStore().Add(cached))
	return cached
}

func (f *replacementFixture) endpoint(pod *core.Pod, ready, serving *bool) {
	f.t.Helper()
	slice := &discovery.EndpointSlice{
		ObjectMeta: meta.ObjectMeta{Name: "endpoint", Namespace: pod.Namespace}, AddressType: discovery.AddressTypeIPv4,
		Endpoints: []discovery.Endpoint{{
			Addresses: []string{"10.0.0.1"}, TargetRef: &core.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, UID: pod.UID},
			Conditions: discovery.EndpointConditions{Ready: ready, Serving: serving},
		}},
	}
	_, err := f.client.DiscoveryV1().EndpointSlices(pod.Namespace).Create(f.ctx, slice, meta.CreateOptions{})
	require.NoError(f.t, err)
}

func TestProtectedReplacementCatchesLateUnreadyAdmissionAfterServiceRestores(t *testing.T) {
	f := newReplacementFixture(t)
	f.watcher.Store(replacementConfig("h2c", true))
	// The Service changes back before the admission that consumed h2c is visible.
	f.watcher.Store(replacementConfig("http", true))
	handled, err := f.watcher.catchUpProtectedReplacement(f.ctx, k8sapi.Deployment(f.dep))
	require.NoError(t, err)
	require.False(t, handled)
	stale := f.pod("stale", "h2c")
	cached := f.addPod(stale)
	correct := f.pod("correct", "http")
	correct.Status.Conditions[0].Status = core.ConditionTrue
	f.addPod(correct)
	f.endpoint(stale, ptr.To(false), ptr.To(false))
	f.watcher.reconcileProtectedReplacementPod(f.ctx, cached)
	require.Len(t, f.evicted, 1)
	require.Equal(t, stale.Name, f.evicted[0].Name)
	require.Equal(t, stale.UID, *f.evicted[0].DeleteOptions.Preconditions.UID)
	require.Equal(t, stale.ResourceVersion, *f.evicted[0].DeleteOptions.Preconditions.ResourceVersion)
	require.Zero(t, countActions(f.client.Actions(), "patch", "deployments"))
	f.watcher.reconcileProtectedReplacementPod(f.ctx, correct)
	require.Len(t, f.evicted, 1, "latest protected configuration and Ready sibling are left alone")
}

func TestProtectedReplacementNeverTouchesServingUnknownOrUnrelatedRollout(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*replacementFixture, *core.Pod)
	}{
		{"pod ready", func(_ *replacementFixture, p *core.Pod) { p.Status.Conditions[0].Status = core.ConditionTrue }},
		{"pod unknown", func(_ *replacementFixture, p *core.Pod) { p.Status.Conditions = nil }},
		{"pod pending", func(_ *replacementFixture, p *core.Pod) { p.Status.Phase = core.PodPending }},
		{"terminating", func(_ *replacementFixture, p *core.Pod) {
			p.DeletionTimestamp = ptr.To(meta.Now())
			p.Finalizers = []string{"test/keep"}
		}},
		{"missing current resource version", func(_ *replacementFixture, p *core.Pod) { p.ResourceVersion = "" }},
		{"manual", func(_ *replacementFixture, p *core.Pod) { p.Annotations[annotation.ManuallyInjected] = "true" }},
		{"unrelated true owner despite copied labels", func(_ *replacementFixture, p *core.Pod) { p.OwnerReferences[0].UID = "unrelated-deployment" }},
		{"latest matches", func(f *replacementFixture, _ *core.Pod) { f.watcher.Store(replacementConfig("h2c", true)) }},
		{"feature off", func(f *replacementFixture, _ *core.Pod) { f.watcher.Store(replacementConfig("http", false)) }},
		{"endpoint ready", func(f *replacementFixture, p *core.Pod) { f.endpoint(p, ptr.To(true), ptr.To(false)) }},
		{"endpoint serving", func(f *replacementFixture, p *core.Pod) { f.endpoint(p, ptr.To(false), ptr.To(true)) }},
		{"endpoint unknown", func(f *replacementFixture, p *core.Pod) { f.endpoint(p, nil, ptr.To(false)) }},
		{"unrelated template rollout", func(f *replacementFixture, _ *core.Pod) {
			f.dep.Status.UpdatedReplicas = 0
			_, err := f.client.AppsV1().Deployments(f.dep.Namespace).UpdateStatus(f.ctx, f.dep, meta.UpdateOptions{})
			require.NoError(t, err)
		}},
		{"not our replacement", func(f *replacementFixture, _ *core.Pod) {
			st := f.watcher.lockEvictionState(WorkloadKey{Kind: k8sapi.DeploymentKind, Name: "api", Namespace: "default"})
			st.replacementPending = false
			st.Unlock()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplacementFixture(t)
			p := f.pod("stale", "h2c")
			tc.edit(f, p)
			f.addPod(p)
			_, err := f.watcher.catchUpProtectedReplacement(f.ctx, k8sapi.Deployment(f.dep))
			require.NoError(t, err)
			require.Empty(t, f.evicted)
			require.Zero(t, countActions(f.client.Actions(), "patch", "deployments"))
		})
	}
}

func TestProtectedReplacementRespectsPDBAndRetriesWithoutWorkloadPatch(t *testing.T) {
	f := newReplacementFixture(t)
	cached := f.addPod(f.pod("stale", "h2c"))
	f.denyPDB = true
	f.watcher.reconcileProtectedReplacementPod(f.ctx, cached)
	require.Len(t, f.evicted, 1)
	require.False(t, f.watcher.isEvicted(cached.UID))
	require.Zero(t, countActions(f.client.Actions(), "patch", "deployments"))
	f.denyPDB = false
	f.watcher.reconcilePendingProtectedReplacement(f.ctx, WorkloadKey{Kind: k8sapi.DeploymentKind, Name: "api", Namespace: "default"})
	require.Len(t, f.evicted, 2)
	require.True(t, f.watcher.isEvicted(cached.UID))
	require.Zero(t, countActions(f.client.Actions(), "patch", "deployments"))
}

func TestProtectedReplacementVerifiesDeploymentReplicaSetUIDChain(t *testing.T) {
	f := newReplacementFixture(t)
	pod := f.pod("stale", "h2c")
	deploymentRef := pod.OwnerReferences[0]
	pod.OwnerReferences = []meta.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "api-revision", UID: "current-rs", Controller: ptr.To(true)}}
	rs := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{Name: "api-revision", Namespace: "default", UID: "wrong-rs", OwnerReferences: []meta.OwnerReference{deploymentRef}}}
	_, err := f.client.AppsV1().ReplicaSets("default").Create(f.ctx, rs, meta.CreateOptions{})
	require.NoError(t, err)
	cached := f.addPod(pod)
	f.watcher.reconcileProtectedReplacementPod(f.ctx, cached)
	require.Empty(t, f.evicted, "same ReplicaSet name with a different UID cannot establish ownership")
	rs.UID = "current-rs"
	_, err = f.client.AppsV1().ReplicaSets("default").Update(f.ctx, rs, meta.UpdateOptions{})
	require.NoError(t, err)
	f.watcher.reconcileProtectedReplacementPod(f.ctx, cached)
	require.Len(t, f.evicted, 1)
}
