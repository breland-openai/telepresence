package state

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	argorollouts "github.com/datawire/argo-rollouts-go-client/pkg/apis/rollouts/v1alpha1"
	argofake "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/fake"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func sidecarOwnerReference(kind k8sapi.Kind, name string, uid k8stypes.UID) meta.OwnerReference {
	return meta.OwnerReference{APIVersion: "apps/v1", Kind: string(kind), Name: name, UID: uid, Controller: ptr.To(true)}
}

func sidecarOwnerReferenceWithGroup(group string, kind k8sapi.Kind, name string, uid k8stypes.UID) meta.OwnerReference {
	ref := sidecarOwnerReference(kind, name, uid)
	ref.APIVersion = group
	return ref
}

func sidecarOwnerReplicaSet(name string, uid k8stypes.UID, owner string, ownerUID k8stypes.UID) *apps.ReplicaSet {
	return &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{
		Name: name, Namespace: "testing", UID: uid,
		OwnerReferences: []meta.OwnerReference{sidecarOwnerReference(k8sapi.DeploymentKind, owner, ownerUID)},
	}}
}

func sidecarOrphanReplicaSetWithDesiredLabels() *apps.ReplicaSet {
	return &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{Name: "app-orphan-rs", Namespace: "testing", UID: "orphan-rs", Labels: map[string]string{
		agentconfig.WorkloadNameLabel: "app", agentconfig.WorkloadKindLabel: string(k8sapi.DeploymentKind),
	}}}
}

func sidecarReplicaSetWithIncorrectDeploymentGroup() *apps.ReplicaSet {
	rs := sidecarOwnerReplicaSet("app-wrong-group", "wrong-group-rs", "app", "work-1")
	rs.OwnerReferences[0].APIVersion = "other.io/v1"
	return rs
}

func sidecarSpoofWorkloadLabels(pod *core.Pod, kind k8sapi.Kind) {
	pod.Labels = map[string]string{agentconfig.WorkloadNameLabel: "app", agentconfig.WorkloadKindLabel: string(kind)}
}

func ensureSidecarOwnerWithLaterRegistration(t *testing.T, f *sidecarArrivalFixture, work k8sapi.Workload, invalid, good *core.Pod) {
	t.Helper()
	f.register(invalid.Name, invalid.UID)
	result := f.startKind(f.ctx, work, 4*time.Second)
	sidecarAttempt(t, f)
	select {
	case got := <-result:
		t.Fatalf("manager returned the registered nonowned physical pod with identical config: %+v", got)
	case <-time.After(140 * time.Millisecond):
	}
	f.register(good.Name, good.UID)
	got := sidecarResult(t, result)
	require.NoError(t, got.err)
	require.Len(t, got.agents, 1)
	assert.Equal(t, string(good.UID), got.agents[0].PodUid)
}

func TestEnsureSidecarRequiresExactDeploymentControllerChain(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		controller *meta.OwnerReference
		objects    []runtime.Object
	}{
		{
			name: "same named recreated deployment", controller: ptr.To(sidecarOwnerReference(k8sapi.ReplicaSetKind, "app-old", "old-rs")),
			objects: []runtime.Object{sidecarOwnerReplicaSet("app-old", "old-rs", "app", "work-previous")},
		},
		{
			name: "stale physical replica set UID", controller: ptr.To(sidecarOwnerReference(k8sapi.ReplicaSetKind, "app-stale", "old-rs")),
			objects: []runtime.Object{sidecarOwnerReplicaSet("app-stale", "new-rs", "app", "work-1")},
		},
		{
			name: "orphan replica set with desired workload labels", controller: ptr.To(sidecarOwnerReference(k8sapi.ReplicaSetKind, "app-orphan-rs", "orphan-rs")),
			objects: []runtime.Object{sidecarOrphanReplicaSetWithDesiredLabels()},
		},
		{name: "missing physical replica set UID", controller: ptr.To(sidecarOwnerReference(k8sapi.ReplicaSetKind, "app-rs", ""))},
		{name: "missing physical replica set group", controller: ptr.To(sidecarOwnerReferenceWithGroup("", k8sapi.ReplicaSetKind, "app-rs", "work-rs"))},
		{name: "wrong physical replica set group", controller: ptr.To(sidecarOwnerReferenceWithGroup("other.io/v1", k8sapi.ReplicaSetKind, "app-rs", "work-rs"))},
		{
			name: "missing referenced deployment UID", controller: ptr.To(sidecarOwnerReference(k8sapi.ReplicaSetKind, "app-no-owner-uid", "no-owner-uid")),
			objects: []runtime.Object{sidecarOwnerReplicaSet("app-no-owner-uid", "no-owner-uid", "app", "")},
		},
		{
			name: "wrong referenced deployment group", controller: ptr.To(sidecarOwnerReference(k8sapi.ReplicaSetKind, "app-wrong-group", "wrong-group-rs")),
			objects: []runtime.Object{sidecarReplicaSetWithIncorrectDeploymentGroup()},
		},
		{
			name: "foreign controller with same labels", controller: ptr.To(sidecarOwnerReference(k8sapi.ReplicaSetKind, "foreign-rs", "foreign-rs")),
			objects: []runtime.Object{
				sidecarOwnerReplicaSet("foreign-rs", "foreign-rs", "foreign", "foreign-work"),
				&apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: "foreign", Namespace: "testing", UID: "foreign-work"}},
			},
		},
		{name: "orphan with same labels"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
			invalid := sidecarArrivalPod(t, "app-invalid", "invalid-physical", desired)
			invalid.OwnerReferences = nil
			if tc.controller != nil {
				invalid.OwnerReferences = []meta.OwnerReference{*tc.controller}
			}
			sidecarSpoofWorkloadLabels(invalid, k8sapi.DeploymentKind)
			good := sidecarArrivalPod(t, "app-current", "current-physical", desired)
			objects := append([]runtime.Object{invalid, good}, tc.objects...)
			f := newSidecarArrivalFixture(t, objects...)
			f.watcher.Store(desired)
			ensureSidecarOwnerWithLaterRegistration(t, f, k8sapi.Deployment(f.work), invalid, good)
			var gotReplicaSet, gotDeployment bool
			for _, action := range f.client.Actions() {
				gotReplicaSet = gotReplicaSet || action.Matches("get", "replicasets")
				gotDeployment = gotDeployment || action.Matches("get", "deployments")
			}
			assert.True(t, gotReplicaSet, "candidate must traverse the physically current intermediate ReplicaSet with a native GET")
			assert.True(t, gotDeployment, "candidate must verify the current Deployment through a native GET")
		})
	}
}

func TestEnsureSidecarRequiresExactStatefulController(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		controller *meta.OwnerReference
	}{
		{name: "same named recreated stateful workload", controller: ptr.To(sidecarOwnerReference(k8sapi.StatefulSetKind, "app", "previous-stateful"))},
		{name: "missing referenced stateful workload UID", controller: ptr.To(sidecarOwnerReference(k8sapi.StatefulSetKind, "app", ""))},
		{name: "missing referenced stateful workload group", controller: ptr.To(sidecarOwnerReferenceWithGroup("", k8sapi.StatefulSetKind, "app", "current-stateful"))},
		{name: "wrong referenced stateful workload group", controller: ptr.To(sidecarOwnerReferenceWithGroup("other.io/v1", k8sapi.StatefulSetKind, "app", "current-stateful"))},
		{name: "foreign controller with same labels", controller: ptr.To(sidecarOwnerReference(k8sapi.StatefulSetKind, "foreign", "foreign-stateful"))},
		{name: "orphan with same labels"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
			desired.WorkloadKind = k8sapi.StatefulSetKind
			work := &apps.StatefulSet{ObjectMeta: meta.ObjectMeta{Name: "app", Namespace: "testing", UID: "current-stateful"}}
			foreign := &apps.StatefulSet{ObjectMeta: meta.ObjectMeta{Name: "foreign", Namespace: "testing", UID: "foreign-stateful"}}
			invalid := sidecarArrivalPod(t, "app-invalid", "invalid-physical", desired)
			invalid.OwnerReferences = nil
			if tc.controller != nil {
				invalid.OwnerReferences = []meta.OwnerReference{*tc.controller}
			}
			sidecarSpoofWorkloadLabels(invalid, k8sapi.StatefulSetKind)
			good := sidecarArrivalPod(t, "app-current", "current-physical", desired)
			good.OwnerReferences = []meta.OwnerReference{sidecarOwnerReference(k8sapi.StatefulSetKind, work.Name, work.UID)}
			f := newSidecarArrivalFixture(t, work, foreign, invalid, good)
			f.watcher.Store(desired)
			ensureSidecarOwnerWithLaterRegistration(t, f, k8sapi.StatefulSet(work), invalid, good)
			var gotStateful bool
			for _, action := range f.client.Actions() {
				gotStateful = gotStateful || action.Matches("get", "statefulsets")
			}
			assert.True(t, gotStateful, "candidate must verify the current StatefulSet through a native GET")
		})
	}
}

func TestEnsureSidecarRequiresNativeArgoChainWithReplicaSetsDisabled(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		ref  meta.OwnerReference
	}{
		{name: "old rollout UID", ref: sidecarOwnerReferenceWithGroup("argoproj.io/v1alpha1", k8sapi.RolloutKind, "app", "previous-rollout")},
		{name: "missing rollout group", ref: sidecarOwnerReferenceWithGroup("", k8sapi.RolloutKind, "app", "current-rollout")},
		{name: "wrong rollout group", ref: sidecarOwnerReferenceWithGroup("other.io/v1alpha1", k8sapi.RolloutKind, "app", "current-rollout")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
			desired.WorkloadKind = k8sapi.RolloutKind
			work := &argorollouts.Rollout{ObjectMeta: meta.ObjectMeta{Name: "app", Namespace: "testing", UID: "current-rollout"}}
			invalidRS := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{Name: "argo-invalid", Namespace: "testing", UID: "invalid-rs", OwnerReferences: []meta.OwnerReference{tc.ref}}}
			goodRef := sidecarOwnerReferenceWithGroup("argoproj.io/v1alpha1", k8sapi.RolloutKind, work.Name, work.UID)
			goodRS := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{Name: "argo-current", Namespace: "testing", UID: "current-rs", OwnerReferences: []meta.OwnerReference{goodRef}}}
			invalid := sidecarArrivalPod(t, "app-invalid", "invalid-physical", desired)
			invalid.OwnerReferences = []meta.OwnerReference{sidecarOwnerReference(k8sapi.ReplicaSetKind, invalidRS.Name, invalidRS.UID)}
			sidecarSpoofWorkloadLabels(invalid, k8sapi.RolloutKind)
			good := sidecarArrivalPod(t, "app-current", "current-physical", desired)
			good.OwnerReferences = []meta.OwnerReference{sidecarOwnerReference(k8sapi.ReplicaSetKind, goodRS.Name, goodRS.UID)}
			f := newSidecarArrivalFixture(t, invalidRS, goodRS, invalid, good)
			argo := argofake.NewSimpleClientset(work)
			f.ctx = k8sapi.WithJoinedClientSetInterface(f.ctx, f.client, argo)
			f.watcher.Store(desired)
			ensureSidecarOwnerWithLaterRegistration(t, f, k8sapi.Rollout(work), invalid, good)
			var gotReplicaSet, gotRollout bool
			for _, action := range f.client.Actions() {
				gotReplicaSet = gotReplicaSet || action.Matches("get", "replicasets")
			}
			for _, action := range argo.Actions() {
				gotRollout = gotRollout || action.Matches("get", "rollouts")
			}
			assert.True(t, gotReplicaSet, "standalone ReplicaSet interception is disabled, but its owning intermediary must be read")
			assert.True(t, gotRollout, "candidate must resolve its native Argo controller without trusting Pod labels")
		})
	}
}

func TestEnsureSidecarAcceptsExactWorkloadOwnedByCustomOperator(t *testing.T) {
	t.Parallel()
	operator := meta.OwnerReference{APIVersion: "operator.example/v1alpha1", Kind: "Application", Name: "operator", UID: "operator-uid", Controller: ptr.To(true)}
	t.Run("deployment through native replica set", func(t *testing.T) {
		t.Parallel()
		desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
		good := sidecarArrivalPod(t, "app-current", "current-physical", desired)
		f := newSidecarArrivalFixture(t, good)
		work := f.work.DeepCopy()
		work.OwnerReferences = []meta.OwnerReference{operator}
		_, err := f.client.AppsV1().Deployments("testing").Update(f.ctx, work, meta.UpdateOptions{})
		require.NoError(t, err)
		f.watcher.Store(desired)
		f.register(good.Name, good.UID)
		got := sidecarResult(t, f.startWork(f.ctx, work, 160*time.Millisecond))
		require.NoError(t, got.err)
		require.Len(t, got.agents, 1)
		assert.Equal(t, string(good.UID), got.agents[0].PodUid)
	})
	t.Run("stateful workload through direct native pod", func(t *testing.T) {
		t.Parallel()
		desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
		desired.WorkloadKind = k8sapi.StatefulSetKind
		work := &apps.StatefulSet{ObjectMeta: meta.ObjectMeta{Name: "app", Namespace: "testing", UID: "current-stateful", OwnerReferences: []meta.OwnerReference{operator}}}
		good := sidecarArrivalPod(t, "app-current", "current-physical", desired)
		good.OwnerReferences = []meta.OwnerReference{sidecarOwnerReference(k8sapi.StatefulSetKind, work.Name, work.UID)}
		f := newSidecarArrivalFixture(t, work, good)
		f.watcher.Store(desired)
		f.register(good.Name, good.UID)
		got := sidecarResult(t, f.startKind(f.ctx, k8sapi.StatefulSet(work), 160*time.Millisecond))
		require.NoError(t, got.err)
		require.Len(t, got.agents, 1)
		assert.Equal(t, string(good.UID), got.agents[0].PodUid)
	})
}

func TestEnsureSidecarReturnsFromPhysicalControllerCycle(t *testing.T) {
	const subprocessKey = "TELEPRESENCE_TEST_SIDECAR_OWNER_CYCLE"
	if fixture := os.Getenv(subprocessKey); fixture != "" {
		desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
		pod := sidecarArrivalPod(t, "app-cyclic", "cyclic-physical", desired)
		pod.OwnerReferences = []meta.OwnerReference{sidecarOwnerReference(k8sapi.ReplicaSetKind, "cycle-a", "cycle-a")}
		next := "cycle-a"
		if fixture == "two native replica sets" {
			next = "cycle-b"
		}
		a := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{
			Name: "cycle-a", Namespace: "testing", UID: "cycle-a",
			OwnerReferences: []meta.OwnerReference{sidecarOwnerReference(k8sapi.ReplicaSetKind, next, k8stypes.UID(next))},
		}}
		objects := []runtime.Object{pod, a}
		if next == "cycle-b" {
			b := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{
				Name: "cycle-b", Namespace: "testing", UID: "cycle-b",
				OwnerReferences: []meta.OwnerReference{sidecarOwnerReference(k8sapi.ReplicaSetKind, "cycle-a", "cycle-a")},
			}}
			objects = append(objects, b)
		}
		f := newSidecarArrivalFixture(t, objects...)
		f.watcher.Store(desired)
		f.register(pod.Name, pod.UID)
		got := sidecarResult(t, f.start(f.ctx, 150*time.Millisecond))
		require.Error(t, got.err)
		assert.Empty(t, got.agents)
		return
	}
	t.Parallel()
	for _, fixture := range []string{"self-referencing native replica set", "two native replica sets"} {
		t.Run(fixture, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEnsureSidecarReturnsFromPhysicalControllerCycle$", "-test.count=1")
			command.Env = append(os.Environ(), subprocessKey+"="+fixture)
			output, err := command.CombinedOutput()
			if err != nil {
				if len(output) > 1500 {
					output = output[:1500]
				}
				t.Fatalf("manager did not return safely from %s: %v\n%s", fixture, err, output)
			}
		})
	}
}

func TestEnsureSidecarRechecksTransientPhysicalControllerWithoutRegistration(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	good := sidecarArrivalPod(t, "app-current", "current-physical", desired)
	f := newSidecarArrivalFixture(t, good)
	f.watcher.Store(desired)
	f.register(good.Name, good.UID)
	var calls atomic.Int32
	f.client.PrependReactor("get", "replicasets", func(ktesting.Action) (bool, runtime.Object, error) {
		if calls.Add(1) == 1 {
			return true, nil, apierrors.NewServerTimeout(apps.Resource("replicasets"), "get", 1)
		}
		return false, nil, nil
	})
	got := sidecarResult(t, f.start(f.ctx, 3*time.Second))
	require.NoError(t, got.err)
	require.Len(t, got.agents, 1)
	assert.Equal(t, string(good.UID), got.agents[0].PodUid)
	assert.EqualValues(t, 2, calls.Load())
}

func TestEnsureSidecarRechecksMissingPhysicalControllerWithoutRegistration(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	good := sidecarArrivalPod(t, "app-current", "current-physical", desired)
	good.OwnerReferences = []meta.OwnerReference{sidecarOwnerReference(k8sapi.ReplicaSetKind, "app-eventual", "eventual-rs")}
	f := newSidecarArrivalFixture(t, good)
	f.watcher.Store(desired)
	f.register(good.Name, good.UID)
	observed := make(chan struct{}, 1)
	f.client.PrependReactor("get", "replicasets", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.(ktesting.GetAction).GetName() == "app-eventual" {
			select {
			case observed <- struct{}{}:
			default:
			}
		}
		return false, nil, nil
	})
	result := f.start(f.ctx, 3*time.Second)
	select {
	case <-observed:
	case got := <-result:
		t.Fatalf("manager accepted candidate before its referenced physical controller existed: %+v", got)
	case <-time.After(2 * time.Second):
		t.Fatal("manager did not issue the native GET for its missing intermediate controller")
	}
	controller := sidecarOwnerReplicaSet("app-eventual", "eventual-rs", f.work.Name, f.work.UID)
	_, err := f.client.AppsV1().ReplicaSets("testing").Create(f.ctx, controller, meta.CreateOptions{})
	require.NoError(t, err)
	got := sidecarResult(t, result)
	require.NoError(t, got.err)
	require.Len(t, got.agents, 1)
	assert.Equal(t, string(good.UID), got.agents[0].PodUid)
}

func TestEnsureSidecarReturnsPermanentPhysicalControllerFailure(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	good := sidecarArrivalPod(t, "app-current", "current-physical", desired)
	f := newSidecarArrivalFixture(t, good)
	f.watcher.Store(desired)
	f.register(good.Name, good.UID)
	f.client.PrependReactor("get", "replicasets", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(apps.Resource("replicasets"), "app-rs", errors.New("denied"))
	})
	got := sidecarResult(t, f.start(f.ctx, 3*time.Second))
	require.ErrorContains(t, got.err, "replicasets.apps \"app-rs\" is forbidden")
	assert.Empty(t, got.agents)
}
