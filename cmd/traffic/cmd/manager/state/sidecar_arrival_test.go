package state

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typedcore "k8s.io/client-go/kubernetes/typed/core/v1"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	argofake "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/fake"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/mutator"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/cache"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

const sidecarArrivalImage = "registry.example/tel2:2.99.0"

type sidecarArrivalMap struct {
	mutator.Map
	attempts      chan struct{}
	drops         atomic.Int32
	evictionError error
}

func (m *sidecarArrivalMap) EvictPodsWithAgentConfigMismatch(context.Context, k8sapi.Workload, *agentconfig.Sidecar) error {
	err := m.evictionError
	m.attempts <- struct{}{}
	return err
}

func (m *sidecarArrivalMap) Delete(name, namespace string) {
	m.drops.Add(1)
	m.Map.Delete(name, namespace)
}

func (m *sidecarArrivalMap) Update(name, namespace string, update func(*agentconfig.Sidecar) (*agentconfig.Sidecar, error)) (*agentconfig.Sidecar, error) {
	return m.Map.Update(name, namespace, func(current *agentconfig.Sidecar) (*agentconfig.Sidecar, error) {
		next, err := update(current)
		if current != nil && next == nil && err == nil {
			m.drops.Add(1)
		}
		return next, err
	})
}

type sidecarPodLookupClient struct {
	kubernetes.Interface
	lookup func(context.Context, string, string) (*core.Pod, error)
}

func (c *sidecarPodLookupClient) CoreV1() typedcore.CoreV1Interface {
	return &sidecarCoreLookupClient{CoreV1Interface: c.Interface.CoreV1(), lookup: c.lookup}
}

type sidecarCoreLookupClient struct {
	typedcore.CoreV1Interface
	lookup func(context.Context, string, string) (*core.Pod, error)
}

func (c *sidecarCoreLookupClient) Pods(namespace string) typedcore.PodInterface {
	return &sidecarPodsLookupClient{PodInterface: c.CoreV1Interface.Pods(namespace), namespace: namespace, lookup: c.lookup}
}

type sidecarPodsLookupClient struct {
	typedcore.PodInterface
	namespace string
	lookup    func(context.Context, string, string) (*core.Pod, error)
}

func (c *sidecarPodsLookupClient) Get(ctx context.Context, name string, _ meta.GetOptions) (*core.Pod, error) {
	return c.lookup(ctx, c.namespace, name)
}

type sidecarRestoreObserver struct {
	mutator.Map
	afterUpdate func()
	dispatch    func(context.Context, k8sapi.Workload, *agentconfig.Sidecar) error
}

func (m *sidecarRestoreObserver) Update(name, namespace string, update func(*agentconfig.Sidecar) (*agentconfig.Sidecar, error)) (*agentconfig.Sidecar, error) {
	configured, err := m.Map.Update(name, namespace, update)
	if err == nil && m.afterUpdate != nil {
		m.afterUpdate()
	}
	return configured, err
}

func (m *sidecarRestoreObserver) EvictPodsWithAgentConfigMismatch(ctx context.Context, work k8sapi.Workload, configured *agentconfig.Sidecar) error {
	return m.dispatch(ctx, work, configured)
}

type sidecarArrivalFixture struct {
	ctx     context.Context
	client  *fake.Clientset
	watcher *sidecarArrivalMap
	state   *State
	work    *apps.Deployment
}

func newSidecarArrivalFixture(t *testing.T, objects ...runtime.Object) *sidecarArrivalFixture {
	t.Helper()
	work := &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: "app", Namespace: "testing", UID: "work-1"}}
	rs := sidecarOwnerReplicaSet("app-rs", "work-rs", "app", work.UID)
	client := fake.NewSimpleClientset(append([]runtime.Object{work, rs}, objects...)...)
	watcher := &sidecarArrivalMap{Map: mutator.NewWatcher(), attempts: make(chan struct{}, 8)}
	ctx := k8sapi.WithJoinedClientSetInterface(t.Context(), client, argofake.NewSimpleClientset())
	ctx = mutator.WithMap(ctx, watcher)
	ctx = managerutil.WithResolvedAgentImageRetriever(ctx, managerutil.ImageFromEnv(sidecarArrivalImage))
	s := &State{backgroundCtx: ctx, agents: cache.NewMap[tunnel.SessionID, *AgentSession](agentsEqual, time.Millisecond)}
	return &sidecarArrivalFixture{ctx: ctx, client: client, watcher: watcher, state: s, work: work}
}

func sidecarArrivalConfig(policy agentconfig.ReplacePolicy) *agentconfig.Sidecar {
	return &agentconfig.Sidecar{
		AgentImage: sidecarArrivalImage, AgentName: "app", Namespace: "testing", WorkloadName: "app", WorkloadKind: k8sapi.DeploymentKind,
		Containers: []*agentconfig.Container{{Name: "echo", Replace: policy}},
	}
}

func sidecarArrivalPod(t *testing.T, name string, uid k8stypes.UID, cfg *agentconfig.Sidecar) *core.Pod {
	t.Helper()
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{
		Name: name, Namespace: "testing", UID: uid,
		OwnerReferences: []meta.OwnerReference{sidecarOwnerReference(k8sapi.ReplicaSetKind, "app-rs", "work-rs")},
	}}
	if cfg != nil {
		value, err := agentconfig.MarshalTight(cfg)
		require.NoError(t, err)
		pod.Annotations = map[string]string{annotation.Config: value}
	}
	return pod
}

func (f *sidecarArrivalFixture) register(name string, uid k8stypes.UID) {
	f.state.agents.Store(tunnel.SessionID(name+string(uid)), &AgentSession{AgentInfo: &rpc.AgentInfo{
		Name: "app", Namespace: "testing", PodName: name, PodUid: string(uid),
	}})
}

type sidecarArrivalResult struct {
	agents []*AgentSession
	err    error
}

func (f *sidecarArrivalFixture) start(ctx context.Context, timeout time.Duration) <-chan sidecarArrivalResult {
	return f.startWork(ctx, f.work, timeout)
}

func (f *sidecarArrivalFixture) startWork(ctx context.Context, work *apps.Deployment, timeout time.Duration) <-chan sidecarArrivalResult {
	return f.startKind(ctx, k8sapi.Deployment(work), timeout)
}

func (f *sidecarArrivalFixture) startKind(ctx context.Context, work k8sapi.Workload, timeout time.Duration) <-chan sidecarArrivalResult {
	ctx = managerutil.WithEnv(ctx, &managerutil.Env{AgentInjectPolicy: agentconfig.OnDemand, AgentArrivalTimeout: timeout})
	result := make(chan sidecarArrivalResult, 1)
	go func() {
		_, agents, err := f.state.ensureAgent(ctx, work, false, false, nil, agentconfig.ReplacePolicyInactive)
		result <- sidecarArrivalResult{agents, err}
	}()
	return result
}

func sidecarAttempt(t *testing.T, f *sidecarArrivalFixture) {
	t.Helper()
	select {
	case <-f.watcher.attempts:
	case <-time.After(2 * time.Second):
		t.Fatal("manager never requested the sidecar configuration")
	}
}

func sidecarResult(t *testing.T, result <-chan sidecarArrivalResult) sidecarArrivalResult {
	t.Helper()
	select {
	case r := <-result:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("manager never finished its agent wait")
		return sidecarArrivalResult{}
	}
}

func sidecarWaitsRetired(t *testing.T, state *State) {
	t.Helper()
	groups := 0
	state.sidecarAgentWaits.Range(func(_, _ any) bool { groups++; return true })
	assert.Zero(t, groups, "completed requests must not retain a workload wait entry")
}

func TestEnsureSidecarWaitsForPhysicalReplacementConfiguration(t *testing.T) {
	t.Parallel()
	old := sidecarArrivalConfig(agentconfig.ReplacePolicyIntercept)
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	f := newSidecarArrivalFixture(t, sidecarArrivalPod(t, "app-old", "old", old), sidecarArrivalPod(t, "app-new", "new", desired))
	f.watcher.Store(desired)
	f.register("app-old", "old")
	result := f.start(f.ctx, 4*time.Second)
	sidecarAttempt(t, f)
	select {
	case got := <-result:
		t.Fatalf("manager returned the old physical agent before replacement registered: %+v", got)
	case <-time.After(150 * time.Millisecond):
	}
	f.register("app-new", "new")
	got := sidecarResult(t, result)
	require.NoError(t, got.err)
	require.Len(t, got.agents, 1)
	assert.Equal(t, "new", got.agents[0].PodUid)
}

func TestEnsureSidecarWaitsForRecreatedPhysicalPodWithSameName(t *testing.T) {
	t.Parallel()
	old := sidecarArrivalConfig(agentconfig.ReplacePolicyIntercept)
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	f := newSidecarArrivalFixture(t, sidecarArrivalPod(t, "app-0", "old", old))
	f.watcher.Store(desired)
	f.register("app-0", "old")
	result := f.start(f.ctx, 4*time.Second)
	sidecarAttempt(t, f)
	select {
	case got := <-result:
		t.Fatalf("manager returned the previous physical pod: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, f.client.CoreV1().Pods("testing").Delete(f.ctx, "app-0", meta.DeleteOptions{}))
	_, err := f.client.CoreV1().Pods("testing").Create(f.ctx, sidecarArrivalPod(t, "app-0", "new", desired), meta.CreateOptions{})
	require.NoError(t, err)
	f.register("app-0", "new")
	got := sidecarResult(t, result)
	require.NoError(t, got.err)
	require.Len(t, got.agents, 1)
	assert.Equal(t, "new", got.agents[0].PodUid)
}

func TestEnsureSidecarRejectsInvalidPhysicalCandidate(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	terminating := sidecarArrivalPod(t, "app-0", "reported", desired)
	now := meta.Now()
	terminating.DeletionTimestamp = &now
	for _, tc := range []struct {
		name string
		pod  *core.Pod
	}{
		{name: "wrong UID", pod: sidecarArrivalPod(t, "app-0", "different", desired)},
		{name: "terminating", pod: terminating},
		{name: "missing annotation", pod: sidecarArrivalPod(t, "app-0", "reported", nil)},
		{name: "missing pod"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var objects []runtime.Object
			if tc.pod != nil {
				objects = append(objects, tc.pod)
			}
			f := newSidecarArrivalFixture(t, objects...)
			f.watcher.Store(sidecarArrivalConfig(agentconfig.ReplacePolicyContainer))
			f.register("app-0", "reported")
			got := sidecarResult(t, f.start(f.ctx, 120*time.Millisecond))
			require.Error(t, got.err)
			assert.Empty(t, got.agents)
		})
	}
}

func TestEnsureSidecarAcceptsMatchingRegisteredPodBeforeKubernetesReady(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	pod := sidecarArrivalPod(t, "app-0", "reported", desired)
	pod.Status.Conditions = []core.PodCondition{{Type: core.PodReady, Status: core.ConditionFalse}}
	f := newSidecarArrivalFixture(t, pod)
	f.watcher.Store(desired)
	f.register("app-0", "reported")
	got := sidecarResult(t, f.start(f.ctx, 2*time.Second))
	require.NoError(t, got.err)
	require.Len(t, got.agents, 1)
	assert.Equal(t, "reported", got.agents[0].PodUid)
	var podGets int
	for _, action := range f.client.Actions() {
		if action.Matches("get", "pods") {
			podGets++
		}
	}
	assert.Equal(t, 1, podGets)
}

func TestEnsureSidecarDisabledInjectorKeepsManualAgentSelection(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	config, err := agentconfig.MarshalTight(desired)
	require.NoError(t, err)
	f := newSidecarArrivalFixture(t)
	f.work.Spec.Template.Annotations = map[string]string{annotation.Config: config}
	f.register("app-manual", "manual")
	ctx := managerutil.WithEnv(f.ctx, &managerutil.Env{AgentInjectPolicy: agentconfig.Never})
	_, agents, err := f.state.ensureAgent(ctx, k8sapi.Deployment(f.work), false, false, nil, agentconfig.ReplacePolicyInactive)
	require.NoError(t, err)
	require.Len(t, agents, 1)
	assert.Equal(t, "manual", agents[0].PodUid)
	assert.Empty(t, f.client.Actions())
}

func TestEnsureSidecarRechecksTransientPodLookupWithoutRegistration(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	pod := sidecarArrivalPod(t, "app-0", "reported", desired)
	f := newSidecarArrivalFixture(t, pod)
	f.watcher.Store(desired)
	f.register("app-0", "reported")
	var calls atomic.Int32
	f.client.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if calls.Add(1) == 1 {
			return true, nil, apierrors.NewServerTimeout(core.Resource("pods"), "get", 1)
		}
		return false, nil, nil
	})
	got := sidecarResult(t, f.start(f.ctx, 3*time.Second))
	require.NoError(t, got.err)
	require.Len(t, got.agents, 1)
	assert.Equal(t, "reported", got.agents[0].PodUid)
	assert.EqualValues(t, 2, calls.Load())
}

func TestEnsureSidecarReturnsPermanentPhysicalLookupFailure(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	f := newSidecarArrivalFixture(t)
	f.watcher.Store(desired)
	f.register("app-0", "reported")
	f.client.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(core.Resource("pods"), "app-0", errors.New("denied"))
	})
	got := sidecarResult(t, f.start(f.ctx, 2*time.Second))
	require.Error(t, got.err)
	assert.ErrorContains(t, got.err, "pods \"app-0\" is forbidden")
	assert.Empty(t, got.agents)
}

func TestEnsureSidecarAcceptsVerifiedCandidateWhenAnotherLookupIsForbidden(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	pod := sidecarArrivalPod(t, "app-verified", "verified", desired)
	f := newSidecarArrivalFixture(t, pod)
	f.watcher.Store(desired)
	f.register("app-denied", "denied")
	f.register("app-verified", "verified")
	f.client.PrependReactor("get", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.(ktesting.GetAction).GetName() == "app-denied" {
			return true, nil, apierrors.NewForbidden(core.Resource("pods"), "app-denied", errors.New("denied"))
		}
		return false, nil, nil
	})
	got := sidecarResult(t, f.start(f.ctx, 2*time.Second))
	require.NoError(t, got.err)
	require.Len(t, got.agents, 1)
	assert.Equal(t, "verified", got.agents[0].PodUid)
}

func TestEnsureSidecarDoesNotRetryRemovedOrBlacklistedSession(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		stop func(*sidecarArrivalFixture)
	}{
		{name: "removed", stop: func(f *sidecarArrivalFixture) { f.state.agents.Delete(tunnel.SessionID("app-oldold")) }},
		{name: "blacklisted", stop: func(f *sidecarArrivalFixture) { f.watcher.Inactivate("old") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
			f := newSidecarArrivalFixture(t, sidecarArrivalPod(t, "app-good", "good", desired))
			f.watcher.Store(desired)
			f.register("app-old", "old")
			var calls atomic.Int32
			observed := make(chan struct{}, 1)
			f.client.PrependReactor("get", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.(ktesting.GetAction).GetName() != "app-old" {
					return false, nil, nil
				}
				if calls.Add(1) == 1 {
					observed <- struct{}{}
				}
				return true, nil, apierrors.NewServerTimeout(core.Resource("pods"), "get", 1)
			})
			result := f.start(f.ctx, 4*time.Second)
			select {
			case <-observed:
			case <-time.After(2 * time.Second):
				t.Fatal("manager did not check the old physical pod")
			}
			tc.stop(f)
			select {
			case got := <-result:
				t.Fatalf("manager returned a missing agent: %+v", got)
			case <-time.After(650 * time.Millisecond):
			}
			assert.EqualValues(t, 1, calls.Load())
			f.register("app-good", "good")
			got := sidecarResult(t, result)
			require.NoError(t, got.err)
			require.Len(t, got.agents, 1)
			assert.Equal(t, "good", got.agents[0].PodUid)
		})
	}
}

func TestEnsureSidecarTimeoutPreservesNewerDesiredConfiguration(t *testing.T) {
	t.Parallel()
	f := newSidecarArrivalFixture(t)
	old := sidecarArrivalConfig(agentconfig.ReplacePolicyIntercept)
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	f.watcher.Store(old)
	first := f.start(f.ctx, 250*time.Millisecond)
	sidecarAttempt(t, f)
	f.watcher.Store(desired)
	secondCtx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	second := f.start(secondCtx, 3*time.Second)
	sidecarAttempt(t, f)
	got := sidecarResult(t, first)
	require.Error(t, got.err)
	assert.Equal(t, desired, f.watcher.Get("app", "testing"))
	cancel()
	_ = sidecarResult(t, second)
}

func TestEnsureSidecarOlderTimeoutDoesNotDropIdenticalConfigWhileNewerWaits(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	f := newSidecarArrivalFixture(t, sidecarArrivalPod(t, "app-0", "physical", desired))
	f.watcher.Store(desired)
	older := f.start(f.ctx, 180*time.Millisecond)
	sidecarAttempt(t, f)
	newer := f.start(f.ctx, 3*time.Second)
	sidecarAttempt(t, f)
	got := sidecarResult(t, older)
	require.Error(t, got.err)
	assert.Equal(t, desired, f.watcher.Get("app", "testing"))
	assert.Zero(t, f.watcher.drops.Load())
	f.register("app-0", "physical")
	got = sidecarResult(t, newer)
	require.NoError(t, got.err)
	require.Len(t, got.agents, 1)
	assert.Equal(t, "physical", got.agents[0].PodUid)
	assert.Equal(t, desired, f.watcher.Get("app", "testing"))
	assert.Zero(t, f.watcher.drops.Load())
	sidecarWaitsRetired(t, f.state)
}

func TestEnsureSidecarOlderTimeoutDoesNotDropIdenticalConfigAfterNewerSucceeds(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	f := newSidecarArrivalFixture(t, sidecarArrivalPod(t, "app-0", "physical", desired))
	f.watcher.Store(desired)
	f.register("app-0", "physical")
	observed := make(chan struct{}, 1)
	olderClient := &sidecarPodLookupClient{Interface: f.client, lookup: func(_ context.Context, namespace, name string) (*core.Pod, error) {
		assert.Equal(t, "testing", namespace)
		assert.Equal(t, "app-0", name)
		select {
		case observed <- struct{}{}:
		default:
		}
		return nil, apierrors.NewServerTimeout(core.Resource("pods"), "get", 1)
	}}
	olderCtx := k8sapi.WithJoinedClientSetInterface(f.ctx, olderClient, argofake.NewSimpleClientset())
	older := f.start(olderCtx, 250*time.Millisecond)
	sidecarAttempt(t, f)
	select {
	case <-observed:
	case <-time.After(2 * time.Second):
		t.Fatal("older manager Ensure never performed its independent physical Pod lookup")
	}
	newer := f.start(f.ctx, 3*time.Second)
	sidecarAttempt(t, f)
	got := sidecarResult(t, newer)
	require.NoError(t, got.err)
	require.Len(t, got.agents, 1)
	assert.Equal(t, "physical", got.agents[0].PodUid)
	got = sidecarResult(t, older)
	require.Error(t, got.err)
	assert.Equal(t, desired, f.watcher.Get("app", "testing"))
	assert.Zero(t, f.watcher.drops.Load())
	sidecarWaitsRetired(t, f.state)
}

func TestEnsureSidecarAllIdenticalWaitersFailAndDropConfigurationOnce(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	f := newSidecarArrivalFixture(t)
	f.watcher.Store(desired)
	older := f.start(f.ctx, 90*time.Millisecond)
	sidecarAttempt(t, f)
	newer := f.start(f.ctx, 180*time.Millisecond)
	sidecarAttempt(t, f)
	got := sidecarResult(t, older)
	require.Error(t, got.err)
	assert.Equal(t, desired, f.watcher.Get("app", "testing"))
	assert.Zero(t, f.watcher.drops.Load())
	got = sidecarResult(t, newer)
	require.Error(t, got.err)
	assert.Nil(t, f.watcher.Get("app", "testing"))
	assert.EqualValues(t, 1, f.watcher.drops.Load())
	sidecarWaitsRetired(t, f.state)
}

func TestEnsureSidecarPreviousWorkloadSuccessDoesNotProtectRecreatedFailure(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	f := newSidecarArrivalFixture(t, sidecarArrivalPod(t, "app-0", "physical", desired))
	f.watcher.Store(desired)
	f.register("app-0", "physical")
	observed := make(chan struct{}, 1)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	oldClient := &sidecarPodLookupClient{Interface: f.client, lookup: func(ctx context.Context, namespace, name string) (*core.Pod, error) {
		select {
		case observed <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return f.client.CoreV1().Pods(namespace).Get(ctx, name, meta.GetOptions{})
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	oldCtx := k8sapi.WithJoinedClientSetInterface(f.ctx, oldClient, argofake.NewSimpleClientset())
	old := f.start(oldCtx, 3*time.Second)
	sidecarAttempt(t, f)
	select {
	case <-observed:
	case <-time.After(2 * time.Second):
		t.Fatal("previous workload Ensure did not start its physical Pod lookup")
	}
	recreated := f.work.DeepCopy()
	recreated.UID = "work-2"
	newClient := &sidecarPodLookupClient{Interface: f.client, lookup: func(context.Context, string, string) (*core.Pod, error) {
		return nil, apierrors.NewServerTimeout(core.Resource("pods"), "get", 1)
	}}
	newCtx := k8sapi.WithJoinedClientSetInterface(f.ctx, newClient, argofake.NewSimpleClientset())
	latest := f.startWork(newCtx, recreated, 550*time.Millisecond)
	sidecarAttempt(t, f)
	close(release)
	got := sidecarResult(t, old)
	require.NoError(t, got.err)
	require.Len(t, got.agents, 1)
	assert.Equal(t, "physical", got.agents[0].PodUid)
	require.NoError(t, f.client.AppsV1().Deployments("testing").Delete(f.ctx, recreated.Name, meta.DeleteOptions{}))
	_, err := f.client.AppsV1().Deployments("testing").Create(f.ctx, recreated, meta.CreateOptions{})
	require.NoError(t, err)
	f.watcher.Store(desired.Clone())
	got = sidecarResult(t, latest)
	require.Error(t, got.err)
	assert.Nil(t, f.watcher.Get("app", "testing"))
	assert.EqualValues(t, 1, f.watcher.drops.Load())
	sidecarWaitsRetired(t, f.state)
}

func TestEnsureSidecarNewerFailureBeforePublicationDoesNotVetoOlderTimeout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		setup    func(*sidecarArrivalFixture) context.Context
		expected string
	}{
		{name: "warning watch", setup: func(f *sidecarArrivalFixture) context.Context {
			client := fake.NewSimpleClientset(f.work)
			client.PrependWatchReactor("events", func(ktesting.Action) (bool, watch.Interface, error) {
				return true, nil, errors.New("warning watch denied")
			})
			return k8sapi.WithJoinedClientSetInterface(f.ctx, client, argofake.NewSimpleClientset())
		}, expected: "warning watch denied"},
		{name: "configuration image", setup: func(f *sidecarArrivalFixture) context.Context {
			return managerutil.WithResolvedAgentImageRetriever(f.ctx, managerutil.ImageFromEnv(""))
		}, expected: "unable to determine what image"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newSidecarArrivalFixture(t)
			f.watcher.Store(sidecarArrivalConfig(agentconfig.ReplacePolicyContainer))
			older := f.start(f.ctx, 180*time.Millisecond)
			sidecarAttempt(t, f)
			newer := f.start(tc.setup(f), 3*time.Second)
			got := sidecarResult(t, newer)
			require.ErrorContains(t, got.err, tc.expected)
			got = sidecarResult(t, older)
			require.Error(t, got.err)
			assert.Nil(t, f.watcher.Get("app", "testing"))
			assert.EqualValues(t, 1, f.watcher.drops.Load())
			sidecarWaitsRetired(t, f.state)
		})
	}
}

func TestEnsureSidecarNewerDirectEvictionFailureKeepsItsPublishedConfiguration(t *testing.T) {
	t.Parallel()
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	f := newSidecarArrivalFixture(t)
	f.watcher.Store(desired)
	older := f.start(f.ctx, 180*time.Millisecond)
	sidecarAttempt(t, f)
	f.watcher.evictionError = errors.New("eviction denied")
	newer := f.start(f.ctx, 3*time.Second)
	sidecarAttempt(t, f)
	got := sidecarResult(t, newer)
	require.ErrorContains(t, got.err, "eviction denied")
	got = sidecarResult(t, older)
	require.Error(t, got.err)
	assert.Equal(t, desired, f.watcher.Get("app", "testing"))
	assert.Zero(t, f.watcher.drops.Load())
	sidecarWaitsRetired(t, f.state)
}

func TestEnsureSidecarReleasesWaitOwnershipOnErrorsBeforeAgentWait(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		setup    func(*sidecarArrivalFixture) context.Context
		expected string
	}{
		{name: "Kubernetes warning watch failure", setup: func(f *sidecarArrivalFixture) context.Context {
			f.client.PrependWatchReactor("events", func(ktesting.Action) (bool, watch.Interface, error) {
				return true, nil, errors.New("warning watch denied")
			})
			return f.ctx
		}, expected: "warning watch denied"},
		{name: "configuration image error", setup: func(f *sidecarArrivalFixture) context.Context {
			return managerutil.WithResolvedAgentImageRetriever(f.ctx, managerutil.ImageFromEnv(""))
		}, expected: "unable to determine what image"},
		{name: "direct eviction failure", setup: func(f *sidecarArrivalFixture) context.Context {
			f.watcher.evictionError = errors.New("eviction denied")
			return f.ctx
		}, expected: "eviction denied"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newSidecarArrivalFixture(t)
			desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
			f.watcher.Store(desired)
			got := sidecarResult(t, f.start(tc.setup(f), 2*time.Second))
			require.ErrorContains(t, got.err, tc.expected)
			assert.Equal(t, desired, f.watcher.Get("app", "testing"))
			assert.Zero(t, f.watcher.drops.Load())
			sidecarWaitsRetired(t, f.state)
		})
	}
}

func TestEnsureSidecarTimeoutPreservesRecreatedWorkloadWithSameConfiguration(t *testing.T) {
	t.Parallel()
	f := newSidecarArrivalFixture(t)
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	f.watcher.Store(desired)
	result := f.start(f.ctx, 150*time.Millisecond)
	sidecarAttempt(t, f)
	recreated := f.work.DeepCopy()
	recreated.UID = "work-2"
	_, err := f.client.AppsV1().Deployments("testing").Update(f.ctx, recreated, meta.UpdateOptions{})
	require.NoError(t, err)
	f.watcher.Store(desired.Clone())
	got := sidecarResult(t, result)
	require.Error(t, got.err)
	assert.Equal(t, desired, f.watcher.Get("app", "testing"))
}

func TestEnsureSidecarTimeoutDropsOnlyOwnConfirmedConfiguration(t *testing.T) {
	t.Parallel()
	f := newSidecarArrivalFixture(t)
	f.watcher.Store(sidecarArrivalConfig(agentconfig.ReplacePolicyContainer))
	got := sidecarResult(t, f.start(f.ctx, 80*time.Millisecond))
	require.Error(t, got.err)
	assert.Nil(t, f.watcher.Get("app", "testing"))
}

func TestEnsureSidecarTimeoutKeepsConfigurationWhenWorkloadLookupFails(t *testing.T) {
	t.Parallel()
	f := newSidecarArrivalFixture(t)
	desired := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	f.watcher.Store(desired)
	f.client.PrependReactor("get", "deployments", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServerTimeout(apps.Resource("deployments"), "get", 1)
	})
	got := sidecarResult(t, f.start(f.ctx, 80*time.Millisecond))
	require.Error(t, got.err)
	assert.Equal(t, desired, f.watcher.Get("app", "testing"))
}

func TestRemoveReplacedInterceptDispatchesPublishedRestoredConfiguration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		noDefault bool
		policy    agentconfig.ReplacePolicy
	}{
		{name: "retain intercepting sidecar", policy: agentconfig.ReplacePolicyIntercept},
		{name: "inactivate sidecar without default port", noDefault: true, policy: agentconfig.ReplacePolicyInactive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			work := &apps.StatefulSet{
				ObjectMeta: meta.ObjectMeta{Name: "app", Namespace: "testing", UID: "work", Generation: 1},
				Spec: apps.StatefulSetSpec{
					Replicas: ptr.To(int32(1)), Selector: &meta.LabelSelector{MatchLabels: map[string]string{"app": "echo"}},
					Template: core.PodTemplateSpec{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{"app": "echo"}}},
				},
				Status: apps.StatefulSetStatus{ObservedGeneration: 1, Replicas: 1, CurrentReplicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1},
			}
			configured := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
			configured.WorkloadKind = k8sapi.StatefulSetKind
			configured.Containers[0].Intercepts = []*agentconfig.Intercept{{ServiceName: "app", ServiceUID: "service", ServicePort: 80, ContainerPort: 8080}}
			pod := sidecarArrivalPod(t, "app-0", "physical", configured)
			pod.Labels = map[string]string{"app": "echo", agentconfig.WorkloadNameLabel: work.Name, agentconfig.WorkloadKindLabel: string(k8sapi.StatefulSetKind)}
			pod.OwnerReferences = []meta.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: work.Name, UID: work.UID, Controller: ptr.To(true)}}
			pod.Status = core.PodStatus{Phase: core.PodRunning, Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}}}
			client := fake.NewSimpleClientset(work, pod)
			ctx := k8sapi.WithJoinedClientSetInterface(t.Context(), client, argofake.NewSimpleClientset())
			ctx = informer.WithFactory(ctx, "")
			ctx = managerutil.WithEnv(ctx, &managerutil.Env{EnabledWorkloadKinds: k8sapi.Kinds{k8sapi.StatefulSetKind}})
			watcher := mutator.NewWatcher()
			watcher.Store(configured)
			ctx = mutator.WithMap(ctx, watcher)
			factory := informer.GetK8sFactory(ctx, "testing")
			require.NoError(t, factory.Apps().V1().StatefulSets().Informer().GetStore().Add(work.DeepCopy()))
			require.NoError(t, factory.Core().V1().Pods().Informer().GetStore().Add(pod.DeepCopy()))
			evicted := make(chan *policy.Eviction, 2)
			client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "eviction" {
					return false, nil, nil
				}
				evicted <- action.(ktesting.CreateAction).GetObject().(*policy.Eviction).DeepCopy()
				return true, nil, nil
			})
			s := &State{backgroundCtx: ctx, intercepts: cache.NewMap[string, *Intercept](interceptEqual, time.Millisecond)}
			s.RestoreIntercepts(ctx, []*rpc.InterceptInfo{{Id: "replaced", Spec: &rpc.InterceptSpec{
				Name: "replace", Agent: work.Name, Namespace: work.Namespace, WorkloadKind: string(k8sapi.StatefulSetKind),
				ContainerName: "echo", ServiceName: "app", PortIdentifier: "80", NoDefaultPort: tc.noDefault, Replace: true,
			}}}, time.Now())
			s.RemoveIntercept("replaced")
			_, exists := s.intercepts.Load("replaced")
			assert.False(t, exists)
			current := watcher.Get(work.Name, work.Namespace)
			require.NotNil(t, current)
			assert.Equal(t, tc.policy, current.Containers[0].Replace)
			select {
			case eviction := <-evicted:
				assert.Equal(t, pod.Name, eviction.Name)
				require.NotNil(t, eviction.DeleteOptions)
				require.NotNil(t, eviction.DeleteOptions.Preconditions)
				assert.Equal(t, ptr.To(pod.UID), eviction.DeleteOptions.Preconditions.UID)
			case <-time.After(150 * time.Millisecond):
				t.Fatal("removing replace did not evict the physical Ready pod carrying the replaced configuration")
			}
			assert.Empty(t, evicted)
		})
	}
}

func TestRestoringSidecarReturnsDispatchFailureAfterPublishingConfiguration(t *testing.T) {
	t.Parallel()
	f := newSidecarArrivalFixture(t)
	base := mutator.NewWatcher()
	base.Store(sidecarArrivalConfig(agentconfig.ReplacePolicyContainer))
	failure := errors.New("eviction denied")
	observed := &sidecarRestoreObserver{
		Map: base,
		dispatch: func(_ context.Context, _ k8sapi.Workload, requested *agentconfig.Sidecar) error {
			assert.Equal(t, agentconfig.ReplacePolicyInactive, requested.Containers[0].Replace)
			assert.Equal(t, agentconfig.ReplacePolicyInactive, base.Get("app", "testing").Containers[0].Replace)
			return failure
		},
	}
	ctx := mutator.WithMap(f.ctx, observed)
	err := f.state.restoreAppContainer(ctx, &rpc.InterceptInfo{Id: "replace", Spec: &rpc.InterceptSpec{
		Agent: "app", Namespace: "testing", ContainerName: "echo", NoDefaultPort: true,
	}}, k8sapi.Deployment(f.work))
	assert.ErrorIs(t, err, failure)
	assert.Equal(t, agentconfig.ReplacePolicyInactive, base.Get("app", "testing").Containers[0].Replace)
}

func TestRestoringSidecarDoesNotOverwriteConfigurationSupersedingDispatch(t *testing.T) {
	t.Parallel()
	f := newSidecarArrivalFixture(t)
	base := mutator.NewWatcher()
	base.Store(sidecarArrivalConfig(agentconfig.ReplacePolicyContainer))
	superseding := sidecarArrivalConfig(agentconfig.ReplacePolicyIntercept)
	observed := &sidecarRestoreObserver{
		Map:         base,
		afterUpdate: func() { base.Store(superseding) },
		dispatch: func(_ context.Context, _ k8sapi.Workload, requested *agentconfig.Sidecar) error {
			assert.Equal(t, agentconfig.ReplacePolicyInactive, requested.Containers[0].Replace)
			assert.Equal(t, agentconfig.ReplacePolicyIntercept, base.Get("app", "testing").Containers[0].Replace)
			return nil
		},
	}
	ctx := mutator.WithMap(f.ctx, observed)
	err := f.state.restoreAppContainer(ctx, &rpc.InterceptInfo{Id: "replace", Spec: &rpc.InterceptSpec{
		Agent: "app", Namespace: "testing", ContainerName: "echo", NoDefaultPort: true,
	}}, k8sapi.Deployment(f.work))
	assert.NoError(t, err)
	assert.Equal(t, superseding, base.Get("app", "testing"))
}

func TestRemoveRestoredNodeInterceptDoesNotMutateSameWorkloadSidecar(t *testing.T) {
	t.Parallel()
	work := &apps.StatefulSet{
		ObjectMeta: meta.ObjectMeta{Name: "app", Namespace: "testing", UID: "work", Generation: 1},
		Spec: apps.StatefulSetSpec{
			Replicas: ptr.To(int32(1)), Selector: &meta.LabelSelector{MatchLabels: map[string]string{"app": "echo"}},
			Template: core.PodTemplateSpec{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{"app": "echo"}}},
		},
		Status: apps.StatefulSetStatus{ObservedGeneration: 1, Replicas: 1, CurrentReplicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1},
	}
	configured := sidecarArrivalConfig(agentconfig.ReplacePolicyContainer)
	configured.WorkloadKind = k8sapi.StatefulSetKind
	pod := sidecarArrivalPod(t, "app-0", "physical-sidecar", configured)
	pod.Labels = map[string]string{"app": "echo", agentconfig.WorkloadNameLabel: work.Name, agentconfig.WorkloadKindLabel: string(k8sapi.StatefulSetKind)}
	pod.OwnerReferences = []meta.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: work.Name, UID: work.UID, Controller: ptr.To(true)}}
	pod.Spec.Containers = []core.Container{{Name: "echo"}, {Name: agentconfig.ContainerName}}
	pod.Spec.NodeName = "node"
	pod.Status = core.PodStatus{Phase: core.PodRunning, PodIP: "10.10.0.5", Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}}}
	job := &batch.Job{ObjectMeta: meta.ObjectMeta{Name: "tel-node-agent-app", Namespace: "ambassador", Labels: map[string]string{
		nodeAgentAppLabel: nodeAgentAppLabelValue, nodeAgentNameLabel: work.Name, nodeAgentNamespaceLabel: work.Namespace, nodeAgentTargetPodLabel: pod.Name,
	}}}
	unrelated := job.DeepCopy()
	unrelated.Name = "tel-node-agent-other"
	unrelated.Labels[nodeAgentNameLabel] = "other"
	client := fake.NewSimpleClientset(work, pod, job, unrelated)
	installDeleteCollectionReactor(client)
	ctx := k8sapi.WithJoinedClientSetInterface(t.Context(), client, argofake.NewSimpleClientset())
	ctx = informer.WithFactory(ctx, "")
	ctx = managerutil.WithEnv(ctx, &managerutil.Env{ManagerNamespace: "ambassador", EnabledWorkloadKinds: k8sapi.Kinds{k8sapi.StatefulSetKind}})
	watcher := mutator.NewWatcher()
	watcher.Store(configured)
	ctx = mutator.WithMap(ctx, watcher)
	factory := informer.GetK8sFactory(ctx, "testing")
	require.NoError(t, factory.Apps().V1().StatefulSets().Informer().GetStore().Add(work.DeepCopy()))
	require.NoError(t, factory.Core().V1().Pods().Informer().GetStore().Add(pod.DeepCopy()))
	evicted := make(chan *policy.Eviction, 2)
	client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "eviction" {
			return false, nil, nil
		}
		evicted <- action.(ktesting.CreateAction).GetObject().(*policy.Eviction).DeepCopy()
		return true, nil, nil
	})
	s := newNodeAgentWatchTestState(ctx)
	s.RestoreIntercepts(ctx, []*rpc.InterceptInfo{{Id: "node", Disposition: rpc.InterceptDispositionType_ACTIVE, Spec: &rpc.InterceptSpec{
		Name: "node", Agent: work.Name, Namespace: work.Namespace, WorkloadKind: string(k8sapi.StatefulSetKind),
		ContainerName: "echo", NoDefaultPort: true, NodeAgent: true,
	}}}, time.Now())
	key := nodeAgentWatchKey{name: work.Name, namespace: work.Namespace}
	_, watching := s.nodeAgentPodWatchers.Load(key)
	require.True(t, watching)
	require.Eventually(t, func() bool {
		for _, action := range client.Actions() {
			if action.Matches("watch", "pods") {
				return true
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond)
	_, err := client.BatchV1().Jobs(job.Namespace).Get(ctx, job.Name, meta.GetOptions{})
	require.NoError(t, err)
	s.RemoveIntercept("node")
	_, err = client.BatchV1().Jobs(job.Namespace).Get(ctx, job.Name, meta.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err), "last restored node claim should still reap its own job")
	_, err = client.BatchV1().Jobs(unrelated.Namespace).Get(ctx, unrelated.Name, meta.GetOptions{})
	assert.NoError(t, err, "unrelated node agent job should remain")
	assert.Eventually(t, func() bool { _, present := s.nodeAgentPodWatchers.Load(key); return !present }, 2*time.Second, 5*time.Millisecond)
	current := watcher.Get(work.Name, work.Namespace)
	require.NotNil(t, current)
	assert.Equal(t, agentconfig.ReplacePolicyContainer, current.Containers[0].Replace, "node detach must not change same-workload cached sidecar")
	assert.Empty(t, evicted, "node detach must not evict the independently configured physical sidecar pod")
}
