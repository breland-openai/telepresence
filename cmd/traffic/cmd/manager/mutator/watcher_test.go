package mutator

import (
	"testing"
	"time"

	argofake "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/fake"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/namespaces"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/labels"
)

func TestAgentConfigUpdatesDoNotBlockUnrelatedWorkloads(t *testing.T) {
	cw := NewWatcher().(*configWatcher)
	namespace := "app-space"
	cw.Store(&agentconfig.Sidecar{AgentName: "blocked", Namespace: namespace})
	want := &agentconfig.Sidecar{AgentName: "ready", Namespace: namespace}
	cw.Store(want)

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		defer close(done)
		_, err := cw.Update("blocked", namespace, func(sc *agentconfig.Sidecar) (*agentconfig.Sidecar, error) {
			close(entered)
			<-release
			return sc, nil
		})
		errCh <- err
	}()

	<-entered
	got := make(chan *agentconfig.Sidecar, 1)
	go func() {
		got <- cw.Get("ready", namespace)
	}()

	select {
	case sc := <-got:
		require.Equal(t, want, sc)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("unrelated workload config read blocked behind another workload update")
	}

	close(release)
	<-done
	require.NoError(t, <-errCh)
}

func TestRegenerateAgentMapsSkipsUnannotatedPodOwner(t *testing.T) {
	const ns = "testing"
	wl, obj := reconciliationTestWorkload(k8sapi.StatefulSetKind, "echo", ns)
	cfg := reconciliationTestConfig(wl, agentconfig.ReplacePolicyIntercept)
	encoded, err := agentconfig.MarshalTight(cfg)
	require.NoError(t, err)
	agent := reconciliationTestPod(wl, "agent", encoded, true)

	other, _ := reconciliationTestWorkload(k8sapi.StatefulSetKind, "other", ns)
	pod := reconciliationTestPod(other, "plain", "", true)
	delete(pod.Annotations, annotation.Config)

	client := fake.NewSimpleClientset()
	busy := apierrors.NewTooManyRequests("owner unavailable", 1)
	client.PrependReactor("get", "statefulsets", func(action ktesting.Action) (bool, runtime.Object, error) {
		require.Equal(t, other.GetName(), action.(ktesting.GetAction).GetName())
		return true, nil, busy
	})
	ctx := k8sapi.WithJoinedClientSetInterface(t.Context(), client, argofake.NewSimpleClientset())
	ctx = informer.WithFactory(ctx, "")
	ctx = managerutil.WithEnv(ctx, &managerutil.Env{EnabledWorkloadKinds: k8sapi.Kinds{k8sapi.StatefulSetKind}})
	selector := make(chan *labels.Selector, 1)
	selector <- &labels.Selector{MatchExpressions: []*labels.Requirement{{
		Key: labels.NameLabelKey, Operator: labels.OperatorIn, Values: []string{ns},
	}}}
	ctx, err = namespaces.InitContext(ctx, selector)
	require.NoError(t, err)
	client.ClearActions()
	f := informer.GetK8sFactory(ctx, ns)
	require.NoError(t, f.Apps().V1().StatefulSets().Informer().GetStore().Add(obj))
	pods := f.Core().V1().Pods().Informer().GetStore()
	require.NoError(t, pods.Add(agent))
	require.NoError(t, pods.Add(pod))

	cw := NewWatcher()
	require.NoError(t, cw.RegenerateAgentMaps(ctx, cfg.AgentImage))
	require.Equal(t, cfg, cw.Get(wl.GetName(), ns))
	require.Empty(t, client.Actions())

	// An annotated pod still needs an authoritative owner if the cache misses.
	annotated := pod.DeepCopy()
	annotated.Annotations[annotation.Config] = encoded
	require.NoError(t, pods.Update(annotated))
	require.ErrorIs(t, cw.RegenerateAgentMaps(ctx, cfg.AgentImage), busy)
	require.Len(t, client.Actions(), 1)
}
