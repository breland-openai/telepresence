package mutator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	features "k8s.io/client-go/features"
	featurestest "k8s.io/client-go/features/testing"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func TestVerifiedPodInventory(t *testing.T) {
	featurestest.SetFeatureDuringTest(t, features.WatchListClient, false)
	wl, obj := reconciliationTestWorkload(k8sapi.DeploymentKind, "echo", "default")
	wl.SetResourceVersion("1")
	config := reconciliationTestConfig(wl, agentconfig.ReplacePolicyIntercept)
	encoded, err := agentconfig.MarshalTight(config)
	require.NoError(t, err)
	pod := reconciliationTestPod(wl, "current", encoded, false)
	pod.ResourceVersion = "2"
	pod.Labels = map[string]string{"app": "echo"}
	rs := &apps.ReplicaSet{ObjectMeta: meta.ObjectMeta{
		Name: "revision", Namespace: pod.Namespace, UID: "rs", ResourceVersion: "3", OwnerReferences: pod.OwnerReferences,
	}}
	pod.OwnerReferences = []meta.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: ptr.To(true)}}
	client := fake.NewSimpleClientset(obj, rs, pod)
	streams := map[string]chan *watch.RaceFreeFakeWatcher{}
	for _, name := range []string{"pods", "replicasets", "deployments"} {
		ch := make(chan *watch.RaceFreeFakeWatcher, 4)
		streams[name] = ch
		client.PrependWatchReactor(name, func(ktesting.Action) (bool, watch.Interface, error) {
			w := watch.NewRaceFreeFake()
			ch <- w
			return true, w, nil
		})
	}
	factory := informers.NewSharedInformerFactory(client, 0)
	v := observeInventory(factory, client, "", k8sapi.Kinds{k8sapi.DeploymentKind, k8sapi.ReplicaSetKind})
	c := NewWatcher().(*configWatcher)
	c.informers.Store("", &informersWithCancel{inventory: v})
	c.Store(config)
	check := func() map[types.UID]string { return c.VerifiedPods(wl, encoded) }
	require.Empty(t, check())
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	factory.Start(ctx.Done())
	for _, synced := range factory.WaitForCacheSync(ctx.Done()) {
		require.True(t, synced)
	}
	nextStream := func(name string) *watch.RaceFreeFakeWatcher {
		select {
		case w := <-streams[name]:
			return w
		case <-ctx.Done():
			t.Fatal("watch did not start for " + name)
			return nil
		}
	}
	pw, rw, dw := nextStream("pods"), nextStream("replicasets"), nextStream("deployments")
	require.Empty(t, check(), "initial cache sync alone cannot authorize a shortcut")
	bookmark := func(w *watch.RaceFreeFakeWatcher, object runtime.Object, version string) {
		event := object.DeepCopyObject()
		event.(meta.Object).SetResourceVersion(version)
		w.Action(watch.Bookmark, event)
	}
	bookmark(pw, pod, "10")
	bookmark(rw, rs, "10")
	bookmark(dw, obj, "10")
	eventually := func(f func() bool) { require.Eventually(t, f, 2*time.Second, 5*time.Millisecond) }
	want := map[types.UID]string{pod.UID: pod.Name}
	eventually(func() bool { return len(check()) == 1 })
	client.ClearActions()
	require.Equal(t, want, check())
	require.Empty(t, client.Actions())
	key := evictionKey(wl)
	state := c.lockEvictionState(key)
	state.acceptedReplacement = newPodReplacement(wl, pod, []*core.Pod{pod})
	state.Unlock()
	require.Empty(t, check(), "an accepted eviction is outstanding even while the old pod is present")
	c.deleteEvictionState(key)
	require.Equal(t, want, check())

	stale := rs.DeepCopy()
	stale.UID = "new-rs"
	stale.ResourceVersion = "11"
	rw.Modify(stale)
	eventually(func() bool { p, ok := v.owners[k8sapi.ReplicaSetKind].checkpoint(); return ok && p.version == "11" })
	require.Empty(t, check(), "a recreated controller with the same name cannot own the old pod")
	correct := rs.DeepCopy()
	correct.ResourceVersion = "12"
	rw.Modify(correct)
	eventually(func() bool { return len(check()) == 1 })

	extra := pod.DeepCopy()
	extra.Name = "uninjected"
	extra.UID = "unregistered"
	extra.ResourceVersion = "13"
	delete(extra.Annotations, annotation.Config)
	pw.Add(extra.DeepCopy())
	eventually(func() bool { p, ok := v.pods.checkpoint(); return ok && p.version == "13" })
	require.Empty(t, check(), "all selector-matching physical pods must be accounted for")
	extra.ResourceVersion = "14"
	pw.Delete(extra.DeepCopy())
	eventually(func() bool { return len(check()) == 1 })

	pw.Stop()
	pw = nextStream("pods")
	require.Empty(t, check(), "reopening a watch does not establish progress")
	bookmark(pw, pod, "15")
	eventually(func() bool { return len(check()) == 1 })
	v.pods.mu.Lock()
	v.pods.position.bookmark = time.Now().Add(-2 * maxBookmarkAge)
	v.pods.mu.Unlock()
	require.Empty(t, check(), "a silent stream has a finite freshness window")
	bookmark(pw, pod, "16")
	eventually(func() bool { return len(check()) == 1 })
	v.pods.GetStore().Bookmark("15")
	require.Empty(t, check(), "received watch progress must reach the store")
}
