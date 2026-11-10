package mutator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	features "k8s.io/client-go/features"
	featurestest "k8s.io/client-go/features/testing"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	clocktest "k8s.io/utils/clock/testing"
)

type streamingPodClient struct{ *fake.Clientset }

func (streamingPodClient) IsWatchListSemanticsUnSupported() bool { return false }

func TestStreamingInventoryAndSilentWatch(t *testing.T) {
	featurestest.SetFeatureDuringTest(t, features.WatchListClient, true)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "echo", Namespace: "default", UID: "old", ResourceVersion: "9"}}
	client := streamingPodClient{fake.NewSimpleClientset(pod)}
	type opening struct {
		options meta.ListOptions
		stream  *watch.RaceFreeFakeWatcher
	}
	openings := make(chan opening, 2)
	client.PrependWatchReactor("pods", func(a ktesting.Action) (bool, watch.Interface, error) {
		w := watch.NewRaceFreeFake()
		openings <- opening{a.(ktesting.WatchActionImpl).GetListOptions(), w}
		return true, w, nil
	})
	nextWatch := func() *watch.RaceFreeFakeWatcher {
		t.Helper()
		select {
		case start := <-openings:
			require.NotNil(t, start.options.SendInitialEvents)
			require.True(t, *start.options.SendInitialEvents)
			require.Equal(t, meta.ResourceVersionMatchNotOlderThan, start.options.ResourceVersionMatch)
			return start.stream
		case <-ctx.Done():
			t.Fatal("initial events watch did not open")
			return nil
		}
	}
	resource := client.CoreV1().Pods(pod.Namespace)
	factory := informers.NewSharedInformerFactory(client, 0)
	i := observeResource(factory, &core.Pod{}, resource)
	clock := clocktest.NewFakeClock(time.Now())
	i.now = clock.Now
	factory.Start(ctx.Done())
	t.Cleanup(func() { cancel(); factory.Shutdown() })
	wait := func(f func() bool) { t.Helper(); require.Eventually(t, f, 3*time.Second, time.Millisecond) }
	sendBookmark := func(w *watch.RaceFreeFakeWatcher, rv string, end bool) {
		b := &core.Pod{ObjectMeta: meta.ObjectMeta{ResourceVersion: rv}}
		if end {
			b.Annotations = map[string]string{meta.InitialEventsAnnotationKey: "true"}
		}
		w.Action(watch.Bookmark, b)
		wait(func() bool { i.mu.Lock(); defer i.mu.Unlock(); return i.position.version == rv })
	}
	w := nextWatch()
	w.Add(pod.DeepCopy())
	sendBookmark(w, "10", false)
	_, ok := i.checkpoint()
	require.False(t, ok)
	sendBookmark(w, "11", true)
	wait(func() bool { _, synced := i.checkpoint(); return synced })
	for _, a := range client.Actions() {
		require.False(t, a.Matches("list", "pods"), "bootstrap must exercise streaming initial events")
	}
	replacement := pod.DeepCopy()
	replacement.UID = "new"
	replacement.ResourceVersion = "12"
	require.NoError(t, resource.Delete(ctx, pod.Name, meta.DeleteOptions{}))
	_, err := resource.Create(ctx, replacement.DeepCopy(), meta.CreateOptions{})
	require.NoError(t, err)

	// The API has a new pod while the watch stays open and delivers nothing.
	clock.Step(maxBookmarkAge - time.Nanosecond)
	_, ok = i.checkpoint()
	require.True(t, ok)
	cached, exists, err := i.GetStore().Get(pod)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, pod.UID, cached.(*core.Pod).UID)
	clock.Step(2 * time.Nanosecond)
	_, ok = i.checkpoint()
	require.False(t, ok)
	w.Modify(replacement.DeepCopy())
	wait(func() bool { return i.GetStore().LastStoreSyncResourceVersion() == "12" })
	_, ok = i.checkpoint()
	require.False(t, ok, "ordinary events do not renew the bookmark deadline")
	sendBookmark(w, "13", false)
	wait(func() bool { _, synced := i.checkpoint(); return synced })

	w.Error(&meta.Status{Status: meta.StatusFailure, Code: 410, Reason: meta.StatusReasonExpired})
	w = nextWatch()
	// HasSynced remains true across a relist. An ordinary bookmark can even
	// equal the previous store's revision while a new snapshot is incomplete.
	require.True(t, i.HasSynced())
	sendBookmark(w, "13", false)
	_, ok = i.checkpoint()
	require.False(t, ok)
	w.Add(replacement.DeepCopy())
	sendBookmark(w, "14", true)
	wait(func() bool { p, synced := i.checkpoint(); return synced && p.version == "14" })
}
