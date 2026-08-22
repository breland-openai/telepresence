package cache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSubscribeInitialSnapshotDoesNotWaitForOtherSubscribers(t *testing.T) {
	done := make(chan struct{})
	defer close(done)

	m := NewMap[string, string](func(a, b string) bool { return a == b }, time.Hour)
	m.Store("existing", "first")

	var blockExisting atomic.Bool
	var enteredOnce sync.Once
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseExisting := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseExisting()

	existing := m.Subscribe(done, func(_ string, _ string) bool {
		if blockExisting.Load() {
			enteredOnce.Do(func() { close(entered) })
			<-release
		}
		return true
	})
	require.Equal(t, map[string]string{"existing": "first"}, (<-existing).Upserts)

	// Leave an unrelated subscriber marked without letting the delayed
	// notifier run. Its filter will block if Subscribe flushes global state.
	m.Store("pending", "second")
	blockExisting.Store(true)

	result := make(chan (<-chan Delta[string, string]), 1)
	go func() {
		result <- m.Subscribe(done, nil)
	}()

	var incoming <-chan Delta[string, string]
	select {
	case incoming = <-result:
	case <-entered:
		releaseExisting()
		<-result
		t.Fatal("initial subscription waited for an unrelated subscriber")
	case <-time.After(time.Second):
		t.Fatal("initial subscription did not return promptly")
	}

	require.Equal(t, map[string]string{
		"existing": "first",
		"pending":  "second",
	}, (<-incoming).Upserts)

	// Direct initialization must not consume or discard pending updates for
	// existing subscribers.
	blockExisting.Store(false)
	releaseExisting()
	m.notify()
	require.Equal(t, map[string]string{"pending": "second"}, (<-existing).Upserts)
}

func TestSubscribeInitialSnapshotPreservesConcurrentUpdate(t *testing.T) {
	done := make(chan struct{})
	defer close(done)

	m := NewMap[string, string](func(a, b string) bool { return a == b }, time.Millisecond)
	m.Store("existing", "first")

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	result := make(chan (<-chan Delta[string, string]), 1)
	go func() {
		result <- m.Subscribe(done, func(key, _ string) bool {
			if key == "existing" {
				once.Do(func() { close(started) })
				<-release
			}
			return true
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("initial snapshot did not start")
	}

	m.Store("concurrent", "second")
	close(release)

	var incoming <-chan Delta[string, string]
	select {
	case incoming = <-result:
	case <-time.After(time.Second):
		t.Fatal("initial subscription did not finish")
	}

	first := <-incoming
	require.Equal(t, "first", first.Upserts["existing"])
	if first.Upserts["concurrent"] == "second" {
		return
	}
	select {
	case next := <-incoming:
		require.Equal(t, "second", next.Upserts["concurrent"])
	case <-time.After(time.Second):
		t.Fatal("update during initial subscription was lost")
	}
}

func TestSubscribeInitialSnapshotFiltersValuesAndCloses(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   map[string]string
	}{
		{name: "empty", values: map[string]string{}, want: map[string]string{}},
		{
			name:   "filtered",
			values: map[string]string{"keep": "included", "drop": "excluded"},
			want:   map[string]string{"keep": "included"},
		},
		{
			name:   "all filtered",
			values: map[string]string{"drop": "excluded"},
			want:   map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMap[string, string](func(a, b string) bool { return a == b }, time.Hour)
			for key, value := range tt.values {
				m.Store(key, value)
			}
			done := make(chan struct{})
			incoming := m.Subscribe(done, func(key, _ string) bool { return key != "drop" })
			require.Equal(t, tt.want, (<-incoming).Upserts)
			close(done)
			select {
			case _, ok := <-incoming:
				require.False(t, ok)
			case <-time.After(time.Second):
				t.Fatal("canceled subscription did not close")
			}
		})
	}
}

func TestSubscribePendingDeltaMergeDoesNotMutateDeliveredDeltas(t *testing.T) {
	tests := []struct {
		name       string
		initial    map[string]string
		first      func(*Map[string, string])
		second     func(*Map[string, string])
		wantFirst  Delta[string, string]
		wantMerged Delta[string, string]
	}{
		{
			name:   "upserts add",
			first:  func(m *Map[string, string]) { m.Store("first", "one") },
			second: func(m *Map[string, string]) { m.Store("second", "two") },
			wantFirst: Delta[string, string]{
				Upserts: map[string]string{"first": "one"},
			},
			wantMerged: Delta[string, string]{
				Upserts: map[string]string{"first": "one", "second": "two"},
			},
		},
		{
			name:    "upserts remove",
			initial: map[string]string{"first": "one"},
			first:   func(m *Map[string, string]) { m.Store("first", "updated") },
			second:  func(m *Map[string, string]) { m.Delete("first") },
			wantFirst: Delta[string, string]{
				Upserts: map[string]string{"first": "updated"},
			},
			wantMerged: Delta[string, string]{
				Upserts:  map[string]string{},
				Removals: map[string]string{"first": "updated"},
			},
		},
		{
			name:    "removals add",
			initial: map[string]string{"first": "one", "second": "two"},
			first:   func(m *Map[string, string]) { m.Delete("first") },
			second:  func(m *Map[string, string]) { m.Delete("second") },
			wantFirst: Delta[string, string]{
				Removals: map[string]string{"first": "one"},
			},
			wantMerged: Delta[string, string]{
				Removals: map[string]string{"first": "one", "second": "two"},
			},
		},
		{
			name:    "removals remove",
			initial: map[string]string{"first": "one"},
			first:   func(m *Map[string, string]) { m.Delete("first") },
			second:  func(m *Map[string, string]) { m.Store("first", "updated") },
			wantFirst: Delta[string, string]{
				Removals: map[string]string{"first": "one"},
			},
			wantMerged: Delta[string, string]{
				Upserts:  map[string]string{"first": "updated"},
				Removals: map[string]string{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := make(chan struct{})
			defer close(done)

			m := NewMap[string, string](func(a, b string) bool { return a == b }, time.Hour)
			for key, value := range tt.initial {
				m.Store(key, value)
			}
			fast := m.Subscribe(done, nil)
			slow := m.Subscribe(done, nil)
			<-fast
			<-slow

			tt.first(m)
			m.notify()
			delivered := <-fast
			require.Equal(t, tt.wantFirst, delivered)

			tt.second(m)
			m.notify()

			require.Equal(t, tt.wantFirst, delivered, "coalescing a pending delta changed another subscriber's delivered delta")
			require.Equal(t, tt.wantMerged, <-slow)
		})
	}
}

func TestAllDeltaSendAfterSubscriptionClose(t *testing.T) {
	sb := &subscription[string, string]{
		channel: make(chan Delta[string, string], 1),
		doneCh:  make(chan struct{}),
	}
	sb.close()

	ad := allDelta[string, string]{
		snapshot: map[string]string{"key": "value"},
	}
	require.NotPanics(t, func() {
		ad.send(sb)
	})

	_, ok := <-sb.channel
	require.False(t, ok)
}
