package cache

import (
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifyPreservesMutationDuringDelivery(t *testing.T) {
	tests := []struct {
		name    string
		initial string
		mutate  func(*Map[string, string])
		want    string
	}{
		{name: "create", mutate: func(m *Map[string, string]) { m.Store("intercept", "WAITING") }, want: "WAITING"},
		{name: "update", initial: "WAITING", mutate: func(m *Map[string, string]) { m.Store("intercept", "ACTIVE") }, want: "ACTIVE"},
		{name: "delete", initial: "ACTIVE", mutate: func(m *Map[string, string]) { m.Delete("intercept") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMap[string, string](func(a, b string) bool { return a == b }, time.Hour)
			done := make(chan struct{})
			t.Cleanup(func() { close(done); m.notifier.Stop() })
			if tt.initial != "" {
				m.Store("intercept", tt.initial)
			}
			var armed atomic.Bool
			entered, unblock := notifyBarrier(t)
			incoming := m.Subscribe(done, func(key, _ string) bool {
				if key == "sentinel" && armed.CompareAndSwap(true, false) {
					entered()
				}
				return true
			})
			state := map[string]string{}
			applyNotifyDelta(state, <-incoming)
			require.Equal(t, tt.initial, state["intercept"])

			m.Store("sentinel", "first")
			armed.Store(true)
			finished := make(chan struct{})
			go func() { m.notify(); close(finished) }()
			unblock.wait()
			tt.mutate(m)
			unblock.release()
			waitNotify(t, finished)
			drainNotifyDeltas(state, incoming)
			require.Equal(t, "first", state["sentinel"])

			// Store schedules another notification after the first snapshot.
			m.notify()
			drainNotifyDeltas(state, incoming)
			value, present := m.Load("intercept")
			require.Equal(t, tt.want, value)
			require.Equal(t, tt.want != "", present)
			assert.Equal(t, tt.want, state["intercept"])
			_, present = state["intercept"]
			assert.Equal(t, tt.want != "", present)

			m.Store("control", "third")
			m.notify()
			drainNotifyDeltas(state, incoming)
			require.Equal(t, "third", state["control"])
			assert.Equal(t, tt.want, state["intercept"])
		})
	}
}

func TestNotifyDoesNotBlockOrRegressNewSubscription(t *testing.T) {
	m := NewMap[string, string](func(a, b string) bool { return a == b }, time.Hour)
	done := make(chan struct{})
	t.Cleanup(func() { close(done); m.notifier.Stop() })
	m.Store("intercept", "initial")
	var armed atomic.Bool
	entered, unblock := notifyBarrier(t)
	slow := m.Subscribe(done, func(key, _ string) bool {
		if key == "sentinel" && armed.CompareAndSwap(true, false) {
			entered()
		}
		return true
	})
	require.Equal(t, "initial", (<-slow).Upserts["intercept"])
	m.Store("intercept", "WAITING")
	m.Store("sentinel", "first")
	armed.Store(true)
	finished := make(chan struct{})
	go func() { m.notify(); close(finished) }()
	unblock.wait()
	m.Store("intercept", "ACTIVE")

	result := make(chan (<-chan Delta[string, string]), 1)
	go func() { result <- m.Subscribe(done, nil) }()
	var incoming <-chan Delta[string, string]
	select {
	case incoming = <-result:
	case <-time.After(time.Second):
		t.Fatal("new subscription waited for an unrelated blocked notifier")
	}
	state := map[string]string{}
	applyNotifyDelta(state, <-incoming)
	require.Equal(t, "ACTIVE", state["intercept"])

	unblock.release()
	waitNotify(t, finished)
	drainNotifyDeltas(state, incoming)
	require.Equal(t, "ACTIVE", state["intercept"], "an older notifier overwrote the initial snapshot")
	m.notify()
	drainNotifyDeltas(state, incoming)
	require.Equal(t, "ACTIVE", state["intercept"])
}

func TestNotifyDoesNotSendUnrelatedChanges(t *testing.T) {
	m := NewMap[string, string](func(a, b string) bool { return a == b }, time.Hour)
	done := make(chan struct{})
	t.Cleanup(func() { close(done); m.notifier.Stop() })
	incoming := m.Subscribe(done, func(key, _ string) bool { return key == "keep" })
	require.Empty(t, (<-incoming).Upserts)
	m.Store("drop", "unrelated")
	m.notify()
	select {
	case delta := <-incoming:
		t.Fatalf("unexpected notification for unrelated change: %+v", delta)
	default:
	}
	m.Store("keep", "included")
	m.notify()
	require.Equal(t, map[string]string{"keep": "included"}, (<-incoming).Upserts)
}

func TestNotifyReconcilesSubscriptionBetweenCoalescedMutations(t *testing.T) {
	tests := []struct {
		name    string
		initial string
		middle  string
		final   string
	}{
		{name: "appears_then_disappears", middle: "WAITING"},
		{name: "disappears_then_appears", initial: "ACTIVE", final: "ACTIVE"},
		{name: "changes_then_returns", initial: "ACTIVE", middle: "WAITING", final: "ACTIVE"},
		{name: "changes_again", initial: "OLD", middle: "WAITING", final: "ACTIVE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMap[string, string](func(a, b string) bool { return a == b }, time.Hour)
			done := make(chan struct{})
			t.Cleanup(func() { close(done); m.notifier.Stop() })
			set := func(value string) {
				if value == "" {
					m.Delete("intercept")
				} else {
					m.Store("intercept", value)
				}
			}
			set(tt.initial)
			early := m.Subscribe(done, nil)
			earlyState := map[string]string{}
			applyNotifyDelta(earlyState, <-early)
			set(tt.middle)
			late := m.Subscribe(done, func(key, _ string) bool { return key == "intercept" })
			lateState := map[string]string{}
			applyNotifyDelta(lateState, <-late)
			require.Equal(t, tt.middle, lateState["intercept"])
			set(tt.final)
			m.notify()
			drainNotifyDeltas(earlyState, early)
			drainNotifyDeltas(lateState, late)
			require.Equal(t, tt.final, earlyState["intercept"])
			require.Equal(t, tt.final, lateState["intercept"])
			m.Store("unrelated", "control")
			m.notify()
			drainNotifyDeltas(lateState, late)
			require.NotContains(t, lateState, "unrelated")
			if tt.final == "" {
				require.NotContains(t, lateState, "intercept")
			}
		})
	}
}

func TestNotifyTimerReschedulesMutationDuringDelivery(t *testing.T) {
	m := NewMap[string, string](func(a, b string) bool { return a == b }, 5*time.Millisecond)
	done := make(chan struct{})
	t.Cleanup(func() { close(done); m.notifier.Stop() })
	var armed atomic.Bool
	entered, unblock := notifyBarrier(t)
	incoming := m.Subscribe(done, func(key, _ string) bool {
		if key == "sentinel" && armed.CompareAndSwap(true, false) {
			entered()
		}
		return true
	})
	state := map[string]string{}
	applyNotifyDelta(state, <-incoming)
	armed.Store(true)
	m.Store("sentinel", "first")
	unblock.wait()
	m.Store("intercept", "ACTIVE")
	unblock.release()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for state["intercept"] != "ACTIVE" {
		select {
		case delta := <-incoming:
			applyNotifyDelta(state, delta)
		case <-timeout.C:
			t.Fatal("rescheduled timer failed to deliver the concurrent mutation")
		}
	}
}

func TestNotifyReconcilesDistinctSubscriptionsAndCancellation(t *testing.T) {
	m := NewMap[string, string](func(a, b string) bool { return a == b }, time.Hour)
	done := make(chan struct{})
	t.Cleanup(func() { close(done); m.notifier.Stop() })
	early := m.Subscribe(done, nil)
	<-early
	m.Store("intercept", "WAITING")
	middle := m.Subscribe(done, nil)
	middleState := map[string]string{}
	applyNotifyDelta(middleState, <-middle)
	m.Store("intercept", "ACTIVE")
	late := m.Subscribe(done, nil)
	lateState := map[string]string{}
	applyNotifyDelta(lateState, <-late)
	closed := make(chan struct{})
	canceled := m.Subscribe(closed, nil)
	<-canceled
	require.EqualValues(t, 3, m.catchUps.Load())
	close(closed)
	waitNotify(t, closedNotify(canceled))
	require.EqualValues(t, 2, m.catchUps.Load())
	m.Delete("intercept")
	m.notify()
	drainNotifyDeltas(middleState, middle)
	drainNotifyDeltas(lateState, late)
	require.Empty(t, middleState)
	require.Empty(t, lateState)
	require.Zero(t, m.catchUps.Load())
}

func closedNotify(ch <-chan Delta[string, string]) <-chan struct{} {
	closed := make(chan struct{})
	go func() {
		for range ch {
		}
		close(closed)
	}()
	return closed
}

func TestNotifyTracksValueFilterMembership(t *testing.T) {
	tests := []struct {
		name    string
		initial string
		middle  string
		final   string
		late    bool
	}{
		{name: "existing_becomes_invisible", initial: "WAITING", final: "ACTIVE"},
		{name: "existing_becomes_visible", initial: "ACTIVE", final: "WAITING"},
		{name: "late_becomes_invisible", initial: "ACTIVE", middle: "WAITING", final: "ACTIVE", late: true},
		{name: "late_becomes_visible", initial: "WAITING", middle: "ACTIVE", final: "WAITING", late: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMap[string, string](func(a, b string) bool { return a == b }, time.Hour)
			done := make(chan struct{})
			t.Cleanup(func() { close(done); m.notifier.Stop() })
			m.Store("intercept", tt.initial)
			if tt.late {
				early := m.Subscribe(done, nil)
				<-early
				m.Store("intercept", tt.middle)
			}
			incoming := m.Subscribe(done, func(_, value string) bool { return value == "WAITING" })
			state := map[string]string{}
			applyNotifyDelta(state, <-incoming)
			m.Store("intercept", tt.final)
			m.notify()
			drainNotifyDeltas(state, incoming)
			if tt.final == "WAITING" {
				require.Equal(t, map[string]string{"intercept": "WAITING"}, state)
			} else {
				require.Empty(t, state)
			}
		})
	}
}

func TestNotifyNewSubscriptionsDoNotPostponeExistingWatcher(t *testing.T) {
	m := NewMap[string, string](func(a, b string) bool { return a == b }, 40*time.Millisecond)
	done := make(chan struct{})
	t.Cleanup(func() { close(done); m.notifier.Stop() })
	incoming := m.Subscribe(done, nil)
	<-incoming
	m.Store("intercept", "ACTIVE")
	join := time.NewTicker(2 * time.Millisecond)
	defer join.Stop()
	timeout := time.NewTimer(250 * time.Millisecond)
	defer timeout.Stop()
	for {
		select {
		case delta := <-incoming:
			require.Equal(t, "ACTIVE", delta.Upserts["intercept"])
			return
		case <-join.C:
			late := m.Subscribe(done, nil)
			<-late
		case <-timeout.C:
			t.Fatal("read-only subscriptions postponed an already scheduled update")
		}
	}
}

type notifyBlock struct {
	t       *testing.T
	entered chan struct{}
	blocked chan struct{}
	once    sync.Once
}

func notifyBarrier(t *testing.T) (func(), *notifyBlock) {
	t.Helper()
	b := &notifyBlock{t: t, entered: make(chan struct{}), blocked: make(chan struct{})}
	t.Cleanup(b.release)
	return func() { close(b.entered); <-b.blocked }, b
}

func (b *notifyBlock) wait() {
	b.t.Helper()
	waitNotify(b.t, b.entered)
}

func (b *notifyBlock) release() {
	b.once.Do(func() { close(b.blocked) })
}

func waitNotify(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("notification did not reach the expected synchronization point")
	}
}

func applyNotifyDelta(state map[string]string, delta Delta[string, string]) {
	maps.Copy(state, delta.Upserts)
	for key := range delta.Removals {
		delete(state, key)
	}
}

func drainNotifyDeltas(state map[string]string, incoming <-chan Delta[string, string]) {
	for {
		select {
		case delta := <-incoming:
			applyNotifyDelta(state, delta)
		default:
			return
		}
	}
}
