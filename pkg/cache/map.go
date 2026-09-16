package cache

import (
	"maps"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
)

type Delta[K comparable, V any] struct {
	Upserts  map[K]V
	Removals map[K]V
}

func (delta *Delta[K, V]) Merge(other Delta[K, V]) {
	if len(delta.Upserts) == 0 {
		delta.Upserts = other.Upserts
	} else {
		delta.Upserts = maps.Clone(delta.Upserts)
		for k, v := range other.Upserts {
			delta.Upserts[k] = v
		}
		for k := range other.Removals {
			delete(delta.Upserts, k)
		}
	}
	if len(delta.Removals) == 0 {
		delta.Removals = other.Removals
	} else {
		delta.Removals = maps.Clone(delta.Removals)
		for k, v := range other.Removals {
			delta.Removals[k] = v
		}
		for k := range other.Upserts {
			delete(delta.Removals, k)
		}
	}
}

type subscription[K comparable, V any] struct {
	sync.Mutex
	channel     chan Delta[K, V]
	include     func(K, V) bool
	owner       *Map[K, V]
	catchUp     map[K]baselineValue[V]
	initialized bool
	doneCh      <-chan struct{}
	closed      bool
}

type baselineValue[V any] struct {
	value   V
	present bool
}

func (sb *subscription[K, V]) clearCatchUp() map[K]baselineValue[V] {
	catchUp := sb.catchUp
	if len(catchUp) > 0 {
		sb.catchUp = nil
		sb.owner.catchUps.Add(-1)
	}
	return catchUp
}

func (sb *subscription[K, V]) close() {
	sb.Lock()
	if !sb.closed {
		sb.clearCatchUp()
		close(sb.channel)
		sb.closed = true
	}
	sb.Unlock()
}

type Map[K comparable, V any] struct {
	*xsync.Map[K, V]
	notifyLock  sync.Mutex
	snapLock    sync.Mutex
	snapshot    map[K]V
	equal       func(V, V) bool
	subscribers *xsync.Map[uuid.UUID, *subscription[K, V]]
	catchUps    atomic.Int64
	notifyDelay time.Duration
	notifier    *time.Timer
}

// NewMap creates a new Map instance configured with the given options.
func NewMap[K comparable, V any](equal func(V, V) bool, notifyDelay time.Duration, config ...func(*xsync.MapConfig)) *Map[K, V] {
	m := &Map[K, V]{
		Map:         xsync.NewMap[K, V](config...),
		equal:       equal,
		subscribers: xsync.NewMap[uuid.UUID, *subscription[K, V]](),
		notifyDelay: notifyDelay,
	}
	m.notifier = time.AfterFunc(math.MaxInt64, m.notify)
	return m
}

// Subscribe returns a channel that will emit deltas that corresponds to modifications of the contained
// values filtered by the given filter.
//
// The first delta is a snapshot of all values, and it is emitted immediately after the call to Subscribe().
// After that, a new Delta is emitted when an included value changes or when a
// value enters or leaves the filter.
//
// The values contained in a delta will reflect actual values in the map and must be considered immutable.
// Mutating them will mutate the map without the map's knowledge and hence not trigger notifications to
// subscribers.
//
// The returned channel will be closed when the given channel is closed.
func (m *Map[K, V]) Subscribe(done <-chan struct{}, includeFilter func(K, V) bool) <-chan Delta[K, V] {
	ch := make(chan Delta[K, V], 1)
	select {
	case <-done:
		close(ch)
	default:
		id := uuid.New()
		sb := &subscription[K, V]{include: includeFilter, channel: ch, doneCh: done, owner: m}
		// Registration and the initial snapshot must precede every notifier
		// snapshot that can deliver to this subscription.
		sb.Lock()
		m.snapLock.Lock()
		snapshot := m.LoadAll()
		if m.snapshot == nil {
			m.snapshot = snapshot
		} else {
			// A new watcher starts from the current map, which may be ahead of
			// the shared snapshot. Reconcile only those differing keys on its
			// first notification, even if later writes cancel the shared delta.
			sb.catchUp = m.subscriptionBaseline(snapshot)
		}
		needsCatchUp := len(sb.catchUp) > 0
		if needsCatchUp {
			m.catchUps.Add(1)
		}
		m.subscribers.Store(id, sb)
		m.snapLock.Unlock()
		initial := allDelta[K, V]{snapshot: snapshot}
		initial.sendLocked(sb)
		sb.Unlock()
		go func() {
			<-done
			m.subscribers.Delete(id)
			sb.close()
		}()
	}
	return ch
}

// subscriptionBaseline requires snapLock and records the new watcher's initial
// values only for keys that differ from the shared notifier snapshot.
func (m *Map[K, V]) subscriptionBaseline(initial map[K]V) map[K]baselineValue[V] {
	var baseline map[K]baselineValue[V]
	for k, v := range initial {
		if prev, ok := m.snapshot[k]; !ok || !m.equal(prev, v) {
			if baseline == nil {
				baseline = make(map[K]baselineValue[V])
			}
			baseline[k] = baselineValue[V]{value: v, present: true}
		}
	}
	for k := range m.snapshot {
		if _, ok := initial[k]; !ok {
			if baseline == nil {
				baseline = make(map[K]baselineValue[V])
			}
			baseline[k] = baselineValue[V]{}
		}
	}
	return baseline
}

// Compute either sets the computed new value for the key or deletes the value for the key.
// When the delete result of the valueFn function is set to true, the value will be deleted if it exists.
// When delete is set to false, the value is updated to the newValue. The ok result indicates whether the
// value was computed and stored, thus, is present in the map. The actual result contains the new value in
// cases where the value was computed and stored. See the example for a few use cases.
//
// This call locks a hash table bucket while the compute function is executed. It means that modifications
// on other entries in the bucket will be blocked until the valueFn executes. Consider this when the function
// includes long-running operations.
func (m *Map[K, V]) Compute(key K, f func(V, bool) (V, xsync.ComputeOp)) (V, bool) {
	modified := false
	actual, ok := m.Map.Compute(key, func(v V, loaded bool) (V, xsync.ComputeOp) {
		fv, op := f(v, loaded)
		switch op {
		case xsync.CancelOp:
		case xsync.UpdateOp:
			if loaded && m.equal(fv, v) {
				fv = v
				op = xsync.CancelOp
			} else {
				modified = true
			}
		case xsync.DeleteOp:
			modified = true
		}
		return fv, op
	})
	if modified {
		m.notifier.Reset(m.notifyDelay)
	}
	return actual, ok
}

// CompareAndSwap checks if the current value for the given key equals the oldValue, and if
// so, swaps the current value for the newValue.
// The swapped result reports whether the value was swapped.
func (m *Map[K, V]) CompareAndSwap(key K, oldValue, newValue V) (swapped bool) {
	m.Compute(key, func(cur V, loaded bool) (V, xsync.ComputeOp) {
		if loaded && m.equal(cur, oldValue) {
			swapped = true
			return newValue, xsync.UpdateOp
		}
		return oldValue, xsync.CancelOp
	})
	return swapped
}

// Delete deletes the value for a key.
func (m *Map[K, V]) Delete(key K) {
	m.Compute(key, func(oldValue V, loaded bool) (V, xsync.ComputeOp) {
		if !loaded {
			return oldValue, xsync.CancelOp
		}
		return oldValue, xsync.DeleteOp
	})
}

// LoadAll return a map of all entries.
func (m *Map[K, V]) LoadAll() map[K]V {
	mr := make(map[K]V, m.Size())
	m.Range(func(key K, value V) bool {
		mr[key] = value
		return true
	})
	return mr
}

// LoadMatching return a map of all entries matching the given filter.
func (m *Map[K, V]) LoadMatching(filter func(K, V) bool) map[K]V {
	mr := make(map[K]V)
	m.Range(func(key K, value V) bool {
		if filter(key, value) {
			mr[key] = value
		}
		return true
	})
	return mr
}

// LoadAndDelete deletes the value for a key, returning the previous value if any.
// The loaded result reports whether the key was present.
func (m *Map[K, V]) LoadAndDelete(key K) (previous V, loaded bool) {
	m.Compute(key, func(oldValue V, wasLoaded bool) (V, xsync.ComputeOp) {
		if wasLoaded {
			previous = oldValue
			loaded = true
			return oldValue, xsync.DeleteOp
		}
		return oldValue, xsync.CancelOp
	})
	return previous, loaded
}

// LoadAndStore stores a new value for the key and returns the existing one, if present. The loaded result is true if the
// existing value was loaded, false otherwise.
func (m *Map[K, V]) LoadAndStore(key K, value V) (existing V, loaded bool) {
	m.Compute(key, func(oldValue V, wasLoaded bool) (V, xsync.ComputeOp) {
		if wasLoaded {
			existing = oldValue
			loaded = true
		}
		return value, xsync.UpdateOp
	})
	return existing, loaded
}

// LoadOrCompute returns the existing value for the key if present. Otherwise, it computes the value using
// the provided function and returns the computed value. The loaded result is true if the value was loaded,
// false if stored.
//
// This call locks a hash table bucket while the compute function is executed. It means that modifications
// on other entries in the bucket will be blocked until the valueFn executes. Consider this when the function
// includes long-running operations.
func (m *Map[K, V]) LoadOrCompute(key K, valueFn func() V) (actual V, loaded bool) {
	actual, _ = m.Compute(key, func(oldValue V, wasLoaded bool) (V, xsync.ComputeOp) {
		if wasLoaded {
			loaded = true
			return oldValue, xsync.CancelOp
		}
		return valueFn(), xsync.UpdateOp
	})
	return actual, loaded
}

// LoadOrStore returns the existing value for the key if present. Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	actual, _ = m.Compute(key, func(oldValue V, wasLoaded bool) (V, xsync.ComputeOp) {
		if wasLoaded {
			loaded = true
			return oldValue, xsync.CancelOp
		}
		return value, xsync.UpdateOp
	})
	return actual, loaded
}

// Store stores a new value for the key.
func (m *Map[K, V]) Store(key K, value V) {
	m.Compute(key, func(oldValue V, wasLoaded bool) (V, xsync.ComputeOp) { return value, xsync.UpdateOp })
}

// notify delivers each delta in snapshot order to its captured subscriptions.
func (m *Map[K, V]) notify() {
	m.notifyLock.Lock()
	defer m.notifyLock.Unlock()

	m.snapLock.Lock()
	delta := m.makeDeltaLocked()
	var recipients []*subscription[K, V]
	if len(delta.upserts) > 0 || len(delta.removals) > 0 || m.catchUps.Load() > 0 {
		recipients = make([]*subscription[K, V], 0, m.subscribers.Size())
		m.subscribers.Range(func(_ uuid.UUID, sb *subscription[K, V]) bool {
			recipients = append(recipients, sb)
			return true
		})
	}
	m.snapLock.Unlock()

	for _, sb := range recipients {
		delta.send(sb)
	}
}

type allDelta[K comparable, V any] struct {
	snapshot map[K]V
	previous map[K]V
	upserts  map[K]V
	removals map[K]V
}

func filteredMap[K comparable, V any](m map[K]V, include func(K, V) bool, exclude map[K]baselineValue[V]) map[K]V {
	if include == nil && len(exclude) == 0 {
		return m
	}
	var fm map[K]V
	for k, v := range m {
		if _, skip := exclude[k]; !skip && (include == nil || include(k, v)) {
			if fm == nil {
				fm = make(map[K]V)
			}
			fm[k] = v
		}
	}
	return fm
}

func (ad *allDelta[K, V]) filteredDelta(initialized bool, include func(K, V) bool, catchUp map[K]baselineValue[V], equal func(V, V) bool) Delta[K, V] {
	var filtered Delta[K, V]
	if initialized {
		filtered.Removals = filteredMap(ad.removals, include, catchUp)
		if include == nil {
			filtered.Upserts = filteredMap(ad.upserts, nil, catchUp)
		} else {
			for k, current := range ad.upserts {
				if _, skip := catchUp[k]; skip {
					continue
				}
				if include(k, current) {
					putDeltaValue(&filtered.Upserts, k, current)
				} else if previous, present := ad.previous[k]; present && include(k, previous) {
					putDeltaValue(&filtered.Removals, k, previous)
				}
			}
		}
	} else {
		filtered.Upserts = filteredMap(ad.snapshot, include, nil)
	}
	for k, initial := range catchUp {
		current, present := ad.snapshot[k]
		switch {
		case present && (!initial.present || !equal(initial.value, current)):
			if include == nil || include(k, current) {
				putDeltaValue(&filtered.Upserts, k, current)
			} else if initial.present && include(k, initial.value) {
				putDeltaValue(&filtered.Removals, k, initial.value)
			}
		case !present && initial.present && (include == nil || include(k, initial.value)):
			putDeltaValue(&filtered.Removals, k, initial.value)
		}
	}
	if include != nil && (!initialized || len(filtered.Upserts) > 0 || len(filtered.Removals) > 0) {
		if filtered.Upserts == nil {
			filtered.Upserts = make(map[K]V)
		}
		if filtered.Removals == nil {
			filtered.Removals = make(map[K]V)
		}
	}
	return filtered
}

func putDeltaValue[K comparable, V any](values *map[K]V, k K, v V) {
	if *values == nil {
		*values = make(map[K]V)
	}
	(*values)[k] = v
}

func (ad *allDelta[K, V]) send(sb *subscription[K, V]) {
	sb.Lock()
	defer sb.Unlock()
	ad.sendLocked(sb)
}

// sendLocked sends a delta while the caller holds the subscription mutex.
func (ad *allDelta[K, V]) sendLocked(sb *subscription[K, V]) {
	if sb.closed {
		return
	}
	initialized := sb.initialized
	sb.initialized = true
	var catchUp map[K]baselineValue[V]
	var equal func(V, V) bool
	if initialized {
		catchUp = sb.clearCatchUp()
		if len(catchUp) > 0 {
			equal = sb.owner.equal
		}
	}
	fd := ad.filteredDelta(initialized, sb.include, catchUp, equal)
	if initialized && len(fd.Upserts) == 0 && len(fd.Removals) == 0 {
		return
	}
	select {
	case <-sb.doneCh:
	case prevDelta := <-sb.channel:
		// The previous delta was not read by the subscriber yet, so we need to merge it with the new delta
		// and put it back on the channel.
		prevDelta.Merge(fd)
		sb.channel <- prevDelta
	default:
		// The channel is empty, so we can just send the delta.
		sb.channel <- fd
	}
}

// makeDeltaLocked requires snapLock.
func (m *Map[K, V]) makeDeltaLocked() allDelta[K, V] {
	previous := m.snapshot
	current := m.LoadAll()
	m.snapshot = current
	var upserts map[K]V
	for k, v := range current {
		if prev, ok := previous[k]; !(ok && m.equal(prev, v)) {
			if upserts == nil {
				upserts = make(map[K]V)
			}
			upserts[k] = v
		}
	}
	var removals map[K]V
	for k, v := range previous {
		if _, ok := current[k]; !ok {
			if removals == nil {
				removals = make(map[K]V)
			}
			removals[k] = v
		}
	}
	return allDelta[K, V]{
		snapshot: current,
		previous: previous,
		upserts:  upserts,
		removals: removals,
	}
}
