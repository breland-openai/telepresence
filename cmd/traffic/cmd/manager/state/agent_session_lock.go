package state

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/types"

	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

type agentLifecycleLocks[K comparable] struct {
	mu      sync.Mutex
	entries map[K]*agentLifecycleLock
}

type agentLifecycleLock struct {
	available chan struct{}
	users     int
}

func (s *State) lockAgentSession(ctx context.Context, uid types.UID, id tunnel.SessionID) (func(), error) {
	unlockPod, err := s.agentPodLocks.lockContext(ctx, uid)
	if err != nil {
		return nil, err
	}
	unlockSession, err := s.agentSessionLocks.lockContext(ctx, id)
	if err != nil {
		unlockPod()
		return nil, err
	}
	return func() {
		unlockSession()
		unlockPod()
	}, nil
}

func (locks *agentLifecycleLocks[K]) lock(key K) func() {
	unlock, _ := locks.lockContext(context.Background(), key)
	return unlock
}

func (locks *agentLifecycleLocks[K]) lockContext(ctx context.Context, key K) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	locks.mu.Lock()
	if locks.entries == nil {
		locks.entries = make(map[K]*agentLifecycleLock)
	}
	entry := locks.entries[key]
	if entry == nil {
		entry = &agentLifecycleLock{available: make(chan struct{}, 1)}
		entry.available <- struct{}{}
		locks.entries[key] = entry
	}
	entry.users++
	locks.mu.Unlock()

	unlock := func() {
		entry.available <- struct{}{}
		locks.release(key, entry)
	}
	select {
	case <-ctx.Done():
		locks.release(key, entry)
		return nil, ctx.Err()
	case <-entry.available:
		if err := ctx.Err(); err != nil {
			unlock()
			return nil, err
		}
		return unlock, nil
	}
}

func (locks *agentLifecycleLocks[K]) release(key K, entry *agentLifecycleLock) {
	locks.mu.Lock()
	defer locks.mu.Unlock()
	entry.users--
	if entry.users == 0 {
		delete(locks.entries, key)
	}
}
