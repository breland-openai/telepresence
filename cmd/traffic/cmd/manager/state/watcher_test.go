package state

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestWatcherDispatchSkipsCanceledSubscription(t *testing.T) {
	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan []Event, 1)
	ch <- nil

	w := &watcher{
		subscriptions: map[uuid.UUID]subscription{
			uuid.New(): {ch: ch, done: subCtx.Done()},
		},
		events: []Event{{}},
	}

	dispatched := make(chan struct{})
	go func() {
		w.dispatch(context.Background())
		close(dispatched)
	}()

	require.Eventually(t, func() bool {
		w.Lock()
		defer w.Unlock()
		return w.events == nil
	}, time.Second, time.Millisecond)

	cancel()
	select {
	case <-dispatched:
	case <-time.After(time.Second):
		t.Fatal("dispatch blocked on canceled subscription")
	}
}
