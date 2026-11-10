package trafficmgr

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
)

func TestInterceptCleanupDoesNotWaitIndefinitelyForMount(t *testing.T) {
	s := &session{service: rootDaemonReconnectTestService{}}
	mountCanceled := make(chan struct{})
	ic := &intercept{
		InterceptInfo: &manager.InterceptInfo{Spec: &manager.InterceptSpec{Name: "test"}},
		cancel:        func() { close(mountCanceled) },
	}
	ic.wg.Add(1)
	defer ic.wg.Done()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- s.removeInterceptWithContext(ctx, ic) }()
	select {
	case <-mountCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("intercept did not request mount shutdown")
	}
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		t.Fatal("intercept cleanup did not respect its deadline")
	}
}
