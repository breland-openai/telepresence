package rootd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/log"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

func TestSessionRunJoinsWorkersAfterStartFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancelSession := context.WithCancelCause(client.WithConfig(t.Context(), client.GetDefaultConfig()))
		defer cancelSession(context.Canceled)
		s := newTestStreamSession(ctx)
		s.handlers = tunnel.NewPool()
		startErr := errors.New("session failed after starting a worker")
		initErrs := make(chan error, 1)
		cleanupStarted := make(chan struct{})
		cleanupAllowed := make(chan struct{})
		finishCleanup := sync.OnceFunc(func() { close(cleanupAllowed) })
		defer finishCleanup()
		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
			s.runWithStart(initErrs, cancelSession, func(g log.Group) error {
				g.Go("worker", func(context.Context) error {
					// Session workers may use the session context directly.
					<-s.Done()
					close(cleanupStarted)
					<-cleanupAllowed
					return nil
				})
				return startErr
			})
		}()
		synctest.Wait()
		require.ErrorIs(t, <-initErrs, startErr)
		require.ErrorIs(t, s.Err(), context.Canceled)
		require.ErrorIs(t, context.Cause(s), startErr)
		select {
		case <-cleanupStarted:
		default:
			t.Error("worker did not begin cleanup after startup failed")
		}
		select {
		case <-runDone:
			t.Error("session finished before its worker finished cleanup")
		default:
		}

		finishCleanup()
		synctest.Wait()
		select {
		case <-runDone:
		default:
			t.Error("session did not finish after worker cleanup completed")
		}
	})
}

func TestSessionRunClosesManagerConnectionAfterStartFailure(t *testing.T) {
	ctx, cancelSession := context.WithCancelCause(client.WithConfig(t.Context(), client.GetDefaultConfig()))
	defer cancelSession(context.Canceled)
	s := newTestStreamSession(ctx)
	s.handlers = tunnel.NewPool()
	// NewClient stays idle, so this test opens no network connection.
	conn, err := grpc.NewClient("passthrough:///127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	s.managerConn = conn
	s.ownsManagerConn = true
	startErr := errors.New("session failed before starting DNS")
	initErrs := make(chan error, 1)
	s.runWithStart(initErrs, cancelSession, func(log.Group) error { return startErr })

	require.ErrorIs(t, <-initErrs, startErr)
	require.Equal(t, connectivity.Shutdown, conn.GetState(), "failed startup must close the session's manager connection")
}
