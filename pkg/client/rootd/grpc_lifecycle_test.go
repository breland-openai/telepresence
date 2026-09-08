package rootd

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
)

func TestConnectWaitsForDisconnectedSessionCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := client.GetDefaultConfig()
		ctx := client.WithConfig(t.Context(), cfg)
		sessionCtx, cancelSession := context.WithCancelCause(ctx)
		defer cancelSession(context.Canceled)
		s := newService(cfg, true)
		s.Context = ctx
		s.session = newTestStreamSession(sessionCtx)
		s.sessionCancel = cancelSession
		s.sessionRunning = make(chan struct{})
		oldSessionDone := s.sessionRunning
		finishCleanup := sync.OnceFunc(func() { close(oldSessionDone) })
		defer finishCleanup()

		disconnected := make(chan error, 1)
		go func() {
			_, err := s.Disconnect(ctx, &emptypb.Empty{})
			disconnected <- err
		}()
		synctest.Wait()
		require.ErrorIs(t, sessionCtx.Err(), context.Canceled)

		// Invalid JSON exposes advancement into session setup without touching
		// host routing or requiring a traffic-manager connection.
		connected := make(chan error, 1)
		go func() {
			_, err := s.Connect(ctx, &rpc.NetworkConfig{ClientConfig: []byte("{")})
			connected <- err
		}()
		synctest.Wait()

		var connectErr error
		returned := false
		select {
		case connectErr = <-connected:
			returned = true
			t.Errorf("Connect advanced before the disconnected session finished cleanup: %v", connectErr)
		default:
		}

		finishCleanup()
		synctest.Wait()
		require.NoError(t, <-disconnected)
		if !returned {
			connectErr = <-connected
		}
		_, configErr := client.UnmarshalJSONConfig([]byte("{"), false)
		require.Error(t, configErr)
		require.EqualError(t, connectErr, configErr.Error())
	})
}

func TestConnectCleanupWaitHonorsCancellation(t *testing.T) {
	for _, cancelDaemon := range []bool{false, true} {
		name := "caller"
		if cancelDaemon {
			name = "daemon"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := client.GetDefaultConfig()
				daemonCtx, stopDaemon := context.WithCancel(client.WithConfig(t.Context(), cfg))
				defer stopDaemon()
				callerCtx, stopCaller := context.WithCancel(t.Context())
				defer stopCaller()
				s := newService(cfg, true)
				s.Context = daemonCtx
				s.sessionRunning = make(chan struct{})
				defer close(s.sessionRunning)

				connected := make(chan error, 1)
				go func() {
					_, err := s.Connect(callerCtx, &rpc.NetworkConfig{ClientConfig: []byte("{")})
					connected <- err
				}()
				synctest.Wait()

				if cancelDaemon {
					stopDaemon()
				} else {
					stopCaller()
				}
				synctest.Wait()
				select {
				case err := <-connected:
					require.Equal(t, codes.Canceled, status.Code(err),
						"Connect should return cancellation while cleanup is pending, got %v", err)
				default:
					t.Error("Connect did not return after cancellation while cleanup was pending")
				}
			})
		})
	}
}
