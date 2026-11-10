package agent

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTalkToManagerLoopWaitsAfterSlowFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const retryInterval = time.Second
		var failedAt, retriedAt time.Time
		talkToManagerLoop(ctx, retryInterval, func(context.Context) error {
			if failedAt.IsZero() {
				time.Sleep(2 * retryInterval)
				failedAt = time.Now()
				return status.Error(codes.DeadlineExceeded, "registration timed out")
			}
			retriedAt = time.Now()
			cancel()
			return nil
		})
		require.Equal(t, retryInterval, retriedAt.Sub(failedAt))
	})
}

func TestTalkToManagerLoopRetriesInactivePod(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "grpc status",
			err:  status.Error(codes.Aborted, "inactivated pod"),
		},
		{
			name: "wrapped grpc status",
			err:  fmt.Errorf("arrive as agent: %w", status.Error(codes.Aborted, "inactivated pod")),
		},
		{
			name: "repeatedly wrapped grpc status",
			err: fmt.Errorf("connect to manager: %w",
				fmt.Errorf("arrive as agent: %w", status.Error(codes.Aborted, "inactivated pod"))),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			attempts := 0
			talkToManagerLoop(ctx, time.Millisecond, func(context.Context) error {
				attempts++
				if attempts == 1 {
					return tc.err
				}
				cancel()
				return nil
			})

			require.Equal(t, 2, attempts)
			require.ErrorIs(t, ctx.Err(), context.Canceled)
		})
	}
}

func TestTalkToManagerLoopStopsOnPermanentErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{
			name: "already exists",
			err:  status.Error(codes.AlreadyExists, "agent already exists"),
		},
		{
			name: "canceled",
			err:  status.Error(codes.Canceled, "manager connection canceled"),
		},
		{
			name: "aborted",
			err:  status.Error(codes.Aborted, "agent session is invalid"),
		},
		{
			name: "wrapped aborted",
			err:  fmt.Errorf("arrive as agent: %w", status.Error(codes.Aborted, "agent session is invalid")),
		},
		{
			name: "different inactive pod message",
			err:  status.Error(codes.Aborted, "permanently inactivated pod"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			attempts := 0
			talkToManagerLoop(ctx, time.Millisecond, func(context.Context) error {
				attempts++
				return tc.err
			})

			require.Equal(t, 1, attempts)
			require.NoError(t, ctx.Err())
		})
	}
}

func TestTalkToManagerLoopStopsWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	attempts := 0
	talkToManagerLoop(ctx, time.Hour, func(context.Context) error {
		attempts++
		cancel()
		return status.Error(codes.Aborted, "inactivated pod")
	})
	talkToManagerLoop(ctx, time.Hour, func(context.Context) error {
		attempts++
		return nil
	})

	require.Equal(t, 1, attempts)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
