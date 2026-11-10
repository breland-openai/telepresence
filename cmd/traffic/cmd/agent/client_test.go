package agent

import (
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/clog/testutil"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/quictunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/dos"
	"github.com/telepresenceio/telepresence/v2/pkg/dos/aferofs"
	"github.com/telepresenceio/telepresence/v2/pkg/quicfwd"
	"github.com/telepresenceio/telepresence/v2/pkg/sessiontoken"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

type handshakeManagerClient struct {
	rpc.ManagerClient
	version func(context.Context) (*rpc.VersionInfo2, error)
	arrive  func(context.Context) (*rpc.SessionInfo, error)
}

func (c handshakeManagerClient) Version(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*rpc.VersionInfo2, error) {
	return c.version(ctx)
}

func (c handshakeManagerClient) ArriveAsAgent(ctx context.Context, _ *rpc.AgentInfo, _ ...grpc.CallOption) (*rpc.SessionInfo, error) {
	return c.arrive(ctx)
}

func TestTalkToManagerTimesOutHandshakeRPCs(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		client  handshakeManagerClient
	}{
		{
			name:    "version",
			timeout: managerVersionRPCTimeout,
			client: handshakeManagerClient{
				version: func(ctx context.Context) (*rpc.VersionInfo2, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
				arrive: func(context.Context) (*rpc.SessionInfo, error) {
					t.Fatal("ArriveAsAgent should not be called after Version times out")
					return nil, nil
				},
			},
		},
		{
			name:    "arrive",
			timeout: managerRegistrationTimeout,
			client: handshakeManagerClient{
				version: func(context.Context) (*rpc.VersionInfo2, error) {
					return &rpc.VersionInfo2{Version: "v2.29.2"}, nil
				},
				arrive: func(ctx context.Context) (*rpc.SessionInfo, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := interceptReadinessContext(t)
				file, err := dos.OpenFile(ctx, readyFile, os.O_CREATE|os.O_WRONLY, 0o600)
				require.NoError(t, err)
				require.NoError(t, file.Close())

				oldClientFactory := NewExtendedManagerClient
				NewExtendedManagerClient = func(*grpc.ClientConn, rpc.ManagerClient) rpc.ManagerClient { return tt.client }
				t.Cleanup(func() { NewExtendedManagerClient = oldClientFactory })

				start := time.Now()
				err = TalkToManager(ctx, "127.0.0.1:1", &rpc.AgentInfo{}, nil)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, tt.timeout, time.Since(start))
				require.False(t, readinessMarkerExists(ctx))
			})
		})
	}
}

func TestSlowAgentRegistrationWaitsForSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(interceptReadinessContext(t))
		defer cancel()
		streams := make(chan *fakeInterceptReadinessStream[rpc.InterceptInfoDelta], 1)
		manager := &sessionRecoveryManager{
			arrive: func(ctx context.Context) error {
				time.Sleep(managerVersionRPCTimeout + time.Second)
				return ctx.Err()
			},
			delta: func(ctx context.Context) grpc.ServerStreamingClient[rpc.InterceptInfoDelta] {
				stream := newFakeInterceptReadinessStream[rpc.InterceptInfoDelta](ctx)
				streams <- stream
				return stream
			},
		}
		state := &sessionRecoveryState{handle: func(context.Context, []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest { return nil }}
		oldClientFactory := NewExtendedManagerClient
		NewExtendedManagerClient = func(*grpc.ClientConn, rpc.ManagerClient) rpc.ManagerClient { return manager }
		t.Cleanup(func() { NewExtendedManagerClient = oldClientFactory })
		done := make(chan error, 1)
		go func() { done <- TalkToManager(ctx, "127.0.0.1:1", &rpc.AgentInfo{PodUid: "pod-uid"}, state) }()

		select {
		case stream := <-streams:
			require.False(t, readinessMarkerExists(ctx))
			stream.values <- &rpc.InterceptInfoDelta{}
		case err := <-done:
			t.Fatalf("registration ended before the intercept watch opened: %v", err)
		}
		synctest.Wait()
		require.True(t, readinessMarkerExists(ctx))
		cancel()
		require.NoError(t, <-done)
	})
}

func TestRecoverAgentSessionID(t *testing.T) {
	t.Run("fills empty ID from pod UID", func(t *testing.T) {
		session := &rpc.SessionInfo{ManagerInstallId: "manager-install"}
		recoverAgentSessionID(&rpc.AgentInfo{PodUid: "pod-uid"}, session)

		require.Equal(t, "agent:pod-uid", session.SessionId)
		require.Equal(t, "manager-install", session.ManagerInstallId)
	})

	t.Run("keeps manager-provided ID", func(t *testing.T) {
		session := &rpc.SessionInfo{SessionId: "manager-session"}
		recoverAgentSessionID(&rpc.AgentInfo{PodUid: "pod-uid"}, session)

		require.Equal(t, "manager-session", session.SessionId)
	})
}

type interceptReadinessState struct {
	State
	handle func(context.Context, []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest
}

func (s *interceptReadinessState) HandleIntercepts(
	ctx context.Context,
	intercepts []*rpc.InterceptInfo,
) []*rpc.ReviewInterceptRequest {
	return s.handle(ctx, intercepts)
}

type interceptReadinessManager struct {
	rpc.ManagerClient
	delta     func(context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoDelta], error)
	full      func(context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoSnapshot], error)
	reconnect func(context.Context) error
	review    func(context.Context, *rpc.ReviewInterceptRequest) error
}

func (m *interceptReadinessManager) WatchInterceptsDelta(
	ctx context.Context,
	_ *rpc.SessionInfo,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[rpc.InterceptInfoDelta], error) {
	return m.delta(ctx)
}

func (m *interceptReadinessManager) WatchIntercepts(
	ctx context.Context,
	_ *rpc.SessionInfo,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[rpc.InterceptInfoSnapshot], error) {
	return m.full(ctx)
}

func (m *interceptReadinessManager) ReconnectAgent(
	ctx context.Context,
	_ *rpc.ReconnectAgentRequest,
	_ ...grpc.CallOption,
) (*emptypb.Empty, error) {
	if m.reconnect != nil {
		if err := m.reconnect(ctx); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

func (m *interceptReadinessManager) ReviewIntercept(
	ctx context.Context,
	review *rpc.ReviewInterceptRequest,
	_ ...grpc.CallOption,
) (*emptypb.Empty, error) {
	if m.review != nil {
		if err := m.review(ctx, review); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

type fakeInterceptReadinessStream[T any] struct {
	grpc.ClientStream
	ctx    context.Context
	values chan *T
	errors chan error
}

func newFakeInterceptReadinessStream[T any](ctx context.Context) *fakeInterceptReadinessStream[T] {
	return &fakeInterceptReadinessStream[T]{
		ctx:    ctx,
		values: make(chan *T),
		errors: make(chan error),
	}
}

func (s *fakeInterceptReadinessStream[T]) Recv() (*T, error) {
	select {
	case value := <-s.values:
		return value, nil
	case err := <-s.errors:
		return nil, err
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (*fakeInterceptReadinessStream[T]) CloseSend() error {
	return nil
}

func interceptReadinessContext(t *testing.T) context.Context {
	t.Helper()
	ctx := dos.WithFS(context.Background(), aferofs.Wrap(afero.NewMemMapFs()))
	require.NoError(t, dos.MkdirAll(ctx, readyDir, 0o755))
	return ctx
}

func readinessMarkerExists(ctx context.Context) bool {
	_, err := dos.Stat(ctx, readyFile)
	return err == nil
}

func awaitReadinessSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestInterceptReadinessAfterSnapshotApplied(t *testing.T) {
	tests := []struct {
		name     string
		snapshot []*rpc.InterceptInfo
	}{
		{name: "empty"},
		{
			name: "active",
			snapshot: []*rpc.InterceptInfo{{
				Id:          "active-intercept",
				Disposition: rpc.InterceptDispositionType_ACTIVE,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(interceptReadinessContext(t))
			defer cancel()

			readiness := newInterceptReadiness()
			snapshots := make(chan interceptSnapshot)
			entered := make(chan struct{})
			release := make(chan struct{})
			state := &interceptReadinessState{
				handle: func(_ context.Context, snapshot []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest {
					require.Equal(t, tt.snapshot, snapshot)
					close(entered)
					<-release
					return nil
				},
			}
			done := make(chan error, 1)
			go func() {
				done <- handleInterceptLoop(ctx, nil, &rpc.SessionInfo{}, snapshots, state, readiness)
			}()

			snapshots <- interceptSnapshot{intercepts: tt.snapshot, generation: readiness.currentGeneration()}
			awaitReadinessSignal(t, entered, "snapshot application")
			require.False(t, readinessMarkerExists(ctx))
			close(release)
			awaitReadinessSignal(t, readiness.initialSync, "initial intercept synchronization")
			require.True(t, readinessMarkerExists(ctx))

			cancel()
			require.NoError(t, <-done)
		})
	}
}

func TestInterceptReadinessDoesNotWaitForReview(t *testing.T) {
	ctx, cancel := context.WithCancel(interceptReadinessContext(t))
	defer cancel()

	readiness := newInterceptReadiness()
	snapshots := make(chan interceptSnapshot)
	entered := make(chan struct{})
	release := make(chan struct{})
	manager := &interceptReadinessManager{
		review: func(_ context.Context, review *rpc.ReviewInterceptRequest) error {
			require.Equal(t, "agent-session", review.Session.SessionId)
			close(entered)
			<-release
			return nil
		},
	}
	state := &interceptReadinessState{
		handle: func(context.Context, []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest {
			return []*rpc.ReviewInterceptRequest{{Id: "waiting-intercept"}}
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- handleInterceptLoop(ctx, manager, &rpc.SessionInfo{SessionId: "agent-session"}, snapshots, state, readiness)
	}()

	snapshots <- interceptSnapshot{
		intercepts: []*rpc.InterceptInfo{{Id: "waiting-intercept", Disposition: rpc.InterceptDispositionType_WAITING}},
		generation: readiness.currentGeneration(),
	}
	awaitReadinessSignal(t, entered, "intercept review")
	awaitReadinessSignal(t, readiness.initialSync, "snapshot application")
	require.True(t, readinessMarkerExists(ctx))
	close(release)

	cancel()
	require.NoError(t, <-done)
}

func TestInterceptReviewExpiresBeforeSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(interceptReadinessContext(t), 2*managerSessionRPCTimeout)
		defer cancel()
		manager := &interceptReadinessManager{review: func(ctx context.Context, _ *rpc.ReviewInterceptRequest) error {
			<-ctx.Done()
			return ctx.Err()
		}}
		state := &interceptReadinessState{handle: func(context.Context, []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest {
			return []*rpc.ReviewInterceptRequest{{Id: "intercept"}}
		}}
		snapshots := make(chan interceptSnapshot, 1)
		snapshots <- interceptSnapshot{}
		err := handleInterceptLoop(ctx, manager, &rpc.SessionInfo{}, snapshots, state, newInterceptReadiness())
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NoError(t, ctx.Err(), "a review timeout must not wait for the session to expire")
	})
}

func TestInterceptWatchCoalescesSnapshotsWhileConsumerIsBusy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(interceptReadinessContext(t))
		stream := newFakeInterceptReadinessStream[rpc.InterceptInfoDelta](ctx)
		manager := &interceptReadinessManager{delta: func(context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoDelta], error) {
			return stream, nil
		}}
		snapshots := make(chan interceptSnapshot, 1)
		watchDone := make(chan error, 1)
		go func() {
			watchDone <- interceptWatchLoop(ctx, manager, &rpc.SessionInfo{}, &rpc.AgentInfo{}, snapshots, time.Second, newInterceptReadiness(), func() {})
		}()
		defer func() {
			cancel()
			require.NoError(t, <-watchDone)
		}()

		kept := &rpc.InterceptInfo{Id: "kept"}
		removed := &rpc.InterceptInfo{Id: "removed"}
		added := &rpc.InterceptInfo{Id: "added"}
		fed := make(chan struct{})
		go func() {
			defer close(fed)
			for _, delta := range []*rpc.InterceptInfoDelta{
				{Upserts: map[string]*rpc.InterceptInfo{kept.Id: kept, removed.Id: removed}},
				{Upserts: map[string]*rpc.InterceptInfo{added.Id: added}, Removals: []string{removed.Id}},
				{},
			} {
				select {
				case stream.values <- delta:
				case <-ctx.Done():
					return
				}
			}
		}()
		select {
		case <-fed:
		case <-time.After(time.Second):
			t.Fatal("watch stopped receiving while the snapshot consumer was busy")
		}
		require.ElementsMatch(t, []*rpc.InterceptInfo{kept, added}, (<-snapshots).intercepts)
	})
}

func TestInterceptWatchRetriesStalledRepair(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(interceptReadinessContext(t), 2*managerRegistrationTimeout)
		stream := newFakeInterceptReadinessStream[rpc.InterceptInfoDelta](ctx)
		stream.values = make(chan *rpc.InterceptInfoDelta, 1)
		stream.values <- &rpc.InterceptInfoDelta{Upserts: map[string]*rpc.InterceptInfo{"fresh": {Id: "fresh"}}}
		watches, repairs, refreshes := 0, 0, 0
		manager := &interceptReadinessManager{
			delta: func(context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoDelta], error) {
				watches++
				if watches == 1 {
					return nil, status.Error(codes.Unavailable, "watch interrupted")
				}
				return stream, nil
			},
			reconnect: func(ctx context.Context) error {
				repairs++
				if repairs == 1 {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			},
		}
		snapshots := make(chan interceptSnapshot, 1)
		watchDone := make(chan error, 1)
		start := time.Now()
		go func() {
			watchDone <- interceptWatchLoop(ctx, manager, &rpc.SessionInfo{}, &rpc.AgentInfo{}, snapshots, time.Millisecond, newInterceptReadiness(), func() {
				refreshes++
			})
		}()
		defer func() {
			cancel()
			require.NoError(t, <-watchDone)
		}()
		select {
		case snapshot := <-snapshots:
			require.Equal(t, "fresh", snapshot.intercepts[0].Id)
			require.Equal(t, 2, repairs)
			require.Equal(t, 1, refreshes, "only a successful repair should refresh credentials")
			require.GreaterOrEqual(t, time.Since(start), managerRegistrationTimeout)
		case <-ctx.Done():
			t.Fatal("watch did not recover before the session expired")
		}
	})
}

func TestInterceptReadinessWithdrawnUntilReconnectSnapshotApplied(t *testing.T) {
	ctx, cancel := context.WithCancel(interceptReadinessContext(t))
	defer cancel()

	readiness := newInterceptReadiness()
	snapshots := make(chan interceptSnapshot)
	first := newFakeInterceptReadinessStream[rpc.InterceptInfoDelta](ctx)
	second := newFakeInterceptReadinessStream[rpc.InterceptInfoDelta](ctx)
	reconnected := make(chan struct{})
	applied := make(chan string, 2)
	var attempts int
	var attemptsMu sync.Mutex
	manager := &interceptReadinessManager{
		delta: func(context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoDelta], error) {
			attemptsMu.Lock()
			defer attemptsMu.Unlock()
			attempts++
			if attempts == 1 {
				return first, nil
			}
			return second, nil
		},
		reconnect: func(context.Context) error {
			close(reconnected)
			return nil
		},
	}
	state := &interceptReadinessState{
		handle: func(_ context.Context, snapshot []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest {
			require.Len(t, snapshot, 1)
			applied <- snapshot[0].Id
			return nil
		},
	}
	watchDone := make(chan error, 1)
	handleDone := make(chan error, 1)
	go func() {
		watchDone <- interceptWatchLoop(ctx, manager, &rpc.SessionInfo{}, &rpc.AgentInfo{}, snapshots, 250*time.Millisecond, readiness, func() {})
	}()
	go func() {
		handleDone <- handleInterceptLoop(ctx, manager, &rpc.SessionInfo{}, snapshots, state, readiness)
	}()

	first.values <- &rpc.InterceptInfoDelta{
		Upserts: map[string]*rpc.InterceptInfo{"first": {Id: "first", Disposition: rpc.InterceptDispositionType_ACTIVE}},
	}
	require.Equal(t, "first", <-applied)
	awaitReadinessSignal(t, readiness.initialSync, "first snapshot")
	require.True(t, readinessMarkerExists(ctx))

	first.errors <- io.EOF
	require.Eventually(t, func() bool { return !readinessMarkerExists(ctx) }, 100*time.Millisecond, time.Millisecond)
	select {
	case <-reconnected:
		t.Fatal("watch reconnection occurred before readiness withdrawal was observed")
	default:
	}
	awaitReadinessSignal(t, reconnected, "watch reconnection")
	require.False(t, readinessMarkerExists(ctx))

	second.values <- &rpc.InterceptInfoDelta{
		Upserts: map[string]*rpc.InterceptInfo{"second": {Id: "second", Disposition: rpc.InterceptDispositionType_ACTIVE}},
	}
	require.Equal(t, "second", <-applied)
	require.Eventually(t, func() bool { return readinessMarkerExists(ctx) }, time.Second, time.Millisecond)

	cancel()
	require.NoError(t, <-watchDone)
	require.NoError(t, <-handleDone)
}

func TestInterceptReadinessSupportsLegacySnapshots(t *testing.T) {
	for _, duringReceive := range []bool{false, true} {
		name := "stream provider"
		if duringReceive {
			name = "stream receive"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(interceptReadinessContext(t))
			defer cancel()

			readiness := newInterceptReadiness()
			snapshots := make(chan interceptSnapshot)
			deltaStream := newFakeInterceptReadinessStream[rpc.InterceptInfoDelta](ctx)
			fullStream := newFakeInterceptReadinessStream[rpc.InterceptInfoSnapshot](ctx)
			manager := &interceptReadinessManager{
				delta: func(context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoDelta], error) {
					if duringReceive {
						return deltaStream, nil
					}
					return nil, status.Error(codes.Unimplemented, "delta snapshots unavailable")
				},
				full: func(context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoSnapshot], error) {
					return fullStream, nil
				},
			}
			state := &interceptReadinessState{
				handle: func(_ context.Context, snapshot []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest {
					require.Empty(t, snapshot)
					return nil
				},
			}
			watchDone := make(chan error, 1)
			handleDone := make(chan error, 1)
			go func() {
				watchDone <- interceptWatchLoop(ctx, manager, &rpc.SessionInfo{}, &rpc.AgentInfo{}, snapshots, time.Millisecond, readiness, func() {})
			}()
			go func() {
				handleDone <- handleInterceptLoop(ctx, manager, &rpc.SessionInfo{}, snapshots, state, readiness)
			}()

			if duringReceive {
				deltaStream.errors <- status.Error(codes.Unimplemented, "delta snapshots unavailable")
			}
			fullStream.values <- &rpc.InterceptInfoSnapshot{}
			awaitReadinessSignal(t, readiness.initialSync, "legacy snapshot")
			require.True(t, readinessMarkerExists(ctx))

			cancel()
			require.NoError(t, <-watchDone)
			require.NoError(t, <-handleDone)
		})
	}
}

func TestInterceptReadinessInitialSyncWaitsForDelayedSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(interceptReadinessContext(t))
	defer cancel()

	readiness := newInterceptReadiness()
	snapshots := make(chan interceptSnapshot)
	stream := newFakeInterceptReadinessStream[rpc.InterceptInfoDelta](ctx)
	manager := &interceptReadinessManager{
		delta: func(context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoDelta], error) {
			return stream, nil
		},
		reconnect: func(context.Context) error {
			t.Error("a delayed initial snapshot must not reconnect the agent session")
			return nil
		},
	}
	state := &interceptReadinessState{
		handle: func(context.Context, []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest {
			return nil
		},
	}

	watchDone := make(chan error, 1)
	go func() {
		watchDone <- interceptWatchLoop(ctx, manager, &rpc.SessionInfo{}, &rpc.AgentInfo{}, snapshots, time.Millisecond, readiness, func() {})
	}()
	handleDone := make(chan error, 1)
	go func() {
		handleDone <- handleInterceptLoop(ctx, manager, &rpc.SessionInfo{}, snapshots, state, readiness)
	}()
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- readiness.awaitInitialSync(ctx, time.Millisecond)
	}()

	select {
	case err := <-syncDone:
		t.Fatalf("initial synchronization stopped before a snapshot arrived: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	require.False(t, readinessMarkerExists(ctx))

	stream.values <- &rpc.InterceptInfoDelta{}
	awaitReadinessSignal(t, readiness.initialSync, "delayed initial snapshot")
	require.NoError(t, <-syncDone)
	require.True(t, readinessMarkerExists(ctx))

	cancel()
	require.NoError(t, <-watchDone)
	require.NoError(t, <-handleDone)
}

func TestInterceptReadinessInitialSyncCancellationAfterTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(interceptReadinessContext(t))
	readiness := newInterceptReadiness()
	done := make(chan error, 1)
	go func() {
		done <- readiness.awaitInitialSync(ctx, time.Millisecond)
	}()

	select {
	case err := <-done:
		t.Fatalf("initial synchronization stopped before cancellation: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	require.False(t, readinessMarkerExists(ctx))

	cancel()
	require.NoError(t, <-done)
	require.False(t, readinessMarkerExists(ctx))
}

func TestInterceptReadinessCanceledSessionCannotBecomeReady(t *testing.T) {
	ctx, cancel := context.WithCancel(interceptReadinessContext(t))
	readiness := newInterceptReadiness()
	cancel()

	require.ErrorIs(t, readiness.setSynchronized(ctx, readiness.currentGeneration()), context.Canceled)
	require.False(t, readinessMarkerExists(ctx))
}

func TestInterceptReadinessClearsStaleMarker(t *testing.T) {
	ctx := interceptReadinessContext(t)
	file, err := dos.OpenFile(ctx, readyFile, os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	readiness := newInterceptReadiness()
	require.NoError(t, readiness.setUnsynchronized(ctx))
	require.False(t, readinessMarkerExists(ctx))
	require.NoError(t, readiness.setUnsynchronized(ctx))
}

func TestInterceptReadinessIgnoresStaleSnapshot(t *testing.T) {
	ctx := interceptReadinessContext(t)
	readiness := newInterceptReadiness()
	staleGeneration := readiness.currentGeneration()

	require.NoError(t, readiness.setUnsynchronized(ctx))
	require.NoError(t, readiness.setSynchronized(ctx, staleGeneration))
	require.False(t, readinessMarkerExists(ctx))

	require.NoError(t, readiness.setSynchronized(ctx, readiness.currentGeneration()))
	require.True(t, readinessMarkerExists(ctx))
}

func TestSendInterceptSnapshotReturnsWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sendInterceptSnapshot(ctx, make(chan interceptSnapshot), interceptSnapshot{})
	require.ErrorIs(t, err, context.Canceled)
}

type blockingStream[T any] struct {
	grpc.ClientStream
	ctx  context.Context
	fail <-chan struct{}
}

func (s *blockingStream[T]) Recv() (*T, error) {
	select {
	case <-s.ctx.Done():
		return nil, status.FromContextError(s.ctx.Err()).Err()
	case <-s.fail:
		return nil, status.Error(codes.Unavailable, "manager restarted")
	}
}

func (s *blockingStream[T]) CloseSend() error { return nil }

// restartableManager is an in-process manager whose CA can be swapped, and whose
// current intercept stream can be broken, to simulate a manager restart.
type restartableManager struct {
	rpc.ManagerClient
	mu              sync.Mutex
	ca              *quictunnel.CA
	failStream      chan struct{}
	reconnectCalled int
}

func (m *restartableManager) Version(context.Context, *emptypb.Empty, ...grpc.CallOption) (*rpc.VersionInfo2, error) {
	return &rpc.VersionInfo2{Version: "v2.33.0"}, nil
}

func (m *restartableManager) ArriveAsAgent(context.Context, *rpc.AgentInfo, ...grpc.CallOption) (*rpc.SessionInfo, error) {
	return &rpc.SessionInfo{SessionId: "session-a"}, nil
}

func (m *restartableManager) GetQuicAgentCert(_ context.Context, _ *rpc.SessionInfo, _ ...grpc.CallOption) (*rpc.QuicAgentCert, error) {
	m.mu.Lock()
	ca := m.ca
	m.mu.Unlock()
	sni := quicfwd.AgentSNI("test-pod-uid")
	cert, err := ca.MintServerCert(sni)
	if err != nil {
		return nil, err
	}
	certPEM, keyPEM, err := quictunnel.ServerCertToPEM(cert)
	if err != nil {
		return nil, err
	}
	return &rpc.QuicAgentCert{
		Enabled:            true,
		CertPem:            certPEM,
		KeyPem:             keyPEM,
		CaPem:              ca.CertPEM(),
		Sni:                sni,
		AuthenticationMode: "enforcing",
	}, nil
}

func (m *restartableManager) ReconnectAgent(context.Context, *rpc.ReconnectAgentRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconnectCalled++
	return &emptypb.Empty{}, nil
}

func (m *restartableManager) Remain(context.Context, *rpc.RemainRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (m *restartableManager) Depart(context.Context, *rpc.SessionInfo, ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (m *restartableManager) ReviewIntercept(context.Context, *rpc.ReviewInterceptRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (m *restartableManager) WatchLogLevel(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (grpc.ServerStreamingClient[rpc.LogLevelRequest], error) {
	return &blockingStream[rpc.LogLevelRequest]{ctx: ctx}, nil
}

func (m *restartableManager) WatchInterceptsDelta(ctx context.Context, _ *rpc.SessionInfo, _ ...grpc.CallOption) (grpc.ServerStreamingClient[rpc.InterceptInfoDelta], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &blockingStream[rpc.InterceptInfoDelta]{ctx: ctx, fail: m.failStream}, nil
}

// restart switches to a new CA and breaks the current intercept stream.
func (m *restartableManager) restart(ca *quictunnel.CA) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ca = ca
	old := m.failStream
	m.failStream = make(chan struct{})
	close(old)
}

func (m *restartableManager) reconnects() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reconnectCalled
}

func TestTalkToManager_RefreshesQuicCertAfterManagerRestart(t *testing.T) {
	ctx := dos.WithEnv(testutil.NewContext(t, false), dos.MapEnv{})
	ctx = dos.WithFS(ctx, aferofs.Wrap(afero.NewMemMapFs()))
	st := newFileShareAuthTestState(t, ctx)
	st.AgentConfig().WatchRetryInterval = 10 * time.Millisecond

	ca1, err := quictunnel.NewCA()
	require.NoError(t, err)
	ca2, err := quictunnel.NewCA()
	require.NoError(t, err)
	mgr := &restartableManager{ca: ca1, failStream: make(chan struct{})}

	oldClientFactory := NewExtendedManagerClient
	NewExtendedManagerClient = func(*grpc.ClientConn, rpc.ManagerClient) rpc.ManagerClient { return mgr }
	t.Cleanup(func() { NewExtendedManagerClient = oldClientFactory })

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- TalkToManager(runCtx, "127.0.0.1:1", &rpc.AgentInfo{PodUid: "test-pod-uid"}, st)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("TalkToManager did not return after cancel")
		}
	})

	verifies := func(ca *quictunnel.CA) bool {
		token, _, err := ca.MintSessionToken("session-a")
		if err != nil {
			return false
		}
		tokenCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(sessiontoken.MetadataKey, token))
		verified, err := st.fileShareAuth.verifySession(tokenCtx, tunnel.SessionID("session-a"))
		return err == nil && verified
	}
	require.Eventually(t, func() bool { return verifies(ca1) }, 5*time.Second, 10*time.Millisecond)

	mgr.restart(ca2)

	require.Eventually(t, func() bool { return verifies(ca2) }, 5*time.Second, 10*time.Millisecond,
		"agent must fetch the new manager's CA after ReconnectAgent")
	require.GreaterOrEqual(t, mgr.reconnects(), 1)
}
