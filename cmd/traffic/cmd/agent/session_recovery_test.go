package agent

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
)

type sessionRecoveryManager struct {
	rpc.ManagerClient
	arrive func(context.Context) error
	remain func(context.Context) error
	delta  func(context.Context) grpc.ServerStreamingClient[rpc.InterceptInfoDelta]
	depart atomic.Int32
}

func (*sessionRecoveryManager) Version(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*rpc.VersionInfo2, error) {
	return &rpc.VersionInfo2{Version: "v2.31.2"}, ctx.Err()
}

func (m *sessionRecoveryManager) ArriveAsAgent(ctx context.Context, _ *rpc.AgentInfo, _ ...grpc.CallOption) (*rpc.SessionInfo, error) {
	if err := m.arrive(ctx); err != nil {
		return nil, err
	}
	return &rpc.SessionInfo{SessionId: "agent:pod-uid"}, nil
}

func (m *sessionRecoveryManager) Remain(ctx context.Context, _ *rpc.RemainRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, m.remain(ctx)
}

func (m *sessionRecoveryManager) Depart(context.Context, *rpc.SessionInfo, ...grpc.CallOption) (*emptypb.Empty, error) {
	m.depart.Add(1)
	return &emptypb.Empty{}, nil
}

func (*sessionRecoveryManager) WatchLogLevel(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (grpc.ServerStreamingClient[rpc.LogLevelRequest], error) {
	return newFakeInterceptReadinessStream[rpc.LogLevelRequest](ctx), nil
}

func (m *sessionRecoveryManager) WatchInterceptsDelta(ctx context.Context, _ *rpc.SessionInfo, _ ...grpc.CallOption) (grpc.ServerStreamingClient[rpc.InterceptInfoDelta], error) {
	return m.delta(ctx), nil
}

type sessionRecoveryState struct {
	State
	handle func(context.Context, []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest
}

func (*sessionRecoveryState) AgentConfig() *agentconfig.Sidecar {
	return &agentconfig.Sidecar{WatchRetryInterval: time.Millisecond}
}

func (*sessionRecoveryState) SetManager(*rpc.SessionInfo, rpc.ManagerClient, semver.Version) {}

func (*sessionRecoveryState) RefreshQuicAgentListener(context.Context, context.Context) {}

func (s *sessionRecoveryState) HandleIntercepts(ctx context.Context, intercepts []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest {
	return s.handle(ctx, intercepts)
}

func fastManagerHeartbeats(t *testing.T) {
	t.Helper()
	oldInterval := managerHeartbeatInterval
	managerHeartbeatInterval = time.Millisecond
	t.Cleanup(func() { managerHeartbeatInterval = oldInterval })
}

func TestRemainRetriesTransientErrorsAndReturnsSessionLoss(t *testing.T) {
	fastManagerHeartbeats(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*managerHeartbeatTimeout)
	defer cancel()
	calls := 0
	mgr := &sessionRecoveryManager{remain: func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), managerHeartbeatTimeout)
		calls++
		if calls == 1 {
			return status.Error(codes.Unavailable, "temporary transport failure")
		}
		return status.Error(codes.NotFound, "session expired")
	}}
	err := remainLoop(ctx, mgr, &rpc.SessionInfo{SessionId: "agent:pod-uid"})
	require.Equal(t, codes.NotFound, status.Code(err))
	require.Equal(t, 2, calls)
}

func TestAgentRecoversMissingSessionWithOpenInterceptWatch(t *testing.T) {
	fastManagerHeartbeats(t)
	ctx, cancel := context.WithTimeout(interceptReadinessContext(t), 5*time.Second)
	defer cancel()
	streams := make(chan *fakeInterceptReadinessStream[rpc.InterceptInfoDelta], 2)
	loseSession := make(chan struct{})
	secondArrival := make(chan struct{})
	allowArrival := make(chan struct{})
	staleSnapshot := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	var arrivals atomic.Int32
	mgr := &sessionRecoveryManager{
		arrive: func(ctx context.Context) error {
			if arrivals.Add(1) == 2 {
				close(secondArrival)
				select {
				case <-allowArrival:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
		remain: func(context.Context) error {
			if arrivals.Load() == 1 {
				select {
				case <-loseSession:
					return status.Error(codes.NotFound, "session expired")
				default:
				}
			}
			return nil
		},
		delta: func(ctx context.Context) grpc.ServerStreamingClient[rpc.InterceptInfoDelta] {
			stream := newFakeInterceptReadinessStream[rpc.InterceptInfoDelta](ctx)
			streams <- stream
			return stream
		},
	}
	st := &sessionRecoveryState{handle: func(_ context.Context, snapshot []*rpc.InterceptInfo) []*rpc.ReviewInterceptRequest {
		if len(snapshot) == 1 && snapshot[0].Id == "stale" {
			close(staleSnapshot)
			select {
			case <-releaseSnapshot:
			case <-ctx.Done():
			}
		}
		return nil
	}}
	oldClientFactory := NewExtendedManagerClient
	NewExtendedManagerClient = func(*grpc.ClientConn, rpc.ManagerClient) rpc.ManagerClient { return mgr }
	t.Cleanup(func() { NewExtendedManagerClient = oldClientFactory })
	results := make(chan error, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		talkToManagerLoop(ctx, time.Millisecond, func(ctx context.Context) error {
			err := TalkToManager(ctx, "127.0.0.1:1", &rpc.AgentInfo{PodUid: "pod-uid"}, st)
			results <- err
			return err
		})
	}()
	t.Cleanup(func() {
		cancel()
		awaitReadinessSignal(t, done, "agent cleanup")
	})

	first := <-streams
	first.values <- &rpc.InterceptInfoDelta{}
	require.Eventually(t, func() bool { return readinessMarkerExists(ctx) }, time.Second, time.Millisecond)
	first.values <- &rpc.InterceptInfoDelta{Upserts: map[string]*rpc.InterceptInfo{"stale": {Id: "stale"}}}
	awaitReadinessSignal(t, staleSnapshot, "in-flight snapshot")
	close(loseSession)
	require.Eventually(t, func() bool { return !readinessMarkerExists(ctx) }, time.Second, time.Millisecond)

	// The old snapshot is still being applied, so session cleanup cannot yet run.
	require.EqualValues(t, 1, arrivals.Load())
	close(releaseSnapshot)
	awaitReadinessSignal(t, secondArrival, "new agent registration")
	require.Equal(t, codes.NotFound, status.Code(<-results))
	require.False(t, readinessMarkerExists(ctx))
	require.Zero(t, mgr.depart.Load())

	close(allowArrival)
	second := <-streams
	require.False(t, readinessMarkerExists(ctx))
	second.values <- &rpc.InterceptInfoDelta{}
	require.Eventually(t, func() bool { return readinessMarkerExists(ctx) }, time.Second, time.Millisecond)
	cancel()
	awaitReadinessSignal(t, done, "agent shutdown")
	require.False(t, readinessMarkerExists(context.WithoutCancel(ctx)))
	require.EqualValues(t, 1, mgr.depart.Load())
}
