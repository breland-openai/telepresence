package daemon

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/connector"
	rootdRpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
	"github.com/telepresenceio/telepresence/v2/pkg/client/userd"
	"github.com/telepresenceio/telepresence/v2/pkg/filelocation"
)

func TestConnectingStatusAndConcurrentCallerRemainResponsive(t *testing.T) {
	s := connectionTestService(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	s.newSession = func(_ userd.Service, ctx context.Context, _ *rpc.ConnectRequest, _ *k8s.Kubeconfig, _ *sync.WaitGroup) (userd.Session, *rpc.ConnectInfo, error) {
		calls.Add(1)
		close(started)
		<-release
		if ctx.Err() != nil {
			return nil, nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, nil, status.Error(codes.Aborted, "owner released")
	}
	owner := connectionTestStart(t, s, t.Context())
	connectionTestWait(t, started)

	statusCtx, cancelStatus := context.WithTimeout(t.Context(), time.Second)
	defer cancelStatus()
	statusResult := make(chan error, 1)
	go func() {
		_, err := s.Status(statusCtx, &emptypb.Empty{})
		statusResult <- err
	}()
	err := connectionTestResult(t, statusResult)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "connection in progress")
	require.NoError(t, statusCtx.Err())

	waiterCtx, cancelWaiter := context.WithCancel(t.Context())
	waiter := connectionTestStart(t, s, waiterCtx)
	cancelWaiter()
	require.Equal(t, codes.Canceled, status.Code(connectionTestResult(t, waiter)))
	require.Equal(t, int32(1), calls.Load())
	select {
	case <-owner:
		t.Fatal("canceling a competing caller canceled the owner")
	default:
	}
	close(release)
	require.Equal(t, codes.Aborted, status.Code(connectionTestResult(t, owner)))
}

func TestDisconnectCancelsConstructionWithoutOverlappingRetry(t *testing.T) {
	s := connectionTestService(t)
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	s.newSession = func(_ userd.Service, ctx context.Context, _ *rpc.ConnectRequest, _ *k8s.Kubeconfig, _ *sync.WaitGroup) (userd.Session, *rpc.ConnectInfo, error) {
		if calls.Add(1) != 1 {
			return nil, nil, status.Error(codes.Aborted, "retried")
		}
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil, nil, status.FromContextError(ctx.Err()).Err()
	}
	owner := connectionTestStart(t, s, t.Context())
	connectionTestWait(t, started)
	_, err := s.Disconnect(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	connectionTestWait(t, canceled)

	waiterCtx, cancel := context.WithCancel(t.Context())
	waiter := connectionTestStart(t, s, waiterCtx)
	cancel()
	require.Equal(t, codes.Canceled, status.Code(connectionTestResult(t, waiter)))
	require.Equal(t, int32(1), calls.Load())
	close(release)
	require.Equal(t, codes.Canceled, status.Code(connectionTestResult(t, owner)))
	require.Equal(t, codes.Aborted, status.Code(connectionTestResult(t, connectionTestStart(t, s, t.Context()))))
	require.Equal(t, int32(2), calls.Load())
}

func TestQuitCancelsConstructionWithoutWaitingForIt(t *testing.T) {
	s := connectionTestService(t)
	s.rootSessionInProc = true
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	s.newSession = func(_ userd.Service, ctx context.Context, _ *rpc.ConnectRequest, _ *k8s.Kubeconfig, _ *sync.WaitGroup) (userd.Session, *rpc.ConnectInfo, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil, nil, status.FromContextError(ctx.Err()).Err()
	}
	owner := connectionTestStart(t, s, t.Context())
	connectionTestWait(t, started)
	quit := make(chan error, 1)
	go func() {
		_, err := s.Quit(t.Context(), &emptypb.Empty{})
		quit <- err
	}()
	require.NoError(t, connectionTestResult(t, quit))
	connectionTestWait(t, canceled)
	select {
	case <-owner:
		t.Fatal("construction ended before its fake cleanup was released")
	default:
	}
	close(release)
	require.Equal(t, codes.Canceled, status.Code(connectionTestResult(t, owner)))
}

func TestCanceledConstructionClosesUnpublishedSession(t *testing.T) {
	s := connectionTestService(t)
	started := make(chan struct{})
	release := make(chan struct{})
	closed := make(chan struct{})
	s.newSession = func(_ userd.Service, _ context.Context, _ *rpc.ConnectRequest, _ *k8s.Kubeconfig, _ *sync.WaitGroup) (userd.Session, *rpc.ConnectInfo, error) {
		close(started)
		<-release
		return &unpublishedConnectionTestSession{closed: closed}, &rpc.ConnectInfo{}, nil
	}
	owner := connectionTestStart(t, s, t.Context())
	connectionTestWait(t, started)
	_, err := s.Disconnect(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	close(release)
	require.Equal(t, codes.Canceled, status.Code(connectionTestResult(t, owner)))
	connectionTestWait(t, closed)
	s.sessionLock.RLock()
	defer s.sessionLock.RUnlock()
	require.Nil(t, s.session)
}

func TestOwnerCancellationReachesConstructionSessionContext(t *testing.T) {
	s := connectionTestService(t)
	started := make(chan struct{})
	s.newSession = func(_ userd.Service, _ context.Context, _ *rpc.ConnectRequest, cfg *k8s.Kubeconfig, _ *sync.WaitGroup) (userd.Session, *rpc.ConnectInfo, error) {
		close(started)
		<-cfg.Done()
		return nil, nil, status.FromContextError(cfg.Err()).Err()
	}
	ownerCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	owner := connectionTestStart(t, s, ownerCtx)
	connectionTestWait(t, started)
	cancel()
	require.Equal(t, codes.Canceled, status.Code(connectionTestResult(t, owner)))
}

func TestPublishedSessionOutlivesConnectCallerAndIsReused(t *testing.T) {
	s := connectionTestService(t)
	var active *publishedConnectionTestSession
	var calls atomic.Int32
	s.newSession = func(_ userd.Service, _ context.Context, _ *rpc.ConnectRequest, cfg *k8s.Kubeconfig, _ *sync.WaitGroup) (userd.Session, *rpc.ConnectInfo, error) {
		calls.Add(1)
		active = &publishedConnectionTestSession{ctx: cfg, started: make(chan struct{})}
		return active, &rpc.ConnectInfo{Initial: true}, nil
	}
	ownerCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	owner := connectionTestStart(t, s, ownerCtx)
	require.NoError(t, connectionTestResult(t, owner))
	connectionTestWait(t, active.started)
	cancel()
	require.NoError(t, active.Err())
	info, err := s.Connect(t.Context(), &rpc.ConnectRequest{Name: "test"})
	require.NoError(t, err)
	require.False(t, info.Initial)
	require.Equal(t, "test", info.ConnectionName)
	require.Equal(t, int32(1), calls.Load())
	s.quit(false)
}

func TestActiveQuitBoundsStalledInterceptCleanup(t *testing.T) {
	s := connectionTestService(t)
	cleanupStarted := make(chan struct{})
	var active *publishedConnectionTestSession
	s.newSession = func(_ userd.Service, _ context.Context, _ *rpc.ConnectRequest, cfg *k8s.Kubeconfig, _ *sync.WaitGroup) (userd.Session, *rpc.ConnectInfo, error) {
		active = &publishedConnectionTestSession{ctx: cfg, started: make(chan struct{}), cleanupStarted: cleanupStarted}
		return active, &rpc.ConnectInfo{Initial: true}, nil
	}
	require.NoError(t, connectionTestResult(t, connectionTestStart(t, s, t.Context())))
	connectionTestWait(t, active.started)
	s.rootSessionInProc = true
	result := make(chan error, 1)
	go func() {
		_, err := s.Quit(t.Context(), &emptypb.Empty{})
		result <- err
	}()
	connectionTestWait(t, cleanupStarted)
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("active quit remained stuck in intercept cleanup")
	}
	require.ErrorIs(t, active.Err(), context.Canceled)
}

type unpublishedConnectionTestSession struct {
	userd.Session
	closed chan struct{}
}

func (s *unpublishedConnectionTestSession) Close() { close(s.closed) }

type publishedConnectionTestSession struct {
	userd.Session
	ctx            context.Context
	started        chan struct{}
	cleanupStarted chan struct{}
}

func (s *publishedConnectionTestSession) Deadline() (time.Time, bool) { return s.ctx.Deadline() }

func (s *publishedConnectionTestSession) Done() <-chan struct{} { return s.ctx.Done() }

func (s *publishedConnectionTestSession) Err() error { return s.ctx.Err() }

func (s *publishedConnectionTestSession) Value(key any) any                       { return s.ctx.Value(key) }
func (*publishedConnectionTestSession) MarkActivity()                             {}
func (*publishedConnectionTestSession) Close()                                    {}
func (*publishedConnectionTestSession) CheckStatus(*rpc.ConnectRequest) error     { return nil }
func (*publishedConnectionTestSession) RootSessionEndMetrics() *rootdRpc.Activity { return nil }
func (s *publishedConnectionTestSession) ClearIngestsAndIntercepts(ctx context.Context) error {
	if s.cleanupStarted != nil {
		close(s.cleanupStarted)
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (*publishedConnectionTestSession) Status(context.Context) (*rpc.ConnectInfo, error) {
	return &rpc.ConnectInfo{ConnectionName: "test"}, nil
}

func (s *publishedConnectionTestSession) UpdateStatus(ctx context.Context, _ *rpc.ConnectRequest) (*rpc.ConnectInfo, error) {
	return s.Status(ctx)
}

func (s *publishedConnectionTestSession) Run() {
	close(s.started)
	<-s.ctx.Done()
}

func connectionTestService(t *testing.T) *service {
	t.Helper()
	ctx := filelocation.WithUserHomeDir(t.Context(), t.TempDir())
	ctx = filelocation.WithAppUserConfigDir(ctx, t.TempDir())
	ctx = filelocation.WithAppUserCacheDir(ctx, t.TempDir())
	ctx = filelocation.WithAppSystemConfigDir(ctx, t.TempDir())
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	cfg := client.GetDefaultConfig()
	return newService(client.WithConfig(ctx, cfg), cancel, cfg, grpc.NewServer())
}

func connectionTestStart(t *testing.T, s *service, ctx context.Context) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		_, err := s.Connect(ctx, &rpc.ConnectRequest{
			Name: "test",
			KubeconfigData: []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://cluster.example
contexts:
- name: test
  context:
    cluster: test
    user: test
    namespace: default
current-context: test
users:
- name: test
  user:
    token: test
`),
		})
		result <- err
	}()
	return result
}

func connectionTestWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connection test")
	}
}

func connectionTestResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connection result")
		return nil
	}
}
