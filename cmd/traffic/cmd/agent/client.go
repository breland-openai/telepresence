package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	empty "google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/clog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/dos"
	"github.com/telepresenceio/telepresence/v2/pkg/grpc/watcher"
	"github.com/telepresenceio/telepresence/v2/pkg/log"
	"github.com/telepresenceio/telepresence/v2/pkg/maps"
)

type interceptsStringer []*rpc.InterceptInfo

func (is interceptsStringer) String() string {
	sb := strings.Builder{}
	sb.WriteByte('[')
	for i, ii := range is {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(ii.Id)
		sb.WriteByte(' ')
		sb.WriteString(ii.Disposition.String())
	}
	sb.WriteByte(']')
	return sb.String()
}

var NewExtendedManagerClient func(conn *grpc.ClientConn, ossManager rpc.ManagerClient) rpc.ManagerClient //nolint:gochecknoglobals // extension point

// managerHandshakeTimeout bounds the initial unary RPCs made before the agent
// can enter its reconnecting watch loops. Without a timeout, a manager that
// accepts the transport but stops servicing unary RPCs can leave the agent
// stuck forever before /tmp/agent/ready is created.
var managerHandshakeTimeout = 10 * time.Second //nolint:gochecknoglobals // overridden by tests

var managerHeartbeatInterval = time.Minute //nolint:gochecknoglobals // overridden by tests

const (
	agentSessionIDPrefix         = "agent:"
	initialInterceptSyncTimeout  = 30 * time.Second
	managerSessionRPCTimeout     = 10 * time.Second
	managerHeartbeatFailureLimit = 3
)

type interceptReadiness struct {
	mu           sync.Mutex
	synchronized bool
	generation   uint64
	initialSync  chan struct{}
	initialOnce  sync.Once
}

type interceptSnapshot struct {
	intercepts []*rpc.InterceptInfo
	generation uint64
}

type interceptReadinessStream[T any] struct {
	grpc.ServerStreamingClient[T]
	ctx       context.Context
	readiness *interceptReadiness
}

func (s *interceptReadinessStream[T]) Recv() (*T, error) {
	value, err := s.ServerStreamingClient.Recv()
	if err != nil && s.ctx.Err() == nil {
		if readinessErr := s.readiness.setUnsynchronized(s.ctx); readinessErr != nil {
			return nil, readinessErr
		}
	}
	return value, err
}

func newInterceptReadiness() *interceptReadiness {
	return &interceptReadiness{initialSync: make(chan struct{})}
}

func (r *interceptReadiness) currentGeneration() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.generation
}

func (r *interceptReadiness) setSynchronized(ctx context.Context, generation uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if generation != r.generation {
		return nil
	}
	if r.synchronized {
		return nil
	}
	file, err := dos.OpenFile(ctx, readyFile, os.O_CREATE|os.O_WRONLY, 0o666)
	if err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		_ = dos.Remove(ctx, readyFile)
		return err
	}
	if err = ctx.Err(); err != nil {
		_ = dos.Remove(ctx, readyFile)
		return err
	}
	r.synchronized = true
	r.initialOnce.Do(func() { close(r.initialSync) })
	return nil
}

func (r *interceptReadiness) setUnsynchronized(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.synchronized = false
	r.generation++
	if err := dos.Remove(ctx, readyFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (r *interceptReadiness) cancelIfUnsynchronized(cancel context.CancelFunc) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.synchronized {
		return false
	}
	// Hold the lock so a fresh snapshot cannot restore readiness before cancellation.
	cancel()
	return true
}

func (r *interceptReadiness) awaitInitialSync(ctx context.Context, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-r.initialSync:
		return nil
	case <-ctx.Done():
		return nil
	case <-timer.C:
		clog.Warnf(ctx, "traffic-agent has not synchronized its initial intercept snapshot within %s; continuing to wait", timeout)
	}

	select {
	case <-r.initialSync:
		return nil
	case <-ctx.Done():
		return nil
	}
}

// Agent session IDs are deterministic from the pod UID. Older managers can
// return an empty ID when a retried arrival finds the first call already
// committed, so recover the same ID before opening session streams.
func recoverAgentSessionID(info *rpc.AgentInfo, session *rpc.SessionInfo) {
	if session.GetSessionId() == "" && info.GetPodUid() != "" {
		session.SessionId = agentSessionIDPrefix + info.GetPodUid()
	}
}

func TalkToManager(ctx context.Context, address string, info *rpc.AgentInfo, state State) error {
	// processCtx outlives this single manager session: it bounds the QUIC listener
	// RefreshQuicAgentListener may start below, which must survive the reconnects
	// that create new instances of the (per-connection) ctx shadowed on the next
	// line.
	processCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	readiness := newInterceptReadiness()
	if err := readiness.setUnsynchronized(ctx); err != nil {
		return fmt.Errorf("clear traffic-agent readiness: %w", err)
	}

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	tokenPath := filepath.Join(agentconfig.ManagerTokenMountPath, agentconfig.ManagerTokenFile)
	if _, err := os.Stat(tokenPath); err == nil {
		clog.Debug(ctx, "attaching manager token to traffic-manager connection")
		opts = append(opts, grpc.WithPerRPCCredentials(newManagerTokenCredentials(tokenPath)))
	} else {
		clog.Debug(ctx, "no manager token file present, connecting without a manager token")
	}

	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	manager := rpc.NewManagerClient(conn)
	if NewExtendedManagerClient != nil {
		manager = NewExtendedManagerClient(conn, manager)
	}

	handshakeCtx, handshakeCancel := context.WithTimeout(ctx, managerHandshakeTimeout)
	ver, err := manager.Version(handshakeCtx, &empty.Empty{})
	handshakeCancel()
	if err != nil {
		return fmt.Errorf("get manager version: %w", err)
	}

	verStr := strings.TrimPrefix(ver.Version, "v")
	clog.Infof(ctx, "Connected to Manager %s", verStr)
	mgrVer, err := semver.Parse(verStr)
	if err != nil {
		return fmt.Errorf("failed to parse manager version %q: %s", verStr, err)
	}

	handshakeCtx, handshakeCancel = context.WithTimeout(ctx, managerHandshakeTimeout)
	session, err := manager.ArriveAsAgent(handshakeCtx, info)
	handshakeCancel()
	if err != nil {
		return fmt.Errorf("arrive as agent: %w", err)
	}
	recoverAgentSessionID(info, session)

	state.SetManager(session, manager, mgrVer)

	// Best-effort and non-blocking: a manager without QUIC enabled, or one that's
	// briefly unreachable for this one RPC, must never delay or fail the rest of
	// this session. Bounded independently of ctx's lifetime so a slow manager can't
	// wedge this goroutine past the session's own teardown.
	go func() {
		fetchCtx, fetchCancel := context.WithTimeout(ctx, 10*time.Second)
		defer fetchCancel()
		state.RefreshQuicAgentListener(processCtx, fetchCtx)
	}()

	// Create the /tmp/agent directory if it doesn't exist
	// We use this to place a file which conveys 'readiness'
	// The presence of this file is used in the readiness check.
	if err := dos.MkdirAll(ctx, readyDir, 0o777); err != nil {
		return err
	}
	var sessionRecoveryErr error
	defer func() {
		// The ctx might well be cancelled at this point but is used as parent during
		// the timed clean-up to keep logging intact.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()

		if err := readiness.setUnsynchronized(ctx); err != nil {
			clog.Errorf(ctx, "clear traffic-agent readiness: %v", err)
		}

		// Reset state by processing an empty snapshot
		// - clear out any intercepts
		// - set forwarding to the app
		state.HandleIntercepts(ctx, nil)

		// Depart session
		if sessionRecoveryErr == nil {
			if _, err := manager.Depart(ctx, session); err != nil {
				clog.Errorf(ctx, "depart session: %+v", err)
			}
		}
	}()

	var recoveryOnce sync.Once
	recoverSession := func(err error) error {
		if err == nil || processCtx.Err() != nil {
			return err
		}
		recoveryOnce.Do(func() {
			sessionRecoveryErr = err
			// Cancel before withdrawing readiness so an old snapshot cannot
			// make the session ready again. Do not Depart: the manager may still
			// own this session, and a new connection can reuse its ID.
			cancel()
			if readinessErr := readiness.setUnsynchronized(context.WithoutCancel(ctx)); readinessErr != nil {
				sessionRecoveryErr = errors.Join(err, fmt.Errorf("clear traffic-agent readiness during recovery: %w", readinessErr))
			}
		})
		return err
	}

	wg := log.NewGroup(ctx)

	retryInterval := state.AgentConfig().WatchRetryInterval
	wg.Go("logLevelWatch", func(ctx context.Context) error {
		return logLevelWatchLoop(ctx, state.AgentConfig().LogLevel, manager, retryInterval)
	})
	snapshots := make(chan interceptSnapshot, 1)
	wg.Go("interceptWatch", func(ctx context.Context) error {
		return interceptWatchLoop(ctx, manager, session, info, snapshots, retryInterval, readiness)
	})
	wg.Go("handleIntercept", func(ctx context.Context) error {
		err := handleInterceptLoop(ctx, manager, session, snapshots, state, readiness)
		switch status.Code(err) {
		case codes.DeadlineExceeded, codes.Unavailable:
			return recoverSession(err)
		default:
			return err
		}
	})
	wg.Go("remain", func(ctx context.Context) error {
		return recoverSession(remainLoop(ctx, manager, session, readiness, cancel))
	})
	wg.Go("initialInterceptSync", func(ctx context.Context) error {
		return readiness.awaitInitialSync(ctx, initialInterceptSyncTimeout)
	})

	err = wg.Wait()
	if sessionRecoveryErr != nil {
		return sessionRecoveryErr
	}
	return err
}

func logLevelWatchLoop(ctx context.Context, level slog.Level, manager rpc.ManagerClient, retryInterval time.Duration) error {
	timedLevel := log.NewTimedLevel(level, clog.SetTreeLevel)
	return watcher.WatchWithRetry(ctx, "WatchLogLevel", retryInterval,
		func(ctx context.Context) (grpc.ServerStreamingClient[rpc.LogLevelRequest], error) {
			return manager.WatchLogLevel(ctx, &empty.Empty{})
		},
		func(ll *rpc.LogLevelRequest) error {
			lvl, err := clog.ParseLevel(ll.LogLevel)
			if err != nil {
				return err
			}
			duration := time.Duration(0)
			if ll.Duration != nil {
				duration = ll.Duration.AsDuration()
			}
			timedLevel.Set(ctx, lvl, duration)
			return nil
		},
		nil,
	)
}

func interceptWatchLoop(
	ctx context.Context,
	manager rpc.ManagerClient,
	session *rpc.SessionInfo,
	info *rpc.AgentInfo,
	snapshots chan interceptSnapshot,
	retryInterval time.Duration,
	readiness *interceptReadiness,
) error {
	// Call WatchIntercepts and publish the snapshots on the channel
	snapMap := make(map[string]*rpc.InterceptInfo)
	reconnectAgent := func() error {
		if err := readiness.setUnsynchronized(ctx); err != nil {
			return err
		}
		clear(snapMap)
		repairCtx, cancel := context.WithTimeout(ctx, managerSessionRPCTimeout)
		defer cancel()
		_, err := manager.ReconnectAgent(repairCtx, &rpc.ReconnectAgentRequest{
			Session: session,
			Agent:   info,
		})
		return err
	}
	err := watcher.WatchWithRetry(ctx, "WatchInterceptsDelta", retryInterval,
		func(ctx context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoDelta], error) {
			stream, err := manager.WatchInterceptsDelta(ctx, session)
			if err != nil {
				return nil, err
			}
			return &interceptReadinessStream[rpc.InterceptInfoDelta]{
				ServerStreamingClient: stream,
				ctx:                   ctx,
				readiness:             readiness,
			}, nil
		},
		func(delta *rpc.InterceptInfoDelta) error {
			maps.DeltaUpdate(snapMap, delta.Upserts, delta.Removals)
			return sendInterceptSnapshot(ctx, snapshots, interceptSnapshot{
				intercepts: maps.Values(snapMap),
				generation: readiness.currentGeneration(),
			})
		}, reconnectAgent)
	if err != nil && status.Code(err) == codes.Unimplemented {
		// Fall back to streaming all intercepts if the traffic manager doesn't support delta updates.'
		clog.Warnf(ctx, "WatchInterceptsDelta is not implemented by the traffic-manager, falling back to WatchIntercepts and full snapshots")
		err = watcher.WatchWithRetry(ctx, "WatchIntercepts", retryInterval,
			func(ctx context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoSnapshot], error) {
				stream, err := manager.WatchIntercepts(ctx, session)
				if err != nil {
					return nil, err
				}
				return &interceptReadinessStream[rpc.InterceptInfoSnapshot]{
					ServerStreamingClient: stream,
					ctx:                   ctx,
					readiness:             readiness,
				}, nil
			},
			func(snapshot *rpc.InterceptInfoSnapshot) error {
				snapMap = retainWaitingIntercepts(snapMap, snapshot.Intercepts)
				return sendInterceptSnapshot(ctx, snapshots, interceptSnapshot{
					intercepts: snapshot.Intercepts,
					generation: readiness.currentGeneration(),
				})
			}, reconnectAgent)
	}
	return err
}

// A full watch decodes new messages for every route. Preserve the identity of
// an unchanged, versioned route before snapshots can be coalesced. Managers
// without revisions may be asking for a fresh review on every full snapshot.
func retainWaitingIntercepts(previous map[string]*rpc.InterceptInfo, infos []*rpc.InterceptInfo) map[string]*rpc.InterceptInfo {
	current := make(map[string]*rpc.InterceptInfo)
	for i, info := range infos {
		if info.Disposition != rpc.InterceptDispositionType_WAITING || info.ModifiedAt == nil {
			continue
		}
		if old := previous[info.Id]; old != nil && proto.Equal(old, info) {
			infos[i] = old
		}
		current[info.Id] = infos[i]
	}
	return current
}

func sendInterceptSnapshot(ctx context.Context, snapshots chan interceptSnapshot, snapshot interceptSnapshot) error {
	// The watch loop is the sole sender. Replace a pending complete snapshot so
	// the watch keeps draining while the consumer processes or reviews intercepts.
	select {
	case <-snapshots:
	default:
	}
	select {
	case snapshots <- snapshot:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func remainLoop(ctx context.Context, manager rpc.ManagerClient, session *rpc.SessionInfo, readiness *interceptReadiness, cancelSession context.CancelFunc) error {
	// Loop calling Remain
	ticker := time.NewTicker(managerHeartbeatInterval)
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		heartbeatCtx, cancel := context.WithTimeout(ctx, managerSessionRPCTimeout)
		_, err := manager.Remain(heartbeatCtx, &rpc.RemainRequest{Session: session})
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			failures = 0
			continue
		}
		if status.Code(err) == codes.NotFound {
			return fmt.Errorf("agent session lost: %w", err)
		}
		failures++
		// A lost response does not prove the manager missed the heartbeat.
		if failures >= managerHeartbeatFailureLimit && readiness.cancelIfUnsynchronized(cancelSession) {
			return status.Errorf(codes.Unavailable, "agent session unconfirmed after %d heartbeats: %v", failures, err)
		}
		clog.Warnf(ctx, "remain: %v", err)
	}
}

func handleInterceptLoop(
	ctx context.Context,
	manager rpc.ManagerClient,
	session *rpc.SessionInfo,
	snapshots <-chan interceptSnapshot,
	state State,
	readiness *interceptReadiness,
) error {
	var approvals interceptApprovals
	var generation uint64
	for {
		select {
		case <-ctx.Done():
			return nil
		case snapshot := <-snapshots:
			clog.Debugf(ctx, "HandleIntercepts %s", interceptsStringer(snapshot.intercepts))
			reviews := state.HandleIntercepts(ctx, snapshot.intercepts)
			if err := readiness.setSynchronized(ctx, snapshot.generation); err != nil {
				return err
			}
			if generation != snapshot.generation {
				generation = snapshot.generation
				approvals = nil
			}
			var err error
			approvals, err = approvals.submit(ctx, manager, session, snapshot.intercepts, reviews)
			if err != nil {
				return err
			}
		}
	}
}

// Delta upserts replace an intercept's immutable message. Its identity stays
// stable across unrelated deltas, even when intermediate snapshots are dropped.
type interceptApprovals map[*rpc.InterceptInfo]*rpc.ReviewInterceptRequest

func (previous interceptApprovals) submit(ctx context.Context, manager rpc.ManagerClient, session *rpc.SessionInfo,
	infos []*rpc.InterceptInfo, reviews []*rpc.ReviewInterceptRequest,
) (interceptApprovals, error) {
	if len(reviews) == 0 {
		return nil, nil
	}
	waiting := make(map[string]*rpc.InterceptInfo, len(reviews))
	for _, info := range infos {
		if info.Disposition == rpc.InterceptDispositionType_WAITING {
			waiting[info.Id] = info
		}
	}
	next := make(interceptApprovals, len(reviews))
	for _, review := range reviews {
		review.Session = session
		info := waiting[review.Id]
		if ack := previous[info]; ack != nil && proto.Equal(ack, review) {
			next[info] = ack
			continue
		}
		reviewCtx, cancel := context.WithTimeout(ctx, managerSessionRPCTimeout)
		_, err := manager.ReviewIntercept(reviewCtx, review)
		cancel()
		if err != nil {
			if status.Code(err) == codes.NotFound {
				// Removal can race an in-flight review.
				continue
			}
			return nil, err
		}
		if info != nil && review.Disposition == rpc.InterceptDispositionType_ACTIVE {
			next[info] = proto.CloneOf(review)
			clog.Infof(ctx, "Allowing non-conflicting intercept %q to become active", info.Id)
		}
	}
	return next, nil
}
