package agent

import (
	"context"
	"io"
	"net/netip"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func TestInterceptReviewsSkipUnrelatedUpdates(t *testing.T) {
	for _, mode := range []string{"delta", "full"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) { testInterceptReviewWatch(t, mode) })
		})
	}
}

func testInterceptReviewWatch(t *testing.T, mode string) {
	ctx, cancel := context.WithCancel(interceptReadinessContext(t))
	cs := newTestContainerState(t, netip.MustParseAddr("192.0.2.1"), false)
	cn := cs.Container()
	cn.Intercepts[0].ServiceUID = "test-service"
	state := cs.GlobalState()
	state.AddContainerState(cn.Name, cs)
	target := agentconfig.NewInterceptTarget(cn.Intercepts)
	handler := cs.newPortHandler(ctx, types.PortAndProto{Port: target.ContainerPort(), Proto: target.Protocol()}, cn.Intercepts)
	state.AddInterceptState(state.NewInterceptState(handler, target, cn.Name))

	delta := newFakeInterceptReadinessStream[rpc.InterceptInfoDelta](ctx)
	full := newFakeInterceptReadinessStream[rpc.InterceptInfoSnapshot](ctx)
	var votes []rpc.InterceptDispositionType
	var lastSftpPort int32
	var reviewErr error
	var holdReview chan struct{}
	mgr := &interceptReadinessManager{
		delta: func(context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoDelta], error) {
			if mode == "full" {
				return nil, status.Error(codes.Unimplemented, "use full snapshots")
			}
			return delta, nil
		},
		full: func(context.Context) (grpc.ServerStreamingClient[rpc.InterceptInfoSnapshot], error) { return full, nil },
		review: func(ctx context.Context, r *rpc.ReviewInterceptRequest) error {
			if r.Id != "waiting" {
				t.Errorf("unexpected review for %q", r.Id)
			}
			votes = append(votes, r.Disposition)
			lastSftpPort = r.SftpPort
			if holdReview != nil {
				select {
				case <-holdReview:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return reviewErr
		},
	}
	session := &rpc.SessionInfo{SessionId: "agent"}
	readiness := newInterceptReadiness()
	snapshots := make(chan interceptSnapshot, 1)
	done := make(chan error, 2)
	go func() {
		done <- interceptWatchLoop(ctx, mgr, session, &rpc.AgentInfo{}, snapshots, time.Second, readiness)
	}()
	go func() { done <- handleInterceptLoop(ctx, mgr, session, snapshots, state, readiness) }()
	defer func() {
		cancel()
		require.NoError(t, <-done)
		require.NoError(t, <-done)
	}()
	publish := reviewPublisher(mode, delta, full)
	active := rpc.InterceptDispositionType_ACTIVE
	reject := rpc.InterceptDispositionType_AGENT_ERROR
	waiting := &rpc.InterceptInfo{
		Id: "waiting", Disposition: rpc.InterceptDispositionType_WAITING, ModifiedAt: timestamppb.New(time.Unix(1, 0)),
		Spec: &rpc.InterceptSpec{ServiceUid: "test-service", ServicePort: 80, Protocol: "TCP", Mechanism: "http", ContainerName: cn.Name, HeaderFilters: map[string]string{"x-route": "one"}},
	}
	other := proto.CloneOf(waiting)
	other.Id = "other"
	other.Disposition = active
	other.Spec.HeaderFilters["x-route"] = "two"

	publish(waiting.Id, waiting)
	for range 5 {
		publish(other.Id, other)
		publish(other.Id, nil)
	}
	require.Equal(t, []rpc.InterceptDispositionType{active}, votes)

	other.Spec.HeaderFilters["x-route"] = "one"
	publish(other.Id, other)
	publish(other.Id, nil)
	require.Equal(t, []rpc.InterceptDispositionType{active, reject, active}, votes)

	// A fresh revision may require this replica to replace the previous reviewer.
	waiting.ModifiedAt = timestamppb.New(time.Unix(2, 0))
	reviewErr = status.Error(codes.NotFound, "intercept removed")
	publish(waiting.Id, waiting)
	reviewErr = nil
	other.Spec.HeaderFilters["x-route"] = "two"
	publish(other.Id, other)
	publish(other.Id, nil)
	require.Equal(t, []rpc.InterceptDispositionType{active, reject, active, active, active}, votes)

	publish(waiting.Id, nil)
	holdReview = make(chan struct{})
	publish(waiting.Id, waiting)
	require.Len(t, votes, 6)
	// An intervening removal can be coalesced away while a review is in flight.
	publish(waiting.Id, nil)
	publish(waiting.Id, waiting)
	close(holdReview)
	synctest.Wait()
	require.Len(t, votes, 7)

	if mode == "delta" {
		delta.errors <- io.EOF
	} else {
		full.errors <- io.EOF
	}
	publish(waiting.Id, waiting)
	require.Len(t, votes, 8, "a new watch needs a fresh approval")

	state.SetFileSharingPorts(0, 2222)
	publish(other.Id, other)
	require.Len(t, votes, 9, "a change in the approval payload must be sent")
	require.EqualValues(t, 2222, lastSftpPort)

	waiting.ModifiedAt = nil
	publish(waiting.Id, waiting)
	require.Len(t, votes, 10)
	if mode == "delta" {
		publish(waiting.Id, waiting)
	} else {
		publish(other.Id, nil)
	}
	require.Len(t, votes, 11, "explicit upserts and unversioned full snapshots may require a new reviewer")
}

func reviewPublisher(mode string, delta *fakeInterceptReadinessStream[rpc.InterceptInfoDelta],
	full *fakeInterceptReadinessStream[rpc.InterceptInfoSnapshot],
) func(string, *rpc.InterceptInfo) {
	routes := make(map[string]*rpc.InterceptInfo)
	return func(id string, info *rpc.InterceptInfo) {
		if info == nil {
			delete(routes, id)
		} else {
			routes[id] = info
		}
		if mode == "delta" {
			d := &rpc.InterceptInfoDelta{}
			if info == nil {
				d.Removals = []string{id}
			} else {
				d.Upserts = map[string]*rpc.InterceptInfo{id: proto.CloneOf(info)}
			}
			delta.values <- d
		} else {
			s := &rpc.InterceptInfoSnapshot{}
			for _, v := range routes {
				s.Intercepts = append(s.Intercepts, proto.CloneOf(v))
			}
			full.values <- s
		}
		synctest.Wait()
	}
}
