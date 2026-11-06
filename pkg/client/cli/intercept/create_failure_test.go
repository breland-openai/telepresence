package intercept

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/telepresence/rpc/v2/connector"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/daemon"
)

type failingCreateClient struct {
	createRequestUserClient
	requests []*connector.CreateInterceptRequest
	fail     func(context.Context) error
}

func (*failingCreateClient) Status(context.Context, *emptypb.Empty, ...grpc.CallOption) (*connector.ConnectInfo, error) {
	return &connector.ConnectInfo{}, nil
}

func (c *failingCreateClient) CreateIntercept(ctx context.Context, req *connector.CreateInterceptRequest, _ ...grpc.CallOption) (*manager.InterceptInfo, error) {
	c.requests = append(c.requests, req)
	return nil, c.fail(ctx)
}

func TestCreateFailurePreservesRequestedService(t *testing.T) {
	for _, code := range []codes.Code{codes.ResourceExhausted, codes.Canceled, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ud := &failingCreateClient{}
			switch code {
			case codes.Canceled:
				ud.fail = func(ctx context.Context) error {
					cancel()
					<-ctx.Done()
					return status.FromContextError(ctx.Err()).Err()
				}
			case codes.DeadlineExceeded:
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 20*time.Millisecond)
				defer stop()
				ud.fail = func(ctx context.Context) error {
					<-ctx.Done()
					return status.FromContextError(ctx.Err()).Err()
				}
			default:
				ud.fail = func(context.Context) error { return status.Error(code, "checking Service coverage") }
			}
			cmd := &Command{Name: "local", AgentName: "echo", Namespace: "default", ServiceName: "both", Mechanism: "http"}
			cmd.EnvFlags.File = filepath.Join(t.TempDir(), "intercept.env")
			run := func(ctx context.Context) error {
				state := NewState(cmd, nil).(*state)
				acquired, err := state.create(daemon.WithUserClient(ctx, ud))
				require.False(t, acquired)
				return err
			}
			err := run(ctx)
			require.Equal(t, code, status.Code(err))
			require.Len(t, ud.requests, 1)
			require.NoFileExists(t, cmd.EnvFlags.File)

			ud.fail = func(context.Context) error { return status.Error(codes.Unavailable, "still pending") }
			require.Equal(t, codes.Unavailable, status.Code(run(t.Context())))
			require.Len(t, ud.requests, 2)
			for _, req := range ud.requests {
				require.Equal(t, "both", req.Spec.ServiceName)
				require.Equal(t, "echo", req.Spec.Agent)
			}
		})
	}
}
