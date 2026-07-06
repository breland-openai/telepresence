package trafficmgr

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/clog/testutil"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
)

type remainErrorManager struct {
	manager.UnimplementedManagerServer
	err error
}

func (s *remainErrorManager) Remain(context.Context, *manager.RemainRequest) (*emptypb.Empty, error) {
	return nil, s.err
}

func TestRemainPropagatesManagerFailure(t *testing.T) {
	expectedCode := codes.DeadlineExceeded
	conn, cleanup := dialRemainTestManager(t, &remainErrorManager{
		err: status.Error(expectedCode, "stuck manager connection"),
	})
	defer cleanup()

	ctx := client.WithConfig(testutil.NewContext(t, false), client.GetDefaultConfig())
	s := &session{
		Cluster:     &k8s.Cluster{Kubeconfig: &k8s.Kubeconfig{Context: ctx}},
		managerConn: conn,
		sessionInfo: &manager.SessionInfo{SessionId: "test-session"},
	}

	err := s.remain()

	require.Equal(t, expectedCode, status.Code(err))
}

func dialRemainTestManager(t *testing.T, server manager.ManagerServer) (*grpc.ClientConn, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	manager.RegisterManagerServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return conn, func() {
		conn.Close()
		grpcServer.Stop()
		listener.Close()
	}
}
