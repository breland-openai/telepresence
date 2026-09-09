package k8s

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"k8s.io/client-go/rest"

	"github.com/telepresenceio/clog/testutil"
	authrpc "github.com/telepresenceio/telepresence/rpc/v2/authenticator"
)

type managerTokenCallbackTestServer struct {
	authrpc.UnimplementedAuthenticatorServer
	capability []byte
	token      atomic.Value
}

func (s *managerTokenCallbackTestServer) GetManagerToken(_ context.Context, request *authrpc.GetManagerTokenRequest) (*authrpc.GetManagerTokenResponse, error) {
	if !bytes.Equal(s.capability, request.GetCapability()) {
		return nil, status.Error(codes.PermissionDenied, "private forbidden message")
	}
	return &authrpc.GetManagerTokenResponse{Token: s.token.Load().(string)}, nil
}

func TestRootManagerTokenCallbackRotatesAndPreventsFallbackGRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(testutil.NewContext(t, false))
	t.Cleanup(cancel)
	capability := bytes.Repeat([]byte{'a'}, ManagerTokenCallbackCapabilitySize)
	provider := &managerTokenCallbackTestServer{capability: append([]byte(nil), capability...)}
	provider.token.Store("first-manager-token")
	callbackListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	callbackServer := grpc.NewServer()
	authrpc.RegisterAuthenticatorServer(callbackServer, provider)
	go func() { _ = callbackServer.Serve(callbackListener) }()
	t.Cleanup(callbackServer.Stop)
	kc := &Kubeconfig{RestConfig: &rest.Config{BearerToken: "kubernetes-fallback"}, ManagerTokenFile: "/root/should-not-read", ManagerTokenFileSet: true}
	require.NoError(t, kc.ConfigureRootManagerTokenCallback(ctx, true, callbackListener.Addr().String(), capability))
	capability[0] = 'z'
	auth, err := connectionManagerAuthentication(ctx, kc)
	require.NoError(t, err)
	require.Equal(t, "kubernetes-fallback", kc.RestConfig.BearerToken)
	require.Empty(t, kc.ManagerTokenFile)
	require.False(t, kc.ManagerTokenFileSet)

	listener := bufconn.Listen(1024 * 1024)
	var calls atomic.Int32
	seen := make(chan string, 3)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		calls.Add(1)
		md, _ := metadata.FromIncomingContext(ctx)
		seen <- strings.Join(md.Get("authorization"), ",")
		return handler(ctx, req)
	}))
	healthrpc.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///manager-token-test", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(auth.credentials))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := healthrpc.NewHealthClient(conn)
	_, err = client.Check(ctx, &healthrpc.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, "Bearer first-manager-token", <-seen)
	provider.token.Store("private invalid\nvalue")
	_, err = client.Check(ctx, &healthrpc.HealthCheckRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.NotContains(t, err.Error(), "private")
	require.EqualValues(t, 1, calls.Load())
	_, err = connectionManagerAuthentication(ctx, kc)
	require.Error(t, err)
	provider.token.Store("second-manager-token")
	_, err = client.Check(ctx, &healthrpc.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, "Bearer second-manager-token", <-seen)
	callbackServer.Stop()
	_, err = client.Check(ctx, &healthrpc.HealthCheckRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.EqualValues(t, 2, calls.Load(), "callback failure must prevent the manager call, not use kubeconfig or anonymous auth")
}

func TestRootManagerTokenCallbackRestrictsEndpointAndClearsInheritedEnvironment(t *testing.T) {
	ctx := testutil.NewContext(t, false)
	capability := bytes.Repeat([]byte{'a'}, ManagerTokenCallbackCapabilitySize)
	for _, address := range []string{"", "localhost:1234", "8.8.8.8:1234", "0.0.0.0:1234", "[::]:1234", "127.0.0.1:0"} {
		kc := &Kubeconfig{ManagerTokenFile: "/root/file", ManagerTokenFileSet: true}
		err := kc.ConfigureRootManagerTokenCallback(ctx, true, address, capability)
		require.Error(t, err, address)
		require.Empty(t, kc.ManagerTokenFile)
		require.False(t, kc.ManagerTokenFileSet)
	}
	for _, value := range [][]byte{nil, []byte("private")} {
		err := (&Kubeconfig{}).ConfigureRootManagerTokenCallback(ctx, true, "127.0.0.1:1234", value)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private")
	}
	kc := &Kubeconfig{RestConfig: &rest.Config{BearerToken: "legacy-kubernetes-token"}, ManagerTokenFile: "/root/private", ManagerTokenFileSet: true}
	require.NoError(t, kc.ConfigureRootManagerTokenCallback(ctx, false, "", nil))
	auth, err := connectionManagerAuthentication(ctx, kc)
	require.NoError(t, err)
	metadata, err := auth.credentials.GetRequestMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, "Bearer legacy-kubernetes-token", metadata["authorization"])
}
