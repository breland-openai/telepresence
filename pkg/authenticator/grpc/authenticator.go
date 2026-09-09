package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/telepresenceio/clog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/authenticator"
	"github.com/telepresenceio/telepresence/v2/pkg/authenticator"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func RegisterAuthenticatorServer(srv *grpc.Server, clientConfigProvider k8sapi.ClientConfigProvider) {
	managerTokenProvider, _ := clientConfigProvider.(ManagerTokenProvider)
	rpc.RegisterAuthenticatorServer(srv, &AuthenticatorServer{
		authenticator:        authenticator.NewService(clientConfigProvider),
		managerTokenProvider: managerTokenProvider,
	})
}

type Authenticator interface {
	GetExecCredentials(ctx context.Context, contextName string) ([]byte, error)
}

type ManagerTokenProvider interface {
	ManagerToken(context.Context, []byte, string, string) (string, error)
}

type AuthenticatorServer struct {
	rpc.UnsafeAuthenticatorServer

	authenticator        Authenticator
	managerTokenProvider ManagerTokenProvider
}

// GetContextExecCredentials returns credentials for a particular Kubernetes context on the host machine.
func (h *AuthenticatorServer) GetContextExecCredentials(ctx context.Context, request *rpc.GetContextExecCredentialsRequest) (*rpc.GetContextExecCredentialsResponse, error) {
	clog.Debugf(ctx, "GetContextExecCredentials(%s)", request.ContextName)
	rawExecCredentials, err := h.authenticator.GetExecCredentials(ctx, request.ContextName)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve exec credentils: %w", err)
	}

	return &rpc.GetContextExecCredentialsResponse{
		RawCredentials: rawExecCredentials,
	}, nil
}

// GetManagerToken only accepts a user-daemon-issued connection capability. The
// caller cannot select a path or ask the privileged daemon to read one.
func (h *AuthenticatorServer) GetManagerToken(ctx context.Context, request *rpc.GetManagerTokenRequest) (*rpc.GetManagerTokenResponse, error) {
	if h.managerTokenProvider == nil {
		return nil, status.Error(codes.PermissionDenied, "invalid traffic-manager credential callback")
	}
	token, err := h.managerTokenProvider.ManagerToken(ctx, request.GetCapability(), request.GetManagerPodUid(), request.GetDevboxProxyAudience())
	if err != nil {
		return nil, err
	}
	return &rpc.GetManagerTokenResponse{Token: token}, nil
}
