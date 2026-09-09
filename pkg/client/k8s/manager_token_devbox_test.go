package k8s

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	authrpc "github.com/telepresenceio/telepresence/rpc/v2/authenticator"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/dos"
)

const (
	devboxProxyProduction = "22222222-2222-4222-8222-222222222222"
	devboxProxyStaging    = "33333333-3333-4333-8333-333333333333"
	devboxIdentityTenant  = "11111111-1111-4111-8111-111111111111"
	devboxTestNativeAKS   = "44444444-4444-4444-8444-444444444444"
	devboxTestClient      = "03023879-d5e7-4def-9e9f-64c0dfbf42bd"
	devboxTestPodA        = "55783799-1be0-4cff-bfa7-01e8e8f10505"
	devboxTestPodB        = "a4c6d38c-d356-4f2e-a3db-5baf9ad68523"
)

func devboxTestPolicy() *devboxManagerPolicy {
	return &devboxManagerPolicy{Version: 1, TenantID: devboxIdentityTenant, ManagerNamespace: "ambassador", ManagerServiceAccount: "traffic-manager", ProxyAudiences: map[string]string{
		"devbox-proxy.production-0.example.test": devboxProxyProduction,
		"devbox-proxy.production-1.example.test": devboxProxyProduction,
		"devbox-proxy.staging-0.example.test":    devboxProxyStaging,
	}, ClusterAudiences: map[string]string{"u3s": devboxProxyStaging, "staging-1": devboxProxyStaging, "production-1": devboxProxyProduction}}
}

func devboxTestKubeconfig(rc *rest.Config) *Kubeconfig {
	return &Kubeconfig{RestConfig: rc, devboxManagerPolicyLoader: devboxTestPolicy, devboxManagerPodVerifier: func(context.Context, string, string, string) error { return nil }}
}

func devboxTestTarget(pod string) devboxManagerTarget {
	return devboxManagerTarget{pod: pod, namespace: "ambassador", serviceAccount: "traffic-manager"}
}

func devboxTestRest(host, audience string) *rest.Config {
	return &rest.Config{
		Host: "https://devbox-proxy." + host + ".example.test/clusters/u3s",
		ExecProvider: &clientcmdapi.ExecConfig{
			Command: "kubelogin", APIVersion: "client.authentication.k8s.io/v1beta1", InteractiveMode: clientcmdapi.NeverExecInteractiveMode,
			Args: []string{
				"get-token", "--login", "workloadidentity", "--client-id", devboxTestClient,
				"--tenant-id", devboxIdentityTenant, "--federated-token-file", devboxIdentityFile, "--server-id", "api://" + audience,
			},
			Env: []clientcmdapi.ExecEnvVar{{Name: "AZURE_AUTHORITY_HOST", Value: devboxAuthority}},
		},
	}
}

func TestDevboxNegotiationOnlyAcceptsOfficialKubeconfig(t *testing.T) {
	for _, tc := range []struct{ host, audience string }{
		{"production-0", devboxProxyProduction}, {"production-1", devboxProxyProduction}, {"staging-0", devboxProxyStaging},
	} {
		rc := devboxTestRest(tc.host, tc.audience)
		original := rc.ExecProvider.DeepCopy()
		for _, command := range []string{"kubelogin", "/usr/bin/kubelogin", "/usr/local/bin/kubelogin"} {
			rc.ExecProvider.Command, original.Command = command, command
			clone := devboxTestPolicy().officialProxyExec(rc)
			require.Equal(t, original, clone)
			clone.Args[4], clone.Env[0].Value = "changed", "changed"
			require.Equal(t, original, rc.ExecProvider)
		}
	}
	cases := map[string]func(*rest.Config){
		"wrong origin":    func(r *rest.Config) { r.Host = "https://attacker.invalid/clusters/u3s" },
		"origin suffix":   func(r *rest.Config) { r.Host = strings.Replace(r.Host, ".test/", ".test.attacker.invalid/", 1) },
		"nonsecure":       func(r *rest.Config) { r.Host = strings.Replace(r.Host, "https:", "http:", 1) },
		"userinfo":        func(r *rest.Config) { r.Host = strings.Replace(r.Host, "https://", "https://user@", 1) },
		"alternate port":  func(r *rest.Config) { r.Host = strings.Replace(r.Host, ".test/", ".test:8443/", 1) },
		"trailing slash":  func(r *rest.Config) { r.Host += "/" },
		"query":           func(r *rest.Config) { r.Host += "?" },
		"fragment":        func(r *rest.Config) { r.Host += "#" },
		"escaped cluster": func(r *rest.Config) { r.Host = strings.Replace(r.Host, "u3s", "%753s", 1) },
		"bad cluster":     func(r *rest.Config) { r.Host += "/../u3s" },
		"wrong pair":      func(r *rest.Config) { r.ExecProvider.Args[10] = "api://" + devboxProxyStaging },
		"native AKS":      func(r *rest.Config) { r.ExecProvider.Args[10] = devboxTestNativeAKS },
		"arbitrary executable": func(r *rest.Config) {
			r.ExecProvider.Command = "/tmp/kubelogin"
		},
		"Azure CLI": func(r *rest.Config) { r.ExecProvider.Args[2] = "azurecli" },
		"extra arguments": func(r *rest.Config) {
			r.ExecProvider.Args = append(r.ExecProvider.Args, "--server-id", devboxProxyStaging)
		},
		"wrong tenant":    func(r *rest.Config) { r.ExecProvider.Args[6] = devboxTestClient },
		"wrong source":    func(r *rest.Config) { r.ExecProvider.Args[8] = "/tmp/other-identity" },
		"wrong authority": func(r *rest.Config) { r.ExecProvider.Env[0].Value = "https://attacker.invalid/" },
		"missing authority": func(r *rest.Config) {
			r.ExecProvider.Env = nil
		},
		"insecure TLS":    func(r *rest.Config) { r.Insecure = true },
		"TLS server name": func(r *rest.Config) { r.ServerName = "attacker.invalid" },
		"TLS CA data":     func(r *rest.Config) { r.CAData = []byte("custom") },
		"TLS CA file":     func(r *rest.Config) { r.CAFile = "/tmp/ca" },
		"custom proxy":    func(r *rest.Config) { r.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil } },
		"custom transport": func(r *rest.Config) {
			r.WrapTransport = func(t http.RoundTripper) http.RoundTripper { return t }
		},
		"static token": func(r *rest.Config) { r.BearerToken = "static" },
		"static file":  func(r *rest.Config) { r.BearerTokenFile = "/tmp/static" },
		"basic authentication": func(r *rest.Config) {
			r.Username, r.Password = "username", "synthetic-password"
		},
		"extra client certificate": func(r *rest.Config) { r.CertFile = "/tmp/cert" },
		"extra private key":        func(r *rest.Config) { r.KeyData = []byte("synthetic-private-key") },
		"impersonation":            func(r *rest.Config) { r.Impersonate.UserName = "someone" },
		"extra impersonation":      func(r *rest.Config) { r.Impersonate.Extra = map[string][]string{"test": {"synthetic"}} },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			rc := devboxTestRest("production-0", devboxProxyProduction)
			change(rc)
			require.Nil(t, devboxTestPolicy().officialProxyExec(rc))
			kc := devboxTestKubeconfig(rc)
			require.Nil(t, kc.NegotiatedDevboxManagerTokenProvider("ambassador"))
			if usesDevboxWorkloadIdentity(rc) {
				auth, err := connectionManagerAuthenticationForVersion(t.Context(), kc, devboxTestTarget(devboxTestPodA), devboxProxyStaging)
				require.Error(t, err)
				require.Nil(t, auth.credentials)
			}
		})
	}
}

type devboxNegotiationManager struct {
	rpc.UnimplementedManagerServer
	audience string
	seen     chan string
}

func (s *devboxNegotiationManager) Version(ctx context.Context, _ *emptypb.Empty) (*rpc.VersionInfo2, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.seen <- "Version:" + strings.Join(md.Get("authorization"), ",")
	return &rpc.VersionInfo2{AuthDevboxProxyAudience: s.audience}, nil
}

func (s *devboxNegotiationManager) ArriveAsClient(ctx context.Context, _ *rpc.ClientInfo) (*rpc.SessionInfo, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.seen <- "Arrive:" + strings.Join(md.Get("authorization"), ",")
	return &rpc.SessionInfo{SessionId: "synthetic"}, nil
}

func dialDevboxNegotiationManager(t *testing.T, server *devboxNegotiationManager, gate *managerCredentialGate) rpc.ManagerClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	rpc.RegisterManagerServer(s, server)
	go func() { _ = s.Serve(listener) }()
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient("passthrough:///devbox-manager", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(gate))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return rpc.NewManagerClient(conn)
}

func TestDevboxNegotiationAuthenticatesOnlyAfterVersionAndRebindsReplacement(t *testing.T) {
	kc := devboxTestKubeconfig(devboxTestRest("production-0", devboxProxyProduction))
	original := kc.RestConfig.ExecProvider.DeepCopy()
	broker := kc.devboxManagerTokenBroker()
	var requested []string
	var mu sync.Mutex
	broker.makeSource = func(exec *clientcmdapi.ExecConfig) managerTokenSource {
		mu.Lock()
		defer mu.Unlock()
		requested = append(requested, exec.Args[10])
		return staticTokenSource(strings.TrimPrefix(exec.Args[10], "api://"))
	}
	for _, tc := range []struct{ pod, audience string }{{devboxTestPodA, devboxProxyStaging}, {devboxTestPodB, devboxProxyStaging}} {
		before := len(requested)
		gate := &managerCredentialGate{}
		server := &devboxNegotiationManager{audience: tc.audience, seen: make(chan string, 2)}
		client := dialDevboxNegotiationManager(t, server, gate)
		version, err := client.Version(t.Context(), &emptypb.Empty{})
		require.NoError(t, err)
		require.Equal(t, "Version:", <-server.seen, "a replacement must never receive the previous audience before advertising its own")
		require.Len(t, requested, before)
		auth, err := connectionManagerAuthenticationForVersion(t.Context(), kc, devboxTestTarget(tc.pod), version.GetAuthDevboxProxyAudience())
		require.NoError(t, err)
		require.True(t, auth.credentials.failClosed)
		gate.activate(auth.credentials)
		_, err = client.ArriveAsClient(t.Context(), &rpc.ClientInfo{})
		require.NoError(t, err)
		require.Equal(t, "Arrive:Bearer "+tc.audience, <-server.seen)
		require.Equal(t, "api://"+tc.audience, requested[len(requested)-1])
	}
	require.Equal(t, original, kc.RestConfig.ExecProvider, "ordinary Kubernetes and portforward credentials are never replaced")
	root := kc.NegotiatedDevboxManagerTokenProvider("ambassador")
	token, err := root(t.Context(), devboxTestPodB, devboxProxyStaging)
	require.NoError(t, err)
	require.Equal(t, devboxProxyStaging, token)
	require.Len(t, requested, 2, "user and root share the same source for the same pod and audience")
	_, err = root(t.Context(), devboxTestPodB, devboxProxyProduction)
	require.Error(t, err)
	require.Len(t, requested, 2, "a different cluster audience never obtains or reuses a credential")
	for _, audience := range []string{"unknown", devboxProxyProduction, "api://" + devboxProxyStaging, devboxTestNativeAKS} {
		gate := &managerCredentialGate{}
		auth, err := connectionManagerAuthenticationForVersion(t.Context(), kc, devboxTestTarget(devboxTestPodA), audience)
		require.Error(t, err)
		require.Nil(t, auth.credentials)
		md, err := gate.GetRequestMetadata(t.Context())
		require.NoError(t, err)
		require.Empty(t, md)
	}
	require.Len(t, requested, 2)
	_, err = root(t.Context(), "other", devboxProxyStaging)
	require.Error(t, err)
}

func TestDevboxNegotiationOldManagerAndExplicitCredentialsRetainCompatibility(t *testing.T) {
	kc := devboxTestKubeconfig(devboxTestRest("production-0", devboxProxyProduction))
	broker := kc.devboxManagerTokenBroker()
	var issued atomic.Int32
	broker.makeSource = func(*clientcmdapi.ExecConfig) managerTokenSource {
		issued.Add(1)
		return staticTokenSource("alternate")
	}
	auth, err := connectionManagerAuthenticationForVersion(t.Context(), kc, devboxTestTarget(devboxTestPodA), "")
	require.NoError(t, err)
	legacy, ok := auth.credentials.source.(*managerAuthTokenSource)
	require.True(t, ok)
	ordinary, ok := legacy.bearer.(*execTokenSource)
	require.True(t, ok)
	require.Equal(t, "api://"+devboxProxyProduction, ordinary.execConfig.Args[10])
	require.Zero(t, issued.Load())
	kc.ManagerTokenFileSet = true
	kc.ManagerTokenFile = filepath.Join(t.TempDir(), "explicit")
	require.NoError(t, os.WriteFile(kc.ManagerTokenFile, []byte("explicit-token"), 0o600))
	auth, err = connectionManagerAuthenticationForVersion(t.Context(), kc, devboxTestTarget(devboxTestPodA), devboxProxyStaging)
	require.NoError(t, err)
	md, err := auth.credentials.GetRequestMetadata(t.Context())
	require.NoError(t, err)
	require.Equal(t, "Bearer explicit-token", md["authorization"])
	require.Zero(t, issued.Load())
	other := devboxTestKubeconfig(&rest.Config{BearerToken: "workstation-token"})
	auth, err = connectionManagerAuthenticationForVersion(t.Context(), other, devboxTestTarget(devboxTestPodA), devboxProxyStaging)
	require.NoError(t, err)
	md, err = auth.credentials.GetRequestMetadata(t.Context())
	require.NoError(t, err)
	require.Equal(t, "Bearer workstation-token", md["authorization"])
}

type devboxNegotiationCallback struct {
	authrpc.UnimplementedAuthenticatorServer
	seen chan devboxManagerTokenKey
}

func (s *devboxNegotiationCallback) GetManagerToken(_ context.Context, req *authrpc.GetManagerTokenRequest) (*authrpc.GetManagerTokenResponse, error) {
	s.seen <- devboxManagerTokenKey{pod: req.GetManagerPodUid(), audience: req.GetDevboxProxyAudience()}
	return &authrpc.GetManagerTokenResponse{Token: "root-" + req.GetDevboxProxyAudience()}, nil
}

func TestDevboxNegotiationRootWaitsForCapabilityAndSendsExactPinnedScope(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	service := &devboxNegotiationCallback{seen: make(chan devboxManagerTokenKey, 6)}
	server := grpc.NewServer()
	authrpc.RegisterAuthenticatorServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	kc := devboxTestKubeconfig(&rest.Config{Host: devboxTestRest("production-0", devboxProxyProduction).Host, BearerToken: "original-kubeauth"})
	require.NoError(t, kc.ConfigureRootManagerTokenCallback(t.Context(), true, listener.Addr().String(), bytes.Repeat([]byte{'c'}, 32), true))
	auth, err := connectionManagerAuthenticationForVersion(t.Context(), kc, devboxTestTarget(devboxTestPodA), "")
	require.NoError(t, err)
	md, err := auth.credentials.GetRequestMetadata(t.Context())
	require.NoError(t, err)
	require.Equal(t, "Bearer original-kubeauth", md["authorization"])
	select {
	case <-service.seen:
		t.Fatal("legacy Version must not trigger the callback")
	default:
	}
	for _, tc := range []devboxManagerTokenKey{{devboxTestPodA, devboxProxyStaging}, {devboxTestPodB, devboxProxyStaging}} {
		auth, err = connectionManagerAuthenticationForVersion(t.Context(), kc, devboxTestTarget(tc.pod), tc.audience)
		require.NoError(t, err)
		require.Equal(t, tc, <-service.seen)
		md, err = auth.credentials.GetRequestMetadata(t.Context())
		require.NoError(t, err)
		require.Equal(t, "Bearer root-"+tc.audience, md["authorization"])
		require.Equal(t, tc, <-service.seen)
	}
	server.Stop()
	_, err = connectionManagerAuthenticationForVersion(t.Context(), kc, devboxTestTarget(devboxTestPodA), devboxProxyStaging)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "root-")
}

func TestDevboxNegotiationRotatesCredentialAndRedactsErrors(t *testing.T) {
	counter := filepath.Join(t.TempDir(), "counter")
	exec := newExecTokenSource(execHelperConfig(map[string]string{
		"EXEC_HELPER_TOKEN": "first-synthetic", "EXEC_HELPER_EXPIRY": time.Now().Add(time.Hour).UTC().Format(time.RFC3339), "EXEC_HELPER_COUNTER": counter,
	}))
	exec.requireExpiry = true
	safe := &safeDevboxTokenSource{source: exec}
	for range 2 {
		token, err := safe.Token(t.Context())
		require.NoError(t, err)
		require.Equal(t, "first-synthetic", token)
	}
	exec.mu.Lock()
	exec.expiry = time.Now().Add(-time.Second)
	for i := range exec.execConfig.Env {
		if exec.execConfig.Env[i].Name == "EXEC_HELPER_TOKEN" {
			exec.execConfig.Env[i].Value = "rotated-synthetic"
		}
	}
	exec.mu.Unlock()
	token, err := safe.Token(t.Context())
	require.NoError(t, err)
	require.Equal(t, "rotated-synthetic", token)
	count, err := os.ReadFile(counter)
	require.NoError(t, err)
	require.Equal(t, "2\n", string(count))
	const sensitive = "credential-marker-must-not-appear"
	safe.source = staticTokenSourceFunc(func(ctx context.Context) (string, error) {
		_, _ = fmt.Fprint(dos.Stderr(ctx), sensitive)
		return "", errors.New(sensitive)
	})
	var output bytes.Buffer
	token, err = safe.Token(dos.WithStderr(t.Context(), &output))
	require.Empty(t, token)
	require.Error(t, err)
	require.NotContains(t, err.Error(), sensitive)
	require.Empty(t, output.String())
	md, err := newRequiredManagerTokenCredentials(safe).GetRequestMetadata(t.Context())
	require.Nil(t, md)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.NotContains(t, err.Error(), sensitive)
}
