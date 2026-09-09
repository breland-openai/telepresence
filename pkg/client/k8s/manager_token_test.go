package k8s

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/telepresenceio/clog/testutil"
)

func TestNewManagerTokenSource_StaticBearerToken(t *testing.T) {
	ctx := testutil.NewContext(t, false)

	kc := &Kubeconfig{RestConfig: &rest.Config{BearerToken: "static-tok"}}
	src := newManagerTokenSource(kc)
	require.NotNil(t, src)

	md, err := newManagerTokenCredentials(src).GetRequestMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"authorization": "Bearer static-tok"}, md)
}

func TestNewManagerTokenSource_BearerTokenFile(t *testing.T) {
	ctx := testutil.NewContext(t, false)

	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("file-tok\n"), 0o600))

	kc := &Kubeconfig{RestConfig: &rest.Config{BearerTokenFile: path}}
	src := newManagerTokenSource(kc)
	require.NotNil(t, src)

	md, err := newManagerTokenCredentials(src).GetRequestMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"authorization": "Bearer file-tok"}, md)
}

func TestNewManagerTokenSource_CertOnly(t *testing.T) {
	kc := &Kubeconfig{RestConfig: &rest.Config{}}
	require.Nil(t, newManagerTokenSource(kc))
}

func TestNewManagerTokenSource_AuthProviderUnsupported(t *testing.T) {
	ctx := testutil.NewContext(t, false)
	kc := &Kubeconfig{
		Context:    ctx,
		RestConfig: &rest.Config{AuthProvider: &clientcmdapi.AuthProviderConfig{Name: "gcp"}},
	}
	require.Nil(t, newManagerTokenSource(kc))
}

const execHelperSentinel = "TELEPRESENCE_TEST_EXEC_HELPER"

// TestManagerTokenExecHelper is not a real test. When the sentinel env var is
// set it is re-executed as a fake exec credential plugin: it prints an
// ExecCredential JSON built from env vars, optionally bumping a counter file,
// and exits before the test runner emits anything else, so the captured stdout
// is exactly the JSON. Using the test binary itself keeps the fixture portable
// (a shell script is not executable on Windows).
func TestManagerTokenExecHelper(t *testing.T) {
	if os.Getenv(execHelperSentinel) != "1" {
		return
	}
	if cp := os.Getenv("EXEC_HELPER_COUNTER"); cp != "" {
		n := 0
		if b, err := os.ReadFile(cp); err == nil {
			_, _ = fmt.Sscanf(string(b), "%d", &n)
		}
		_ = os.WriteFile(cp, []byte(fmt.Sprintf("%d\n", n+1)), 0o600)
	}
	if os.Getenv("EXEC_HELPER_NO_TOKEN") == "1" {
		fmt.Println(`{"kind":"ExecCredential","apiVersion":"client.authentication.k8s.io/v1","status":{"clientCertificateData":"cert-data"}}`)
	} else {
		fmt.Printf(`{"kind":"ExecCredential","apiVersion":"client.authentication.k8s.io/v1","status":{"token":%q,"expirationTimestamp":%q}}`+"\n",
			os.Getenv("EXEC_HELPER_TOKEN"), os.Getenv("EXEC_HELPER_EXPIRY"))
	}
	os.Exit(0)
}

// execHelperConfig returns an ExecConfig that re-runs the test binary as the
// fake plugin in TestManagerTokenExecHelper, controlled by the given env vars.
func execHelperConfig(env map[string]string) *clientcmdapi.ExecConfig {
	ev := []clientcmdapi.ExecEnvVar{{Name: execHelperSentinel, Value: "1"}}
	for k, v := range env {
		ev = append(ev, clientcmdapi.ExecEnvVar{Name: k, Value: v})
	}
	return &clientcmdapi.ExecConfig{
		Command: os.Args[0],
		Args:    []string{"-test.run=^TestManagerTokenExecHelper$"},
		Env:     ev,
	}
}

func TestExecTokenSource_CachesUntilExpiry(t *testing.T) {
	ctx := testutil.NewContext(t, false)

	counterPath := filepath.Join(t.TempDir(), "count")
	expiry := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	src := newExecTokenSource(execHelperConfig(map[string]string{
		"EXEC_HELPER_TOKEN":   "future-token",
		"EXEC_HELPER_EXPIRY":  expiry,
		"EXEC_HELPER_COUNTER": counterPath,
	}))

	tok, err := src.Token(ctx)
	require.NoError(t, err)
	require.Equal(t, "future-token", tok)

	tok, err = src.Token(ctx)
	require.NoError(t, err)
	require.Equal(t, "future-token", tok)

	data, err := os.ReadFile(counterPath)
	require.NoError(t, err)
	require.Equal(t, "1\n", string(data), "plugin should only run once while the token is still valid")
}

func TestExecTokenSource_ExpiredTokenTriggersReExecution(t *testing.T) {
	ctx := testutil.NewContext(t, false)

	counterPath := filepath.Join(t.TempDir(), "count")
	expiry := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	src := newExecTokenSource(execHelperConfig(map[string]string{
		"EXEC_HELPER_TOKEN":   "expired-token",
		"EXEC_HELPER_EXPIRY":  expiry,
		"EXEC_HELPER_COUNTER": counterPath,
	}))

	_, err := src.Token(ctx)
	require.NoError(t, err)
	_, err = src.Token(ctx)
	require.NoError(t, err)

	data, err := os.ReadFile(counterPath)
	require.NoError(t, err)
	require.Equal(t, "2\n", string(data), "plugin should re-run once the cached token has expired")
}

func TestExecTokenSource_NoBearerToken(t *testing.T) {
	ctx := testutil.NewContext(t, false)

	src := newExecTokenSource(execHelperConfig(map[string]string{"EXEC_HELPER_NO_TOKEN": "1"}))
	tok, err := src.Token(ctx)
	require.Empty(t, tok)
	require.ErrorIs(t, err, errNoBearerToken)

	// The credentials wrapper must treat this as "no token available", not a
	// hard failure: empty metadata, nil error.
	md, err := newManagerTokenCredentials(src).GetRequestMetadata(ctx)
	require.NoError(t, err)
	require.Empty(t, md)
}

func TestManagerTokenCredentials_RequireTransportSecurity(t *testing.T) {
	creds := newManagerTokenCredentials(staticTokenSource("x"))
	require.False(t, creds.RequireTransportSecurity())
}

func TestExplicitManagerTokenFileTakesPrecedenceAndDoesNotChangeKubernetesAuth(t *testing.T) {
	ctx := testutil.NewContext(t, false)
	path := filepath.Join(t.TempDir(), "manager-token")
	require.NoError(t, os.WriteFile(path, []byte("manager.token\r\n"), 0o600))
	kc := &Kubeconfig{RestConfig: &rest.Config{BearerToken: "kubernetes-proxy-token"}, ManagerTokenFile: path, ManagerTokenFileSet: true}
	auth, err := connectionManagerAuthentication(ctx, kc)
	require.NoError(t, err)
	require.True(t, auth.hasBearer)
	require.Nil(t, auth.x509)
	metadata, err := auth.credentials.GetRequestMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"authorization": "Bearer manager.token"}, metadata)
	kubeToken, err := newManagerTokenSource(kc).Token(ctx)
	require.NoError(t, err)
	require.Equal(t, "kubernetes-proxy-token", kubeToken, "ordinary Kubernetes config remains unmodified")
	require.Equal(t, "kubernetes-proxy-token", kc.RestConfig.BearerToken)
	deleteResult, err := connectionManagerAuthentication(ctx, &Kubeconfig{RestConfig: kc.RestConfig})
	require.NoError(t, err)
	metadata, err = deleteResult.credentials.GetRequestMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"authorization": "Bearer kubernetes-proxy-token"}, metadata)
}

func TestExplicitManagerTokenFileRotatesAndFailsClosed(t *testing.T) {
	ctx := testutil.NewContext(t, false)
	dir := t.TempDir()
	path := filepath.Join(dir, "manager-token")
	require.NoError(t, os.WriteFile(path, []byte("first-token"), 0o600))
	kc := &Kubeconfig{RestConfig: &rest.Config{BearerToken: "fallback-secret"}, ManagerTokenFile: path, ManagerTokenFileSet: true}
	auth, err := connectionManagerAuthentication(ctx, kc)
	require.NoError(t, err)
	check := func(want string) {
		t.Helper()
		metadata, err := auth.credentials.GetRequestMetadata(ctx)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"authorization": "Bearer " + want}, metadata)
	}
	check("first-token")
	if runtime.GOOS == "windows" {
		require.NoError(t, os.WriteFile(path, []byte("second-token\n"), 0o600))
	} else {
		replacement := filepath.Join(dir, "replacement")
		require.NoError(t, os.WriteFile(replacement, []byte("second-token\n"), 0o600))
		require.NoError(t, os.Rename(replacement, path))
	}
	check("second-token")
	for _, invalid := range []string{"", "second-token\nsecret-other-line", "wrong token", "\x00do-not-log-this", "Bearer abc", "é"} {
		require.NoError(t, os.WriteFile(path, []byte(invalid), 0o600))
		metadata, err := auth.credentials.GetRequestMetadata(ctx)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.Nil(t, metadata)
		require.NotContains(t, err.Error(), "fallback-secret")
		require.NotContains(t, err.Error(), "do-not-log-this")
		_, err = connectionManagerAuthentication(ctx, kc)
		require.ErrorContains(t, err, ManagerTokenFileEnv)
	}
	require.NoError(t, os.Remove(path))
	metadata, err := auth.credentials.GetRequestMetadata(ctx)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Nil(t, metadata)
	require.NoError(t, os.WriteFile(path, []byte("recovered-token"), 0o600))
	check("recovered-token")
}

func TestExplicitManagerTokenFileRejectsBadPathsAndSupportsSymlinkRotation(t *testing.T) {
	ctx := testutil.NewContext(t, false)
	for _, path := range []string{"", "relative-file", t.TempDir(), filepath.Join(t.TempDir(), "missing")} {
		_, err := connectionManagerAuthentication(ctx, &Kubeconfig{RestConfig: &rest.Config{BearerToken: "do-not-fallback"}, ManagerTokenFile: path, ManagerTokenFileSet: true})
		require.ErrorContains(t, err, ManagerTokenFileEnv)
		require.NotContains(t, err.Error(), "do-not-fallback")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "oversized")
	require.NoError(t, os.WriteFile(path, []byte(strings.Repeat("a", maxManagerTokenFileSize+1)), 0o600))
	_, err := managerFileTokenSource(path).Token(ctx)
	require.ErrorContains(t, err, "too large")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = newRequiredManagerTokenCredentials(managerFileTokenSource(path)).GetRequestMetadata(canceled)
	require.Equal(t, codes.Canceled, status.Code(err))
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require extra privileges on Windows")
	}
	first := filepath.Join(dir, "first")
	second := filepath.Join(dir, "second")
	link := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(first, []byte("first"), 0o600))
	require.NoError(t, os.WriteFile(second, []byte("second"), 0o600))
	require.NoError(t, os.Symlink(first, link))
	source := managerFileTokenSource(link)
	token, err := source.Token(ctx)
	require.NoError(t, err)
	require.Equal(t, "first", token)
	newLink := filepath.Join(dir, "new-link")
	require.NoError(t, os.Symlink(second, newLink))
	require.NoError(t, os.Rename(newLink, link))
	token, err = source.Token(ctx)
	require.NoError(t, err)
	require.Equal(t, "second", token)
}

func TestExplicitManagerBearerSyntax(t *testing.T) {
	for _, valid := range []string{"abc", "eyJ.eyJ.signature", "AZaz09-._~+/=="} {
		require.True(t, validManagerBearerToken(valid), valid)
	}
	for _, invalid := range []string{"", "=", "=abc", "a=b", "abc\r", " abc", "abc ", "abc\n", "a:b", "ñ"} {
		require.False(t, validManagerBearerToken(invalid), invalid)
	}
}

func TestManagerTokenFileRequestIsPerConnection(t *testing.T) {
	first := &Kubeconfig{ManagerTokenFile: "/one", ManagerTokenFileSet: true}
	second := &Kubeconfig{}
	require.True(t, first.ManagerTokenFileMatchesRequest(nil))
	require.True(t, first.ManagerTokenFileMatchesRequest(map[string]string{ManagerTokenFileEnv: "/one"}))
	require.False(t, first.ManagerTokenFileMatchesRequest(map[string]string{ManagerTokenFileEnv: "/two"}))
	require.False(t, first.ManagerTokenFileMatchesRequest(map[string]string{ManagerTokenFileEnv: ""}))
	require.True(t, second.ManagerTokenFileMatchesRequest(nil))
	require.False(t, second.ManagerTokenFileMatchesRequest(map[string]string{ManagerTokenFileEnv: "/one"}))
	require.False(t, second.ManagerTokenFileMatchesRequest(map[string]string{ManagerTokenFileEnv: ""}))
}

func TestExplicitManagerTokenFilePreventsAnonymousGRPCAfterBadRotation(t *testing.T) {
	ctx := testutil.NewContext(t, false)
	path := filepath.Join(t.TempDir(), "manager-token")
	require.NoError(t, os.WriteFile(path, []byte("first-manager-token"), 0o600))
	auth, err := connectionManagerAuthentication(ctx, &Kubeconfig{RestConfig: &rest.Config{BearerToken: "kubernetes-fallback"}, ManagerTokenFile: path, ManagerTokenFileSet: true})
	require.NoError(t, err)
	listener := bufconn.Listen(1024 * 1024)
	var calls atomic.Int32
	seen := make(chan string, 2)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
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
	require.NoError(t, os.WriteFile(path, []byte("bad token\nwith-private-content"), 0o600))
	_, err = client.Check(ctx, &healthrpc.HealthCheckRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.NotContains(t, err.Error(), "private-content")
	require.EqualValues(t, 1, calls.Load(), "invalid explicit token must prevent the RPC, not deliver it anonymously or with kubeconfig credentials")
	require.NoError(t, os.WriteFile(path, []byte("second-manager-token"), 0o600))
	_, err = client.Check(ctx, &healthrpc.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, "Bearer second-manager-token", <-seen)
	require.EqualValues(t, 2, calls.Load())
}
