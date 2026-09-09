package k8s

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/transport"

	"github.com/telepresenceio/clog"
	authrpc "github.com/telepresenceio/telepresence/rpc/v2/authenticator"
	"github.com/telepresenceio/telepresence/v2/pkg/authenticator"
)

// managerTokenSource yields the bearer token the kubeconfig's credentials
// resolve to, for presentation to the traffic-manager.
type managerTokenSource interface {
	Token(ctx context.Context) (string, error)
}

// ManagerTokenFileEnv selects a bearer token used only for traffic-manager RPCs.
// The Kubernetes API connection always retains the kubeconfig's own credentials.
const ManagerTokenFileEnv = "TELEPRESENCE_MANAGER_TOKEN_FILE"

const maxManagerTokenFileSize = 16 * 1024

// ManagerTokenCallbackCapabilitySize is the entropy, in bytes, of a user-daemon
// capability that grants the root daemon access to one connection's credential.
const ManagerTokenCallbackCapabilitySize = 32

// errNoBearerToken indicates that the kubeconfig's credentials yield a client
// certificate rather than a bearer token.
var errNoBearerToken = errors.New("exec credential plugin did not yield a bearer token")

// newManagerTokenSource returns a source for kc's credentials, or nil when the
// kubeconfig cannot produce a bearer token (client-certificate-only credentials).
func newManagerTokenSource(kc *Kubeconfig) managerTokenSource {
	if kc == nil || kc.RestConfig == nil {
		return nil
	}
	rc := kc.RestConfig
	switch {
	case rc.BearerToken != "":
		return staticTokenSource(rc.BearerToken)
	case rc.BearerTokenFile != "":
		return &oauth2TokenSource{ts: transport.NewCachedFileTokenSource(rc.BearerTokenFile)}
	case rc.ExecProvider != nil:
		return newExecTokenSource(rc.ExecProvider)
	case rc.AuthProvider != nil:
		clog.Debugf(kc, "manager bearer token: auth-provider %q is not supported for kubeconfig credentials", rc.AuthProvider.Name)
		return nil
	default:
		return nil
	}
}

// managerFileTokenSource reopens the file for every RPC, so atomic token rotation
// is visible without reconnecting and a missing or bad replacement never reuses
// the previous credential. Kubernetes projected token symlinks are supported.
type managerFileTokenSource string

// ReadManagerTokenFile reads the explicit manager credential with the current
// process's privileges. The user daemon uses this to serve an authorized root
// daemon callback; the privileged daemon must not read caller-selected paths.
func ReadManagerTokenFile(ctx context.Context, path string) (string, error) {
	return managerFileTokenSource(path).Token(ctx)
}

func (s managerFileTokenSource) Token(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path := string(s)
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("%s must name an absolute path", ManagerTokenFileEnv)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s does not name a readable regular token file", ManagerTokenFileEnv)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("%s token file could not be opened", ManagerTokenFileEnv)
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", fmt.Errorf("%s does not name a readable regular token file", ManagerTokenFileEnv)
	}
	if info.Size() > maxManagerTokenFileSize {
		return "", fmt.Errorf("%s token file is too large", ManagerTokenFileEnv)
	}
	content, err := io.ReadAll(io.LimitReader(file, maxManagerTokenFileSize+1))
	if err != nil {
		return "", fmt.Errorf("%s token file could not be read", ManagerTokenFileEnv)
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	if len(content) > maxManagerTokenFileSize {
		return "", fmt.Errorf("%s token file is too large", ManagerTokenFileEnv)
	}
	token := string(content)
	if strings.HasSuffix(token, "\n") {
		token = strings.TrimSuffix(strings.TrimSuffix(token, "\n"), "\r")
	}
	if !validManagerBearerToken(token) {
		return "", fmt.Errorf("%s token file is empty or contains an invalid bearer token", ManagerTokenFileEnv)
	}
	return token, nil
}

func validManagerBearerToken(token string) bool {
	if token == "" {
		return false
	}
	padding := false
	for index := range len(token) {
		c := token[index]
		if c == '=' {
			if index == 0 {
				return false
			}
			padding = true
			continue
		}
		if padding || !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("-._~+/", rune(c))) {
			return false
		}
	}
	return true
}

// ConfigureRootManagerTokenCallback applies the connection input from the user
// daemon. Passing no callback explicitly clears any root-process environment
// override; a supplied callback is required and never falls back to kubeconfig.
func (kf *Kubeconfig) ConfigureRootManagerTokenCallback(ctx context.Context, supplied bool, address string, capability []byte, negotiated ...bool) error {
	kf.ManagerTokenFile, kf.ManagerTokenFileSet, kf.managerTokenCallback, kf.managerTokenCallbackNegotiated = "", false, nil, false
	if !supplied {
		return nil
	}
	addr, err := netip.ParseAddrPort(address)
	if err != nil || !addr.Addr().IsLoopback() || addr.Port() == 0 || len(capability) != ManagerTokenCallbackCapabilitySize {
		return errors.New("invalid traffic-manager credential callback")
	}
	conn, err := grpc.NewClient("passthrough:///"+addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return errors.New("unable to create traffic-manager credential callback")
	}
	context.AfterFunc(ctx, func() { _ = conn.Close() })
	kf.managerTokenCallback = &managerCallbackTokenSource{
		client: authrpc.NewAuthenticatorClient(conn), capability: append([]byte(nil), capability...),
	}
	kf.managerTokenCallbackNegotiated = len(negotiated) > 0 && negotiated[0]
	return nil
}

type managerCallbackTokenSource struct {
	client     authrpc.AuthenticatorClient
	capability []byte
	managerPod string
	audience   string
}

func (s *managerCallbackTokenSource) Token(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	response, err := s.client.GetManagerToken(ctx, &authrpc.GetManagerTokenRequest{Capability: s.capability, ManagerPodUid: s.managerPod, DevboxProxyAudience: s.audience})
	if err != nil || response == nil || len(response.Token) > maxManagerTokenFileSize || !validManagerBearerToken(response.Token) {
		return "", errors.New("the user daemon could not provide the traffic-manager bearer credential")
	}
	return response.Token, nil
}

// staticTokenSource is a fixed bearer token, e.g. from --token or an
// in-cluster service account.
type staticTokenSource string

func (s staticTokenSource) Token(context.Context) (string, error) {
	return string(s), nil
}

// oauth2TokenSource adapts an oauth2.TokenSource (e.g. a cached file token
// source) to managerTokenSource.
type oauth2TokenSource struct {
	ts oauth2.TokenSource
}

func (o *oauth2TokenSource) Token(context.Context) (string, error) {
	tok, err := o.ts.Token()
	if err != nil {
		return "", err
	}
	return tok.AccessToken, nil
}

const (
	// execTokenSafetyMargin is subtracted from a credential's expiration so the
	// token is refreshed slightly before it actually expires.
	execTokenSafetyMargin = time.Minute

	// execTokenDefaultTTL is used to cache a credential that carries no
	// expiration timestamp.
	execTokenDefaultTTL = 10 * time.Minute
)

// execTokenSource runs a kubeconfig exec credential plugin and caches the
// resulting bearer token until it is about to expire.
type execTokenSource struct {
	execConfig    *clientcmdapi.ExecConfig
	requireExpiry bool

	mu     sync.Mutex
	cached bool
	expiry time.Time
	token  string
	tokErr error
}

// newExecTokenSource returns a source that runs ec to obtain a bearer token.
// ec is copied so the source does not retain pointers into mutable RestConfig
// state.
func newExecTokenSource(ec *clientcmdapi.ExecConfig) *execTokenSource {
	cp := *ec
	cp.Args = append([]string(nil), ec.Args...)
	cp.Env = append([]clientcmdapi.ExecEnvVar(nil), ec.Env...)
	return &execTokenSource{execConfig: &cp}
}

// execCredentialStatus mirrors the status field shared by the
// client.authentication.k8s.io v1 and v1beta1 ExecCredential types.
type execCredentialStatus struct {
	Token               string       `json:"token"`
	ExpirationTimestamp *metav1.Time `json:"expirationTimestamp"`
}

func (e *execTokenSource) Token(ctx context.Context) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cached && time.Now().Before(e.expiry) {
		return e.token, e.tokErr
	}

	out, err := authenticator.ResolveExecConfig(ctx, e.execConfig)
	if err != nil {
		e.cached = false
		return "", err
	}
	var cred struct {
		Status execCredentialStatus `json:"status"`
	}
	if err := json.Unmarshal(out, &cred); err != nil {
		e.cached = false
		return "", fmt.Errorf("unable to parse exec credential: %w", err)
	}
	if e.requireExpiry && (cred.Status.ExpirationTimestamp == nil || !cred.Status.ExpirationTimestamp.After(time.Now())) {
		e.cached = false
		return "", errors.New("exec credential did not provide a usable expiry")
	}

	if cred.Status.ExpirationTimestamp != nil {
		e.expiry = cred.Status.ExpirationTimestamp.Add(-execTokenSafetyMargin)
	} else {
		e.expiry = time.Now().Add(execTokenDefaultTTL)
	}
	e.cached = true
	if cred.Status.Token == "" {
		e.token = ""
		e.tokErr = errNoBearerToken
	} else {
		e.token = cred.Status.Token
		e.tokErr = nil
	}
	return e.token, e.tokErr
}

// managerAuthTokenSource composes a bearer-token source with an x509
// handshake source. The bearer source is tried first; the x509 source is
// consulted when the bearer source is absent, yields no token, or yields
// errNoBearerToken (an exec plugin whose credential carries only a client
// certificate).
type managerAuthTokenSource struct {
	bearer managerTokenSource
	x509   managerTokenSource
}

func (c *managerAuthTokenSource) Token(ctx context.Context) (string, error) {
	if c.bearer != nil {
		token, err := c.bearer.Token(ctx)
		if err == nil && token != "" {
			return token, nil
		}
		if err != nil && !errors.Is(err, errNoBearerToken) {
			return "", err
		}
	}
	if c.x509 != nil {
		return c.x509.Token(ctx)
	}
	return "", nil
}

// managerTokenCredentials is a credentials.PerRPCCredentials that attaches the
// kubeconfig's bearer token to every RPC to the traffic-manager.
type managerTokenCredentials struct {
	source     managerTokenSource
	failClosed bool
	warnOnce   sync.Once
}

var _ credentials.PerRPCCredentials = (*managerTokenCredentials)(nil)

// newManagerTokenCredentials returns credentials backed by source.
func newManagerTokenCredentials(source managerTokenSource) *managerTokenCredentials {
	return &managerTokenCredentials{source: source}
}

func newRequiredManagerTokenCredentials(source managerTokenSource) *managerTokenCredentials {
	return &managerTokenCredentials{source: source, failClosed: true}
}

// GetRequestMetadata returns the bearer authorization header for the current
// token. A failure to obtain a token (a plugin error, or a plugin that only
// yields a client certificate) is logged once and yields empty metadata
// rather than an error, since the manager is permissive about missing
// tokens. Token contents are never logged.
func (c *managerTokenCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := c.source.Token(ctx)
	if c.failClosed && (err != nil || token == "") {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		if err == nil {
			err = fmt.Errorf("%s token file is empty", ManagerTokenFileEnv)
		}
		return nil, status.Errorf(codes.Unauthenticated, "cannot obtain explicitly configured traffic-manager credential: %v", err)
	}
	if err != nil {
		c.warnOnce.Do(func() {
			clog.Warnf(ctx, "unable to obtain a bearer token for the traffic-manager: %v", err)
		})
		return map[string]string{}, nil
	}
	if token == "" {
		return map[string]string{}, nil
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

// RequireTransportSecurity is false: the connection to the traffic-manager is
// a port-forwarded h2c socket.
func (c *managerTokenCredentials) RequireTransportSecurity() bool {
	return false
}
