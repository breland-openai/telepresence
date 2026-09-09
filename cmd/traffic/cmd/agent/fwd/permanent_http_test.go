package fwd

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	agenttls "github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/agent/tls"
	"github.com/telepresenceio/telepresence/v2/pkg/forwarder"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

type mediatedTestTLS struct{ agenttls.Manager }

func (mediatedTestTLS) UseTLS(context.Context, uint16) bool   { return false }
func (mediatedTestTLS) UseHTTP2(context.Context, uint16) bool { return true }

type secureMediatedTestTLS struct {
	agenttls.Manager
	certificate *tls.Certificate
}

type mediatedHTTPResponse struct {
	StatusCode int
	Header     http.Header
	Body       string
}

func (secureMediatedTestTLS) UseTLS(context.Context, uint16) bool   { return true }
func (secureMediatedTestTLS) UseHTTP2(context.Context, uint16) bool { return false }
func (s secureMediatedTestTLS) GetDownstreamCertificate(uint16) *tls.Certificate {
	return s.certificate
}

func (secureMediatedTestTLS) GetUpstreamCertificate(uint16) (*tls.Certificate, bool) {
	return nil, true // the local test servers use generated self-signed certificates
}

func mediatedBackend(t *testing.T, name string, h2c bool) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	calls := new(atomic.Int32)
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		if strings.EqualFold(req.Header.Get("Upgrade"), "websocket") {
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			_, _ = fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
			_ = rw.Flush()
			message, err := rw.ReadString('\n')
			if err == nil {
				_, _ = fmt.Fprintf(rw, "%s:%s", name, message)
				_ = rw.Flush()
			}
			return
		}
		_, _ = fmt.Fprintf(w, "%s:%s", name, req.Header.Get(localRoutingKeyHeader))
	}))
	if h2c {
		s.Config.Protocols = new(http.Protocols)
		s.Config.Protocols.SetHTTP1(true)
		s.Config.Protocols.SetUnencryptedHTTP2(true)
	}
	s.Start()
	t.Cleanup(s.Close)
	return s, calls
}

func startMediatedForwarder(t *testing.T, backend *httptest.Server, h2c bool) (*tcp, netip.AddrPort) {
	t.Helper()
	var tm agenttls.Manager
	if h2c {
		tm = mediatedTestTLS{}
	}
	return startMediatedForwarderWithTLS(t, backend, tm)
}

func startMediatedForwarderWithTLS(t *testing.T, backend *httptest.Server, tm agenttls.Manager) (*tcp, netip.AddrPort) {
	t.Helper()
	return startMediatedForwarderState(t, backend, tm, true)
}

func startMediatedForwarderState(t *testing.T, backend *httptest.Server, tm agenttls.Manager, synchronized bool) (*tcp, netip.AddrPort) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	f := NewHTTPMediatedTCPInterceptor(ctx, types.PortAndProto{Proto: types.ProtoTCP}, tunnel.AgentToClient, tm,
		backend.Listener.Addr().(*net.TCPAddr).AddrPort(), forwarder.WithListenAddr(netip.MustParseAddr("127.0.0.1"))).(*tcp)
	f.SetRouteGuards(nil, nil)
	if synchronized {
		f.MarkRouteSnapshotInstalled()
	}
	f.SetStreamProvider(&fakeStreamProvider{createFor: func(ctx context.Context, id tunnel.ConnID) (tunnel.Stream, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", id.Destination().String())
		if err != nil {
			return nil, err
		}
		return &httpSocketStream{conn: conn, id: id}, nil
	}})
	ready := make(chan netip.AddrPort)
	done := make(chan error, 1)
	go func() { done <- f.Serve(ctx, ready) }()
	var addr netip.AddrPort
	select {
	case addr = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("agent forwarder did not start")
	}
	t.Cleanup(func() {
		f.SetIntercepting(nil)
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("agent forwarder did not stop")
		}
	})
	return f, addr
}

func TestPermanentMediationBeforeInitialSnapshotFailsEveryKeyOnExistingHTTP1Connection(t *testing.T) {
	staging, stagingCalls := mediatedBackend(t, "staging", false)
	local, _ := mediatedBackend(t, "local", false)
	f, address := startMediatedForwarderState(t, staging, nil, false)
	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	reader := bufio.NewReader(conn)
	request := func(key string, upgrade bool) mediatedHTTPResponse {
		return mediatedH1Request(t, conn, reader, key, upgrade)
	}
	guard := RouteGuard{RoutingKey: "developer", InterceptID: "client:web", Incarnation: "current"}
	mediatedResponse(t, request("", false), 200, "staging:")
	mediatedResponse(t, request(guard.RoutingKey, false), 503, "temporarily unavailable")
	mediatedResponse(t, request("another-developer", false), 503, "temporarily unavailable")
	mediatedResponse(t, request("another-developer", true), 503, "temporarily unavailable")
	f.SetPendingRouteGuards([]RouteGuard{guard})
	f.SetIntercepting([]*manager.InterceptInfo{mediatedRuntime(local, guard, false)})
	mediatedResponse(t, request(guard.RoutingKey, false), 503, "temporarily unavailable")
	mediatedResponse(t, request("", false), 200, "staging:")
	require.EqualValues(t, 2, stagingCalls.Load())
	f.SetRouteGuards([]RouteGuard{guard}, nil)
	mediatedResponse(t, request("another-developer", false), 503, "temporarily unavailable")
	f.MarkRouteSnapshotInstalled()
	mediatedResponse(t, request("another-developer", false), 200, "staging:another-developer")
	mediatedResponse(t, request(guard.RoutingKey, false), 200, "local:developer")
	f.SetIntercepting(nil) // an established agent never goes back to the global startup rule
	mediatedResponse(t, request("another-developer", false), 200, "staging:another-developer")
	mediatedResponse(t, request(guard.RoutingKey, false), 503, "temporarily unavailable")
	require.EqualValues(t, 4, stagingCalls.Load())
}

func mediatedRuntime(backend *httptest.Server, guard RouteGuard, global bool) *manager.InterceptInfo {
	addr := backend.Listener.Addr().(*net.TCPAddr).AddrPort()
	runtime := &manager.InterceptInfo{
		Id: guard.InterceptID, RouteIncarnation: guard.Incarnation,
		ClientSession: &manager.SessionInfo{SessionId: "session"}, Spec: &manager.InterceptSpec{TargetHost: addr.Addr().String(), TargetPort: int32(addr.Port())},
	}
	if !global {
		runtime.Spec.HeaderFilters = map[string]string{localRoutingKeyHeader: guard.RoutingKey}
	}
	return runtime
}

func mediatedH1Request(t *testing.T, conn net.Conn, reader *bufio.Reader, key string, upgrade bool) mediatedHTTPResponse {
	t.Helper()
	var headers string
	if key != "" {
		headers += localRoutingKeyHeader + ": " + key + "\r\n"
	}
	if upgrade {
		headers += "Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: eHl6\r\nSec-WebSocket-Version: 13\r\n"
	}
	return mediatedH1CustomRequest(t, conn, reader, "/hmr", headers)
}

func mediatedH1CustomRequest(t *testing.T, conn net.Conn, reader *bufio.Reader, target, headers string) mediatedHTTPResponse {
	t.Helper()
	_, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: application\r\n%s\r\n", target, headers)
	require.NoError(t, err)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	require.NoError(t, err)
	defer resp.Body.Close()
	result := mediatedHTTPResponse{StatusCode: resp.StatusCode, Header: resp.Header}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		result.Body = string(body)
	}
	return result
}

func mediatedResponse(t *testing.T, resp mediatedHTTPResponse, status int, contains string) {
	t.Helper()
	require.Equal(t, status, resp.StatusCode)
	if status == http.StatusServiceUnavailable {
		require.Equal(t, "1", resp.Header.Get("Retry-After"))
		require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	}
	require.Contains(t, resp.Body, contains)
}

func TestPermanentMediationReusesPreRouteHTTP1ConnectionAndWebSocket(t *testing.T) {
	staging, stagingCalls := mediatedBackend(t, "staging", false)
	local, _ := mediatedBackend(t, "local", false)
	f, address := startMediatedForwarder(t, staging, false)
	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
	reader := bufio.NewReader(conn)
	request := func(key string, upgrade bool) mediatedHTTPResponse {
		return mediatedH1Request(t, conn, reader, key, upgrade)
	}
	guard := RouteGuard{RoutingKey: "developer", InterceptID: "client:web", Incarnation: "current"}
	mediatedResponse(t, request(guard.RoutingKey, false), 200, "staging:developer")
	mediatedResponse(t, request("other", false), 200, "staging:other")
	f.SetPendingRouteGuards([]RouteGuard{guard})
	mediatedResponse(t, request(guard.RoutingKey, false), 503, "temporarily unavailable")
	mediatedResponse(t, request(guard.RoutingKey, true), 503, "temporarily unavailable")
	mediatedResponse(t, request("other", false), 200, "staging:other")
	f.SetRouteGuards([]RouteGuard{guard}, nil)
	f.SetIntercepting([]*manager.InterceptInfo{mediatedRuntime(local, guard, false)})
	mediatedResponse(t, request(guard.RoutingKey, false), 200, "local:developer")
	f.SetIntercepting(nil) // manager runtime watch disconnects after durable route was installed
	mediatedResponse(t, request(guard.RoutingKey, false), 503, "temporarily unavailable")
	mediatedResponse(t, request("", false), 200, "staging:")
	f.SetRouteGuards(nil, []RouteGuard{guard})
	mediatedResponse(t, request(guard.RoutingKey, false), 200, "staging:developer")
	guard.Incarnation = "recreated"
	f.SetRouteGuards([]RouteGuard{guard}, nil)
	f.SetIntercepting([]*manager.InterceptInfo{mediatedRuntime(local, guard, false)})
	upgrade := request(guard.RoutingKey, true)
	require.Equal(t, http.StatusSwitchingProtocols, upgrade.StatusCode)
	_, err = io.WriteString(conn, "after-policy-change\n")
	require.NoError(t, err)
	message, err := reader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "local:after-policy-change\n", message)
	require.EqualValues(t, 5, stagingCalls.Load(), "a pending or desired request must never reach staging on the previously opened TCP connection")
}

func TestPermanentMediationReusesPreRouteHTTP2Connection(t *testing.T) {
	staging, stagingCalls := mediatedBackend(t, "staging", true)
	local, _ := mediatedBackend(t, "local", true)
	f, address := startMediatedForwarder(t, staging, true)
	transport := &http.Transport{Protocols: new(http.Protocols)}
	transport.Protocols.SetUnencryptedHTTP2(true)
	var clientDials atomic.Int32
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		clientDials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	request := func(key string) mediatedHTTPResponse {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+address.String()+"/hmr", nil)
		require.NoError(t, err)
		if key != "" {
			req.Header.Set(localRoutingKeyHeader, key)
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 2, resp.ProtoMajor)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return mediatedHTTPResponse{StatusCode: resp.StatusCode, Header: resp.Header, Body: string(body)}
	}
	guard := RouteGuard{RoutingKey: "developer", InterceptID: "client:api", Incarnation: "current"}
	mediatedResponse(t, request(guard.RoutingKey), 200, "staging:developer")
	f.SetRouteGuards([]RouteGuard{guard}, nil)
	mediatedResponse(t, request(guard.RoutingKey), 503, "temporarily unavailable")
	mediatedResponse(t, request("other"), 200, "staging:other")
	f.SetIntercepting([]*manager.InterceptInfo{mediatedRuntime(local, guard, false)})
	mediatedResponse(t, request(guard.RoutingKey), 200, "local:developer")
	f.SetIntercepting(nil)
	mediatedResponse(t, request(guard.RoutingKey), 503, "temporarily unavailable")
	f.SetRouteGuards(nil, []RouteGuard{guard})
	mediatedResponse(t, request(guard.RoutingKey), 200, "staging:developer")
	require.EqualValues(t, 1, clientDials.Load(), "new H2 streams must obey current policy on the original connection")
	require.EqualValues(t, 3, stagingCalls.Load())
}

func TestPermanentMediationRetainsGlobalPortInterceptOnOpenConnection(t *testing.T) {
	staging, stagingCalls := mediatedBackend(t, "staging", false)
	local, _ := mediatedBackend(t, "local", false)
	f, address := startMediatedForwarder(t, staging, false)
	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	reader := bufio.NewReader(conn)
	request := func(key string) mediatedHTTPResponse { return mediatedH1Request(t, conn, reader, key, false) }
	mediatedResponse(t, request(""), 200, "staging:")
	global := RouteGuard{InterceptID: "client:global"}
	f.SetIntercepting([]*manager.InterceptInfo{mediatedRuntime(local, global, true)})
	mediatedResponse(t, request(""), 200, "local:")
	mediatedResponse(t, request("unknown"), 200, "local:unknown")
	guard := RouteGuard{RoutingKey: "developer", InterceptID: "client:key", Incarnation: "current"}
	f.SetRouteGuards([]RouteGuard{guard}, nil)
	mediatedResponse(t, request(guard.RoutingKey), 503, "temporarily unavailable")
	f.SetIntercepting(nil)
	mediatedResponse(t, request(""), 200, "staging:")
	require.EqualValues(t, 2, stagingCalls.Load())
}

func TestPermanentMediationPreservesTLSStagingSNIAndTLSLocalHTTP1(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1") // the real app socket must remain pinned despite virtual SNI
	staging := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Observed-Host", req.Host)
		w.Header().Set("Observed-Query", req.URL.RawQuery)
		for _, header := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
			w.Header().Set("Observed-"+header, req.Header.Get(header))
		}
		_, _ = fmt.Fprintf(w, "staging:%s:%s", req.TLS.ServerName, req.Header.Get(localRoutingKeyHeader))
	}))
	defer staging.Close()
	local := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.TLS == nil {
			t.Error("local was reached without TLS")
		}
		_, _ = fmt.Fprintf(w, "local:%s", req.Header.Get(localRoutingKeyHeader))
	}))
	defer local.Close()
	tm := secureMediatedTestTLS{certificate: &staging.TLS.Certificates[0]}
	f, address := startMediatedForwarderWithTLS(t, staging, tm)
	conn, err := tls.Dial("tcp", address.String(), &tls.Config{ServerName: "virtual.application.example", InsecureSkipVerify: true})
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	reader := bufio.NewReader(conn)
	request := func(key string) mediatedHTTPResponse { return mediatedH1Request(t, conn, reader, key, false) }
	forwarded := mediatedH1CustomRequest(t, conn, reader, "/hmr?values=a;b", "Forwarded: for=original\r\n"+
		"X-Forwarded-For: 198.51.100.22\r\nX-Forwarded-Host: original.example\r\nX-Forwarded-Proto: https\r\n")
	mediatedResponse(t, forwarded, 200, "staging:virtual.application.example:")
	require.Equal(t, "application", forwarded.Header.Get("Observed-Host"))
	require.Equal(t, "values=a;b", forwarded.Header.Get("Observed-Query"))
	require.Equal(t, "for=original", forwarded.Header.Get("Observed-Forwarded"))
	require.Equal(t, "198.51.100.22, 127.0.0.1", forwarded.Header.Get("Observed-X-Forwarded-For"))
	require.Equal(t, "original.example", forwarded.Header.Get("Observed-X-Forwarded-Host"))
	require.Equal(t, "https", forwarded.Header.Get("Observed-X-Forwarded-Proto"))
	hop := mediatedH1CustomRequest(t, conn, reader, "/hmr", "Forwarded: for=original\r\n"+
		"X-Forwarded-For: 198.51.100.22\r\nX-Forwarded-Host: original.example\r\nX-Forwarded-Proto: https\r\n"+
		"Connection: Forwarded, X-Forwarded-For, X-Forwarded-Host, X-Forwarded-Proto\r\n")
	mediatedResponse(t, hop, 200, "staging:virtual.application.example:")
	require.Empty(t, hop.Header.Get("Observed-Forwarded"))
	require.Equal(t, "127.0.0.1", hop.Header.Get("Observed-X-Forwarded-For"))
	require.Empty(t, hop.Header.Get("Observed-X-Forwarded-Host"))
	require.Empty(t, hop.Header.Get("Observed-X-Forwarded-Proto"))
	mediatedResponse(t, request("other"), 200, "staging:virtual.application.example:other")
	guard := RouteGuard{RoutingKey: "developer", InterceptID: "client:secure", Incarnation: "current"}
	f.SetRouteGuards([]RouteGuard{guard}, nil)
	mediatedResponse(t, request(guard.RoutingKey), 503, "temporarily unavailable")
	mediatedResponse(t, request("other"), 200, "staging:virtual.application.example:other")
	f.SetIntercepting([]*manager.InterceptInfo{mediatedRuntime(local, guard, false)})
	mediatedResponse(t, request(guard.RoutingKey), 200, "local:developer")
}

func TestRawPortKeepsOpaqueTCPAfterEmptyStrictRouteState(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer backend.Close()
	go func() {
		conn, err := backend.Accept()
		if err == nil {
			defer conn.Close()
			_, _ = io.Copy(conn, conn)
		}
	}()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f := NewTCPInterceptor(ctx, types.PortAndProto{Proto: types.ProtoTCP}, tunnel.AgentToClient, nil,
		backend.Addr().(*net.TCPAddr).AddrPort(), forwarder.WithListenAddr(netip.MustParseAddr("127.0.0.1"))).(*tcp)
	f.SetRouteGuards(nil, nil)
	require.False(t, f.SupportsHTTPRouteGuards())
	ready := make(chan netip.AddrPort)
	done := make(chan error, 1)
	go func() { done <- f.Serve(ctx, ready) }()
	var address netip.AddrPort
	select {
	case address = <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("raw forwarder did not start")
	}
	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
	want := []byte{0, 255, 0, 'n', 'o', 't', '-', 'h', 't', 't', 'p'}
	_, err = conn.Write(want)
	require.NoError(t, err)
	got := make([]byte, len(want))
	_, err = io.ReadFull(conn, got)
	require.NoError(t, err)
	require.Equal(t, want, got)
	_ = conn.Close()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("raw forwarder did not stop")
	}
}
