package fwd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/matcher"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func TestEnsureGRPCTrailersHeaderForReverseProxy(t *testing.T) {
	backendTe := make(chan string, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		backendTe <- request.Header.Get("Te")
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()

	targetURL, err := url.Parse(backend.URL)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	frontend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		proxy.ServeHTTP(writer, ensureGRPCTrailersHeader(request))
	}))
	defer frontend.Close()

	request, err := http.NewRequest(http.MethodPost, frontend.URL, nil)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/grpc")

	response, err := frontend.Client().Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	select {
	case got := <-backendTe:
		assert.Equal(t, "trailers", got)
	case <-time.After(time.Second):
		t.Fatal("backend did not receive the proxied gRPC request")
	}
}

func TestEnsureGRPCTrailersHeaderOnlyAddsForGRPC(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		te          string
		wantTe      string
		wantClone   bool
	}{
		{
			name:        "gRPC media type with suffix and parameters",
			contentType: "application/grpc+proto; charset=utf-8",
			wantTe:      "trailers",
			wantClone:   true,
		},
		{
			name:        "gRPC request that already advertises trailers",
			contentType: "application/grpc",
			te:          "trailers",
			wantTe:      "trailers",
		},
		{
			name:        "gRPC-web request",
			contentType: "application/grpc-web+proto",
		},
		{
			name:        "non-gRPC request",
			contentType: "application/json",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodPost, "http://example.com", nil)
			require.NoError(t, err)
			request.Header.Set("Content-Type", test.contentType)
			if test.te != "" {
				request.Header.Set("Te", test.te)
			}
			originalHeader := request.Header.Clone()

			got := ensureGRPCTrailersHeader(request)

			assert.Equal(t, test.wantTe, got.Header.Get("Te"))
			assert.Equal(t, originalHeader, request.Header)
			if test.wantClone {
				assert.NotSame(t, request, got)
			} else {
				assert.Same(t, request, got)
			}
		})
	}
}

func TestHTTPInterceptor_shouldInterceptRequest(t *testing.T) {
	headerFilters := map[string]string{
		"X-User-ID":     "dev123",
		"X-Environment": "staging",
	}
	pathFilters := []string{":path-prefix:/api/v1/", ":path-prefix:/admin/"}

	tests := []struct {
		name            string
		headers         map[string]string
		path            string
		shouldIntercept bool
	}{
		{
			name: "matching headers and path",
			headers: map[string]string{
				"X-User-ID":     "dev123",
				"X-Environment": "staging",
			},
			path:            "/api/v1/users",
			shouldIntercept: true,
		},
		{
			name: "matching headers but no path filters",
			headers: map[string]string{
				"X-User-ID":     "dev123",
				"X-Environment": "staging",
			},
			path:            "/some/other/path",
			shouldIntercept: false,
		},
		{
			name: "missing required header",
			headers: map[string]string{
				"X-User-ID": "dev123",
				// Missing X-Environment
			},
			path:            "/api/v1/users",
			shouldIntercept: false,
		},
		{
			name: "wrong header value",
			headers: map[string]string{
				"X-User-ID":     "prod456",
				"X-Environment": "staging",
			},
			path:            "/api/v1/users",
			shouldIntercept: false,
		},
		{
			name: "wildcard header match",
			headers: map[string]string{
				"X-User-ID":     "dev456",
				"X-Environment": "staging",
			},
			path:            "/api/v1/users",
			shouldIntercept: false, // Exact match required by default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, "http://example.com"+tt.path, nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			result := shouldInterceptRequest(req, headerFilters, pathFilters)
			assert.Equal(t, tt.shouldIntercept, result)
		})
	}
}

func TestHTTPInterceptor_matchesPattern(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		pattern string
		matches bool
	}{
		{"exact match", "dev123", "dev123", true},
		{"no match", "dev123", "prod456", false},
		{"wildcard match", "dev123", "dev.*", true},
		{"wildcard no match", "prod123", "dev.*", false},
		{"complex wildcard", "dev-user-123", "dev-.*-123", true},
		{"empty value", "", "dev*", false},
		{"empty pattern", "dev123", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matcher.NewValue(tt.pattern).Matches(tt.value)
			assert.Equal(t, tt.matches, result)
		})
	}
}

func TestHTTPInterceptor_noFilters(t *testing.T) {
	headerFilters := map[string]string{}
	pathFilters := []string{}

	req, _ := http.NewRequest(http.MethodGet, "http://example.com/any/path", nil)

	// No filters means intercept everything
	result := shouldInterceptRequest(req, headerFilters, pathFilters)
	assert.True(t, result)
}

func TestObservedResponseWriterTracksStatusAndBytes(t *testing.T) {
	rec := httptest.NewRecorder()
	writer := &observedResponseWriter{ResponseWriter: rec}

	writer.WriteHeader(http.StatusCreated)
	n, err := writer.Write([]byte("hello"))

	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, http.StatusCreated, writer.statusCode)
	require.EqualValues(t, 5, writer.bytes)
	require.Same(t, rec, writer.Unwrap())
}

func TestHandleHTTPRequest_SelectedInterceptTunnelUnavailable(t *testing.T) {
	const routingHeader = "x-local-routing-key"
	for _, tt := range []struct {
		name       string
		method     string
		path       string
		routingKey string
		websocket  bool
		selected   bool
	}{
		{name: "selected header", method: http.MethodGet, path: "/", routingKey: "developer", selected: true},
		{name: "selected post", method: http.MethodPost, path: "/submit", routingKey: "developer", selected: true},
		{name: "selected websocket", method: http.MethodGet, path: "/hmr", routingKey: "developer", websocket: true, selected: true},
		{name: "selected path", method: http.MethodGet, path: "/path-only/test", selected: true},
		{name: "unkeyed traffic", method: http.MethodGet, path: "/"},
		{name: "unkeyed post", method: http.MethodPost, path: "/submit"},
		{name: "other routing key", method: http.MethodGet, path: "/", routingKey: "other-developer"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := NewTCPInterceptor(t.Context(), types.PortAndProto{Proto: types.ProtoTCP}, tunnel.AgentToClient, nil,
				netip.MustParseAddrPort("127.0.0.1:3000")).(*tcp)
			provider := &fakeStreamProvider{err: io.ErrClosedPipe}
			f.SetStreamProvider(provider)
			f.SetIntercepting([]*manager.InterceptInfo{
				{
					Id: "by-header", ClientSession: &manager.SessionInfo{SessionId: "session"},
					Spec: &manager.InterceptSpec{
						TargetHost: "127.0.0.1", TargetPort: 8080,
						HeaderFilters: map[string]string{routingHeader: "developer"},
					},
				},
				{
					Id: "by-path", ClientSession: &manager.SessionInfo{SessionId: "session"},
					Spec: &manager.InterceptSpec{
						TargetHost: "127.0.0.1", TargetPort: 8080,
						PathFilters: []string{":path-prefix:/path-only/"},
					},
				},
			})
			t.Cleanup(func() { f.SetIntercepting(nil) })

			var appCalls int
			var appBody string
			app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				appCalls++
				if !tt.selected {
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					appBody = string(body)
				}
				w.WriteHeader(http.StatusAccepted)
			})
			var reqBody io.Reader
			var wantAppBody string
			if tt.method == http.MethodPost {
				wantAppBody = "request-body"
				reqBody = strings.NewReader(wantAppBody)
			}
			req := httptest.NewRequest(tt.method, "http://web.example"+tt.path, reqBody)
			if tt.routingKey != "" {
				req.Header.Set(routingHeader, tt.routingKey)
			}
			if tt.websocket {
				req.Header.Set("Connection", "Upgrade")
				req.Header.Set("Upgrade", "websocket")
			}
			rec := httptest.NewRecorder()
			f.handleHTTPRequest(rec, req, app)
			if tt.selected {
				assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
				assert.Equal(t, "1", rec.Header().Get("Retry-After"))
				assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
				assert.JSONEq(t, `{"error":"telepresence intercept is temporarily unavailable"}`, rec.Body.String())
				assert.Zero(t, appCalls)
				assert.EqualValues(t, 1, provider.calls.Load())
			} else {
				assert.Equal(t, http.StatusAccepted, rec.Code)
				assert.Empty(t, rec.Header().Get("Retry-After"))
				assert.Equal(t, 1, appCalls)
				assert.Equal(t, wantAppBody, appBody)
				assert.Zero(t, provider.calls.Load())
			}
		})
	}
}

func TestHandleHTTPRequest_SelectedInterceptNonTunnelError(t *testing.T) {
	f := NewTCPInterceptor(t.Context(), types.PortAndProto{Proto: types.ProtoTCP}, tunnel.AgentToClient, nil,
		netip.MustParseAddrPort("127.0.0.1:3000")).(*tcp)
	provider := &fakeStreamProvider{err: io.ErrClosedPipe}
	f.SetStreamProvider(provider)
	f.SetIntercepting([]*manager.InterceptInfo{{
		Id: "invalid-target", ClientSession: &manager.SessionInfo{SessionId: "session"},
		Spec: &manager.InterceptSpec{
			TargetHost: "invalid", TargetPort: 8080, HeaderFilters: map[string]string{"x-local-routing-key": "developer"},
		},
	}})
	t.Cleanup(func() { f.SetIntercepting(nil) })

	var appCalls int
	app := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { appCalls++ })
	req := httptest.NewRequest(http.MethodGet, "http://web.example/", nil)
	req.Header.Set("x-local-routing-key", "developer")
	rec := httptest.NewRecorder()
	f.handleHTTPRequest(rec, req, app)
	assert.Equal(t, http.StatusBadGateway, rec.Code)
	assert.Empty(t, rec.Header().Get("Retry-After"))
	assert.Contains(t, rec.Body.String(), "failed to parse intercept target address")
	assert.Zero(t, appCalls)
	assert.Zero(t, provider.calls.Load())
}

func TestHandleHTTPRequest_SelectedInterceptBoundsBlockedTunnelSetup(t *testing.T) {
	f := NewTCPInterceptor(t.Context(), types.PortAndProto{Proto: types.ProtoTCP}, tunnel.AgentToClient, nil,
		netip.MustParseAddrPort("127.0.0.1:3000")).(*tcp)
	providerCanceled := make(chan struct{})
	f.SetStreamProvider(&fakeStreamProvider{create: func(ctx context.Context) (tunnel.Stream, error) {
		<-ctx.Done()
		close(providerCanceled)
		return nil, ctx.Err()
	}})
	f.SetIntercepting([]*manager.InterceptInfo{{
		Id: "selected", ClientSession: &manager.SessionInfo{SessionId: "session"},
		Spec: &manager.InterceptSpec{
			TargetHost: "127.0.0.1", TargetPort: 8080, HeaderFilters: map[string]string{"x-local-routing-key": "developer"},
		},
	}})
	t.Cleanup(func() { f.SetIntercepting(nil) })
	var appCalls atomic.Int32
	app := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { appCalls.Add(1) })
	req := httptest.NewRequest(http.MethodGet, "http://web.example/", nil)
	req.Header.Set("x-local-routing-key", "developer")
	rec := httptest.NewRecorder()
	start := time.Now()
	f.handleHTTPRequest(rec, req, app)
	assert.Less(t, time.Since(start), 3*httpInterceptConnectionTimeout)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	assert.Zero(t, appCalls.Load())
	select {
	case <-providerCanceled:
	case <-time.After(httpInterceptConnectionTimeout):
		t.Fatal("detached transport dial did not cancel its stream provider")
	}
}

type httpSetupStream struct {
	fakeTapStream
	setup  chan tunnel.Message
	closed chan struct{}
	once   sync.Once
}

func (s *httpSetupStream) Receive(ctx context.Context) (tunnel.Message, error) {
	select {
	case m := <-s.setup:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *httpSetupStream) CloseSend(context.Context) error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func TestHTTPInterceptDialIncludesDeveloperTCPAcknowledgmentInSetupBudget(t *testing.T) {
	const timeout = 50 * time.Millisecond
	stream := &httpSetupStream{setup: make(chan tunnel.Message, 1), closed: make(chan struct{})}
	got, _, _, err := dialHTTPInterceptStream(t.Context(), timeout, func(ctx context.Context) (tunnel.Stream, error) {
		return stream, awaitHTTPInterceptDial(ctx, stream)
	})
	require.Nil(t, got)
	require.ErrorIs(t, err, errHTTPInterceptConnectionTimeout)
	select {
	case <-stream.closed:
	case <-time.After(time.Second):
		t.Fatal("stream with a late developer TCP acknowledgment was not closed")
	}
	stream.setup <- tunnel.NewMessage(tunnel.DialOK, nil)

	for _, code := range []tunnel.MessageCode{tunnel.DialReject, tunnel.Normal} {
		rejected := &httpSetupStream{setup: make(chan tunnel.Message, 1)}
		rejected.setup <- tunnel.NewMessage(code, nil)
		assert.ErrorIs(t, awaitHTTPInterceptDial(t.Context(), rejected), errClientStream)
	}
	accepted := &httpSetupStream{setup: make(chan tunnel.Message, 1)}
	accepted.setup <- tunnel.NewMessage(tunnel.DialOK, nil)
	assert.NoError(t, awaitHTTPInterceptDial(t.Context(), accepted))
}

func TestHandleHTTPRequest_HTTPRetryGetsUniqueTunnelIDForSameIncomingSocket(t *testing.T) {
	f := NewTCPInterceptor(t.Context(), types.PortAndProto{Proto: types.ProtoTCP}, tunnel.AgentToClient, nil,
		netip.MustParseAddrPort("127.0.0.1:3000")).(*tcp)
	ids := make(chan tunnel.ConnID, 2)
	f.SetStreamProvider(&fakeStreamProvider{createFor: func(_ context.Context, id tunnel.ConnID) (tunnel.Stream, error) {
		ids <- id
		return nil, io.ErrClosedPipe
	}})
	f.SetIntercepting([]*manager.InterceptInfo{{
		Id: "selected", ClientSession: &manager.SessionInfo{SessionId: "session"},
		Spec: &manager.InterceptSpec{
			TargetHost: "127.0.0.1", TargetPort: 8080, HeaderFilters: map[string]string{"x-local-routing-key": "developer"},
		},
	}})
	t.Cleanup(func() { f.SetIntercepting(nil) })
	const original = "198.51.100.34:43210"
	for range 2 {
		req := httptest.NewRequest(http.MethodGet, "http://web.example/", nil)
		req.RemoteAddr = original
		req.Header.Set("x-local-routing-key", "developer")
		rec := httptest.NewRecorder()
		f.handleHTTPRequest(rec, req, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("selected request reached staging") }))
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		assert.Equal(t, original, req.RemoteAddr)
	}
	first, second := <-ids, <-ids
	assert.NotEqual(t, first, second, "a late response must not satisfy the next request on the same incoming socket")
	for _, id := range []tunnel.ConnID{first, second} {
		assert.Equal(t, netip.MustParseAddrPort("127.0.0.1:8080"), id.Destination())
		assert.Equal(t, types.ProtoTCP, id.Protocol())
		assert.EqualValues(t, 43210, id.Source().Port())
		assert.True(t, id.Source().Addr().IsPrivate())
	}
}

type httpSocketStream struct {
	fakeTapStream
	conn net.Conn
	id   tunnel.ConnID
	ack  bool
}

func (s *httpSocketStream) ID() tunnel.ConnID { return s.id }

func (s *httpSocketStream) Receive(context.Context) (tunnel.Message, error) {
	if !s.ack {
		s.ack = true
		return tunnel.NewMessage(tunnel.DialOK, nil), nil
	}
	b := make([]byte, 4096)
	n, err := s.conn.Read(b)
	if n > 0 {
		return tunnel.NewMessage(tunnel.Normal, b[:n]), nil
	}
	return nil, err
}

func (s *httpSocketStream) Send(_ context.Context, msg tunnel.Message) error {
	_, err := s.conn.Write(msg.Payload())
	return err
}

func (s *httpSocketStream) CloseSend(context.Context) error { return s.conn.Close() }

func TestHandleHTTPRequest_UniqueTunnelSourcePreservesOriginalHTTPForwardedAddress(t *testing.T) {
	forwarded := make(chan string, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		forwarded <- req.Header.Get("X-Forwarded-For")
		_, _ = io.WriteString(w, "local")
	}))
	defer backend.Close()
	backendAddr := backend.Listener.Addr().(*net.TCPAddr).AddrPort()
	f := NewTCPInterceptor(t.Context(), types.PortAndProto{Proto: types.ProtoTCP}, tunnel.AgentToClient, nil,
		netip.MustParseAddrPort("127.0.0.1:3000")).(*tcp)
	f.SetStreamProvider(&fakeStreamProvider{createFor: func(ctx context.Context, id tunnel.ConnID) (tunnel.Stream, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", id.Destination().String())
		if err != nil {
			return nil, err
		}
		return &httpSocketStream{conn: conn, id: id}, nil
	}})
	f.SetIntercepting([]*manager.InterceptInfo{{
		Id: "selected", ClientSession: &manager.SessionInfo{SessionId: "session"},
		Spec: &manager.InterceptSpec{
			TargetHost: backendAddr.Addr().String(), TargetPort: int32(backendAddr.Port()), HeaderFilters: map[string]string{"x-local-routing-key": "developer"},
		},
	}})
	t.Cleanup(func() { f.SetIntercepting(nil) })
	req := httptest.NewRequest(http.MethodGet, "http://web.example/", nil)
	req.RemoteAddr = "198.51.100.34:43210"
	req.Header.Set("x-local-routing-key", "developer")
	rec := httptest.NewRecorder()
	f.handleHTTPRequest(rec, req, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("selected request reached staging") }))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "local", rec.Body.String())
	assert.Equal(t, "198.51.100.34", <-forwarded)
	assert.Equal(t, "198.51.100.34:43210", req.RemoteAddr)
}

func TestHTTPConnectionAcquisitionBoundsPoolQueueButNotSlowResponse(t *testing.T) {
	const timeout = 75 * time.Millisecond
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		_, _ = io.WriteString(w, "local")
	}))
	defer backend.Close()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxConnsPerHost = 1
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: httpConnectionAcquisitionTransport{transport: tr, timeout: timeout}}
	firstDone := make(chan error, 1)
	go func() {
		resp, err := client.Get(backend.URL)
		if err == nil {
			_, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		firstDone <- err
	}()
	<-firstStarted
	started := time.Now()
	resp, err := client.Get(backend.URL)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Nil(t, resp)
	require.ErrorIs(t, err, errHTTPInterceptConnectionTimeout)
	assert.GreaterOrEqual(t, time.Since(started), timeout/2)
	assert.Less(t, time.Since(started), 2*time.Second)
	assert.EqualValues(t, 1, calls.Load(), "pooled request must not reach the local server")
	select {
	case err = <-firstDone:
		t.Errorf("healthy request ended before its slow response was ready: %v", err)
	default:
	}
	close(releaseFirst)
	require.NoError(t, <-firstDone)
	resp, err = client.Get(backend.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.EqualValues(t, 2, calls.Load(), "the healthy connection should remain usable")
}

func TestHTTPConnectionAcquisitionPreservesStreamingBody(t *testing.T) {
	const timeout = 50 * time.Millisecond
	releaseBody := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		<-releaseBody
		_, _ = io.WriteString(w, "data: local\n\n")
	}))
	defer backend.Close()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: httpConnectionAcquisitionTransport{transport: tr, timeout: timeout}}
	resp, err := client.Get(backend.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	<-time.After(3 * timeout)
	close(releaseBody)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "data: local\n\n", string(body))
}

type httpAcquisitionTestRoundTripper func(*http.Request) (*http.Response, error)

func (f httpAcquisitionTestRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestHTTPConnectionAcquisitionBoundsRetryAfterPreviouslyAcquiredConnection(t *testing.T) {
	const timeout = 50 * time.Millisecond
	tr := httpConnectionAcquisitionTransport{
		timeout: timeout,
		transport: httpAcquisitionTestRoundTripper(func(req *http.Request) (*http.Response, error) {
			trace := httptrace.ContextClientTrace(req.Context())
			trace.GetConn("local")
			trace.GotConn(httptrace.GotConnInfo{Reused: true})
			<-time.After(3 * timeout)
			assert.NoError(t, req.Context().Err(), "acquired connection must remove the first setup deadline")
			trace.GetConn("local") // transport retries when the previously pooled connection is stale
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}
	req := httptest.NewRequest(http.MethodGet, "http://web.example/", nil)
	started := time.Now()
	resp, err := tr.RoundTrip(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Nil(t, resp)
	require.ErrorIs(t, err, errHTTPInterceptConnectionTimeout)
	assert.GreaterOrEqual(t, time.Since(started), 3*timeout)
}

func TestHTTPConnectionAcquisitionPreservesWebSocketUpgrade(t *testing.T) {
	const timeout = 50 * time.Millisecond
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.WriteString(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		_ = rw.Flush()
		msg, err := rw.ReadString('\n')
		if err == nil {
			_, _ = fmt.Fprintf(rw, "local:%s", msg)
			_ = rw.Flush()
		}
	}))
	defer backend.Close()
	target, err := url.Parse(backend.URL)
	require.NoError(t, err)
	tr := http.DefaultTransport.(*http.Transport).Clone()
	defer tr.CloseIdleConnections()
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = httpConnectionAcquisitionTransport{transport: tr, timeout: timeout}
	frontend := httptest.NewServer(proxy)
	defer frontend.Close()
	conn, err := net.Dial("tcp", frontend.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
	_, err = io.WriteString(conn, "GET /hmr HTTP/1.1\r\nHost: frontend\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	require.NoError(t, err)
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	<-time.After(3 * timeout)
	_, err = io.WriteString(conn, "alive\n")
	require.NoError(t, err)
	msg, err := reader.ReadString('\n')
	require.NoError(t, err)
	assert.Equal(t, "local:alive\n", msg)
}

func TestHTTPConnectionAcquisitionPreservesDownstreamCancellation(t *testing.T) {
	const timeout = time.Second
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	started := make(chan struct{})
	unblock := make(chan struct{})
	tr.DialContext = func(context.Context, string, string) (net.Conn, error) {
		close(started)
		<-unblock
		return nil, io.ErrClosedPipe
	}
	defer tr.CloseIdleConnections()
	req := httptest.NewRequest(http.MethodGet, "http://web.example/", nil).WithContext(ctx)
	req.RequestURI = ""
	done := make(chan error, 1)
	go func() {
		resp, err := (httpConnectionAcquisitionTransport{transport: tr, timeout: timeout}).RoundTrip(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()
	<-started
	cancel()
	err := <-done
	assert.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, errClientStream)
	close(unblock)
}

type httpProbeStream struct {
	fakeTapStream
	closed chan struct{}
	once   sync.Once
}

func (s *httpProbeStream) CloseSend(context.Context) error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func TestDialHTTPInterceptStreamCleansLateStream(t *testing.T) {
	const timeout = 50 * time.Millisecond
	release := make(chan struct{})
	entered := make(chan context.Context, 1)
	stream := &httpProbeStream{closed: make(chan struct{})}
	start := time.Now()
	got, _, _, err := dialHTTPInterceptStream(t.Context(), timeout, func(ctx context.Context) (tunnel.Stream, error) {
		entered <- ctx
		<-release // deliberately simulate a provider that is slow to process cancellation
		return stream, nil
	})
	require.ErrorIs(t, err, errHTTPInterceptConnectionTimeout)
	require.Nil(t, got)
	assert.Less(t, time.Since(start), time.Second)
	assert.ErrorIs(t, context.Cause(<-entered), errHTTPInterceptConnectionTimeout)
	close(release)
	select {
	case <-stream.closed:
	case <-time.After(time.Second):
		t.Fatal("the detached dial leaked a stream that arrived after its setup deadline")
	}
}

func TestDialHTTPInterceptStreamKeepsEstablishedStreamUntilConnectionClose(t *testing.T) {
	const timeout = 50 * time.Millisecond
	stream := &httpProbeStream{closed: make(chan struct{})}
	got, streamCtx, cancel, err := dialHTTPInterceptStream(t.Context(), timeout, func(context.Context) (tunnel.Stream, error) {
		return stream, nil
	})
	require.NoError(t, err)
	require.Same(t, stream, got)
	local, remote := net.Pipe()
	defer remote.Close()
	conn := &httpInterceptStreamConn{Conn: local, cancel: cancel}
	defer conn.Close()
	<-time.After(3 * timeout)
	assert.NoError(t, streamCtx.Err(), "the setup timer must not become the established stream's lifetime")
	require.NoError(t, conn.Close())
	assert.ErrorIs(t, streamCtx.Err(), context.Canceled)
	_, err = remote.Read(make([]byte, 1))
	assert.ErrorIs(t, err, io.EOF)
}

// fakeTapStream is a minimal tunnel.Stream that records everything sent to it.
type fakeTapStream struct {
	mu   sync.Mutex
	sent [][]byte
}

func (s *fakeTapStream) Tag() tunnel.Tag   { return tunnel.AgentToClient }
func (s *fakeTapStream) ID() tunnel.ConnID { return "" }
func (s *fakeTapStream) Receive(context.Context) (tunnel.Message, error) {
	return nil, io.EOF
}

func (s *fakeTapStream) Send(_ context.Context, m tunnel.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, append([]byte(nil), m.Payload()...))
	return nil
}

func (s *fakeTapStream) CloseSend(context.Context) error { return nil }
func (s *fakeTapStream) PeerVersion() uint16             { return tunnel.Version }
func (s *fakeTapStream) SessionID() tunnel.SessionID     { return "" }
func (s *fakeTapStream) DialTimeout() time.Duration      { return 0 }
func (s *fakeTapStream) RoundtripLatency() time.Duration { return 0 }
func (s *fakeTapStream) SetTag(tunnel.Tag)               {}

func (s *fakeTapStream) bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []byte
	for _, b := range s.sent {
		all = append(all, b...)
	}
	return all
}

// fakeStreamProvider returns the same result for every intercept.
type fakeStreamProvider struct {
	stream    tunnel.Stream
	err       error
	create    func(context.Context) (tunnel.Stream, error)
	createFor func(context.Context, tunnel.ConnID) (tunnel.Stream, error)
	calls     atomic.Int32
}

func (p *fakeStreamProvider) CreateClientStream(
	ctx context.Context, _ tunnel.Tag, _ tunnel.SessionID, id tunnel.ConnID, _, _ time.Duration,
) (tunnel.Stream, error) {
	p.calls.Add(1)
	if p.createFor != nil {
		return p.createFor(ctx, id)
	}
	if p.create != nil {
		return p.create(ctx)
	}
	return p.stream, p.err
}

func (p *fakeStreamProvider) ReportMetrics(context.Context, *manager.TunnelMetrics) {}
func (p *fakeStreamProvider) MetricsEnabled() bool                                  { return false }

// TestHandleHTTPRequest_WiretapMatch_ForwardsAndTaps proves that a bodied request matching an
// HTTP-filtered wiretap both reaches the application (through defaultHandler) and is streamed to
// the tap as its body is read. The tap goroutine is detached from handleHTTPRequest, so its
// delivery is asserted with a bounded wait rather than immediately after the handler returns.
func TestHandleHTTPRequest_WiretapMatch_ForwardsAndTaps(t *testing.T) {
	ctx := context.Background()
	stream := &fakeTapStream{}
	f := &tcp{interceptor: &interceptor{
		lCtx:           ctx,
		intercepts:     make(interceptControllerMap),
		wiretaps:       make(interceptControllerMap),
		streamProvider: &fakeStreamProvider{stream: stream},
	}}

	wt := &manager.InterceptInfo{
		Id: "wt-1",
		Spec: &manager.InterceptSpec{
			Wiretap:       true,
			TargetHost:    "127.0.0.1",
			TargetPort:    8080,
			HeaderFilters: map[string]string{"k": "v"},
		},
		ClientSession: &manager.SessionInfo{SessionId: "sess-1"},
	}
	f.wiretaps.reconcile(ctx, []*manager.InterceptInfo{wt})

	const body = "hello wiretap"
	var handlerCalled atomic.Bool
	defaultHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled.Store(true)
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, body, string(b))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("app-response"))
	})

	req := httptest.NewRequest(http.MethodPost, "http://example.com/path", strings.NewReader(body))
	req.Header.Set("k", "v")
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		f.handleHTTPRequest(rec, req, defaultHandler)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleHTTPRequest deadlocked: request never reached the application")
	}

	require.True(t, handlerCalled.Load(), "matched request was never forwarded to the application")
	require.Equal(t, "app-response", rec.Body.String())

	// The tap is served by a detached goroutine that is intentionally not awaited by
	// handleHTTPRequest, so it may finish slightly after the handler returns.
	require.Eventually(t, func() bool {
		tapped := string(stream.bytes())
		return strings.Contains(tapped, "POST /path") && strings.Contains(tapped, body)
	}, 2*time.Second, 10*time.Millisecond, "tap did not receive the request line and body")
}

// TestHandleHTTPRequest_WiretapMatch_BodylessRequest_ForwardsAndTaps is a regression test for
// the live deadlock this package's tap-request code had: a bodyless GET is never read by
// httputil.ReverseProxy (or any handler that has no reason to touch the body), so nothing ever
// drives the tapped request body to EOF. handleHTTPRequest used to gate its return on a
// defer wg.Wait() for the tap goroutines, and the tap goroutines in turn blocked forever reading
// a pipe that would only ever see EOF once the body was read - so the handler itself hung and
// the caller received nothing. handleHTTPRequest must return once the request has been served,
// regardless of whether anything read the body, while the tap still receives what could be
// captured (the request line and headers).
//
// The defaultHandler below intentionally never reads r.Body, modeling a bodyless GET forwarded
// through httputil.ReverseProxy. Without the fix this test times out instead of failing fast,
// which is why it uses a done-channel with a bounded wait rather than letting a hang block
// `go test` indefinitely.
func TestHandleHTTPRequest_WiretapMatch_BodylessRequest_ForwardsAndTaps(t *testing.T) {
	ctx := context.Background()
	stream := &fakeTapStream{}
	f := &tcp{interceptor: &interceptor{
		lCtx:           ctx,
		intercepts:     make(interceptControllerMap),
		wiretaps:       make(interceptControllerMap),
		streamProvider: &fakeStreamProvider{stream: stream},
	}}

	wt := &manager.InterceptInfo{
		Id: "wt-1",
		Spec: &manager.InterceptSpec{
			Wiretap:       true,
			TargetHost:    "127.0.0.1",
			TargetPort:    8080,
			HeaderFilters: map[string]string{"k": "v"},
		},
		ClientSession: &manager.SessionInfo{SessionId: "sess-1"},
	}
	f.wiretaps.reconcile(ctx, []*manager.InterceptInfo{wt})

	var handlerCalled atomic.Bool
	// Models a reverse proxy forwarding a bodyless GET: it never reads r.Body.
	defaultHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled.Store(true)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("app-response"))
	})

	req := httptest.NewRequest(http.MethodGet, "http://example.com/path", nil)
	req.Header.Set("k", "v")
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		f.handleHTTPRequest(rec, req, defaultHandler)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleHTTPRequest deadlocked: request never returned for a bodyless (GET) request")
	}

	require.True(t, handlerCalled.Load(), "matched request was never forwarded to the application")
	require.Equal(t, "app-response", rec.Body.String())

	require.Eventually(t, func() bool {
		return strings.Contains(string(stream.bytes()), "GET /path")
	}, 2*time.Second, 10*time.Millisecond, "tap did not receive the request line")
}

// TestHandleHTTPRequest_WiretapNoMatch_ForwardsWithoutTapping verifies that a request which
// does not match any wiretap filter is forwarded to the application without being tapped.
func TestHandleHTTPRequest_WiretapNoMatch_ForwardsWithoutTapping(t *testing.T) {
	ctx := context.Background()
	stream := &fakeTapStream{}
	f := &tcp{interceptor: &interceptor{
		lCtx:           ctx,
		intercepts:     make(interceptControllerMap),
		wiretaps:       make(interceptControllerMap),
		streamProvider: &fakeStreamProvider{stream: stream},
	}}

	wt := &manager.InterceptInfo{
		Id: "wt-1",
		Spec: &manager.InterceptSpec{
			Wiretap:       true,
			TargetHost:    "127.0.0.1",
			TargetPort:    8080,
			HeaderFilters: map[string]string{"k": "v"},
		},
		ClientSession: &manager.SessionInfo{SessionId: "sess-1"},
	}
	f.wiretaps.reconcile(ctx, []*manager.InterceptInfo{wt})

	var handlerCalled atomic.Bool
	defaultHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled.Store(true)
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "http://example.com/path", nil)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		f.handleHTTPRequest(rec, req, defaultHandler)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleHTTPRequest deadlocked")
	}

	require.True(t, handlerCalled.Load(), "non-matching request was never forwarded to the application")
	require.Empty(t, stream.bytes(), "tap should not have received any data for a non-matching request")
}
