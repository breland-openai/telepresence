package fwd

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/iputil"
	"github.com/telepresenceio/telepresence/v2/pkg/matcher"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

const (
	httpInterceptMaxIdleConns        = 512
	httpInterceptMaxIdleConnsPerHost = 128
	httpInterceptMaxConnsPerHost     = 128
	httpInterceptMaxH1Retries        = 1
	httpInterceptConnectionTimeout   = time.Second
	httpInterceptSlowAfter           = 2 * time.Second
	httpInterceptVerySlow            = 10 * time.Second
	localRoutingKeyHeader            = "X-Local-Routing-Key"
)

var errHTTPInterceptConnectionTimeout = fmt.Errorf("%w: timed out waiting for a connection to the developer tunnel", errClientStream)

var errHTTPInterceptRetryLimit = fmt.Errorf("%w: developer connection closed repeatedly before responding", errClientStream)

var httpInterceptTunnelSourcePrefix = func() [8]byte { //nolint:gochecknoglobals // every forwarder in the process shares the tunnel-correlation identity
	var prefix [8]byte
	_, _ = rand.Read(prefix[:])
	prefix[0] = 0xfd // private IPv6 label used only for tunnel correlation; it is never dialed
	return prefix
}()

var httpInterceptTunnelSourceSequence atomic.Uint64 //nolint:gochecknoglobals // distinct forwarders must never issue the same tunnel-correlation address

type httpInterceptDialContextKey struct{}

type httpInterceptDialContext struct {
	src netip.AddrPort
	ii  *manager.InterceptInfo
}

type httpInterceptTransport struct {
	targetURL *url.URL
	transport *http.Transport
}

// HTTP keep-alive and HTTP/2 can start multiple transport dials from the same
// incoming socket. Give each dial its own correlation ID so an old client reply
// cannot be delivered to a later attempt. The destination and the original HTTP
// request (including RemoteAddr/X-Forwarded-For) are unchanged; the client dialer
// connects only to the destination encoded in the tunnel ID.
func uniqueHTTPInterceptTunnelSource(original netip.AddrPort) netip.AddrPort {
	var addr [16]byte
	copy(addr[:], httpInterceptTunnelSourcePrefix[:])
	binary.BigEndian.PutUint64(addr[8:], httpInterceptTunnelSourceSequence.Add(1))
	return netip.AddrPortFrom(netip.AddrFrom16(addr), original.Port())
}

// httpConnectionAcquisition bounds only the time spent waiting for a transport
// connection, including its pool queue and an internal retry on a stale connection.
// It stops its timer when a connection has been acquired; response headers, body and
// upgraded streams keep the original request lifetime. Protected HTTP/1 also limits
// retries that the standard transport itself permits to one per incoming request.
type httpConnectionAcquisition struct {
	mu             sync.Mutex
	timeout        time.Duration
	timer          *time.Timer
	cancel         context.CancelCauseFunc
	closed         bool
	limitH1Retries bool
	waiting        bool
	retries        int
	retryExhausted bool
}

func (a *httpConnectionAcquisition) start() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.startLocked()
}

func (a *httpConnectionAcquisition) startLocked() {
	if a.timer == nil && !a.closed {
		var timer *time.Timer
		timer = time.AfterFunc(a.timeout, func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			if a.timer == timer && !a.closed {
				a.closed = true
				a.cancel(errHTTPInterceptConnectionTimeout)
			}
		})
		a.timer = timer
	}
}

func (a *httpConnectionAcquisition) stop(closeAcquisition bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stopLocked(closeAcquisition)
}

func (a *httpConnectionAcquisition) stopLocked(closeAcquisition bool) {
	if a.timer != nil {
		a.timer.Stop()
		a.timer = nil
	}
	a.closed = a.closed || closeAcquisition
}

func (a *httpConnectionAcquisition) getConnection() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	if a.limitH1Retries && !a.waiting {
		a.waiting = true
		a.retries++
		if a.retries > httpInterceptMaxH1Retries {
			a.retryExhausted = true
			a.stopLocked(true)
			a.cancel(errHTTPInterceptRetryLimit)
			return
		}
	}
	a.startLocked()
}

func (a *httpConnectionAcquisition) gotConnection(info httptrace.GotConnInfo) {
	a.mu.Lock()
	a.waiting = false
	a.stopLocked(false)
	exhausted := a.retryExhausted
	a.mu.Unlock()
	if exhausted {
		abortHTTPInterceptH1RetryConnection(info.Conn)
	}
}

// The standard HTTP/1 transport can return another idle connection after its
// GetConn trace canceled the request, because both select cases are ready. That
// connection is exclusively assigned to this request: fence it before net/http
// can send a third copy. A negotiated HTTP/2 connection is shared and stays open.
func abortHTTPInterceptH1RetryConnection(conn net.Conn) {
	if conn == nil {
		return
	}
	original := conn
	if secured, ok := conn.(interface {
		ConnectionState() tls.ConnectionState
		NetConn() net.Conn
	}); ok {
		if secured.ConnectionState().NegotiatedProtocol == "h2" {
			return
		}
		conn = secured.NetConn()
	}
	if measured, ok := conn.(*metricsReportingConn); ok {
		conn = measured.Conn
	}
	if intercepted, ok := conn.(*httpInterceptStreamConn); ok {
		intercepted.retryWriteBlocked.Store(true)
		intercepted.cancel(errHTTPInterceptRetryLimit)
		// Cancellation already fences writes; network close must not delay the protected response.
		go func() { _ = original.Close() }()
		return
	}
	_ = original.Close()
}

type httpConnectionAcquisitionTransport struct {
	transport http.RoundTripper
	timeout   time.Duration
	protected bool
}

func (t httpConnectionAcquisitionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancelCause(req.Context())
	a := &httpConnectionAcquisition{timeout: t.timeout, cancel: cancel, limitH1Retries: t.protected && req.ProtoMajor == 1, waiting: true}
	a.start()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn: func(string) { a.getConnection() },
		GotConn: a.gotConnection,
	})
	resp, err := t.transport.RoundTrip(req.WithContext(ctx))
	a.stop(true)
	cause := context.Cause(ctx)
	if req.Context().Err() == nil && (errors.Is(cause, errHTTPInterceptConnectionTimeout) || errors.Is(cause, errHTTPInterceptRetryLimit)) {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		cancel(nil)
		return nil, cause
	}
	if err != nil {
		cancel(nil)
		// Once a developer socket has been acquired, its process can still exit
		// before returning HTTP headers, including while beginning a WebSocket
		// upgrade. This is the same protected-route unavailability as a failed
		// dial. A real HTTP response (including a developer's 502) is untouched.
		if t.protected && req.Context().Err() == nil && !errors.Is(err, errClientStream) && httpInterceptConnectionLost(err) {
			return nil, fmt.Errorf("%w: developer connection closed before responding: %w", errClientStream, err)
		}
		return nil, err
	}
	if resp.Body == nil {
		cancel(nil)
	} else {
		body := &httpConnectionAcquisitionBody{ReadCloser: resp.Body, cancel: cancel}
		if writable, ok := resp.Body.(io.ReadWriteCloser); ok {
			resp.Body = &httpConnectionAcquisitionReadWriteBody{httpConnectionAcquisitionBody: body, writer: writable}
		} else {
			resp.Body = body
		}
	}
	return resp, nil
}

func httpInterceptConnectionLost(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) ||
		httpInterceptSocketConnectionLost(err) {
		return true
	}
	// A healthy incoming HTTP request can outlive the inner gRPC tunnel. These
	// statuses mean that connection was interrupted, not that the developer
	// returned an HTTP/gRPC application response, which RoundTrip would return
	// separately as an http.Response. Other gRPC errors retain their diagnostics.
	switch status.Code(err) {
	case codes.Canceled, codes.Unavailable:
		return true
	default:
		return false
	}
}

type httpConnectionAcquisitionBody struct {
	io.ReadCloser
	cancel context.CancelCauseFunc
}

func (b *httpConnectionAcquisitionBody) Close() error {
	defer b.cancel(nil)
	return b.ReadCloser.Close()
}

// ReverseProxy requires a writable body for a 101 WebSocket response.
type httpConnectionAcquisitionReadWriteBody struct {
	*httpConnectionAcquisitionBody
	writer io.Writer
}

func (b *httpConnectionAcquisitionReadWriteBody) Write(p []byte) (int, error) {
	return b.writer.Write(p)
}

// A transport detaches DialContext from its initiating request so it can reuse the
// result. Bound that detached setup separately and retain the stream context until
// the returned connection closes, even for pooled connections and WebSockets.
func dialHTTPInterceptStream(
	ctx context.Context,
	timeout time.Duration,
	create func(context.Context) (tunnel.Stream, error),
) (tunnel.Stream, context.Context, context.CancelCauseFunc, error) {
	streamCtx, cancel := context.WithCancelCause(ctx)
	a := &httpConnectionAcquisition{timeout: timeout, cancel: cancel}
	a.start()
	type result struct {
		stream tunnel.Stream
		err    error
	}
	ch := make(chan result)
	go func() {
		stream, err := create(streamCtx)
		select {
		case ch <- result{stream, err}:
		case <-streamCtx.Done():
			closeUnusedHTTPInterceptStream(streamCtx, stream)
		}
	}()
	select {
	case <-streamCtx.Done():
		a.stop(true)
		return nil, nil, nil, context.Cause(streamCtx)
	case r := <-ch:
		a.stop(true)
		if cause := context.Cause(streamCtx); cause != nil {
			go closeUnusedHTTPInterceptStream(streamCtx, r.stream)
			return nil, nil, nil, cause
		}
		if r.err != nil {
			cancel(nil)
			go closeUnusedHTTPInterceptStream(streamCtx, r.stream)
			return nil, nil, nil, r.err
		}
		return r.stream, streamCtx, cancel, nil
	}
}

func closeUnusedHTTPInterceptStream(ctx context.Context, stream tunnel.Stream) {
	if stream != nil {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), httpInterceptConnectionTimeout)
		defer cancel()
		_ = stream.CloseSend(ctx)
	}
}

// The gRPC stream can be established before the developer has connected its local
// TCP target. That final acknowledgment is also setup, not application response
// time, and must complete before net/http announces GotConn and drops the budget.
func awaitHTTPInterceptDial(ctx context.Context, stream tunnel.Stream) error {
	m, err := stream.Receive(ctx)
	if err != nil {
		return fmt.Errorf("%w: waiting for the developer connection: %w", errClientStream, err)
	}
	switch m.Code() {
	case tunnel.DialOK:
		return nil
	case tunnel.DialReject:
		return fmt.Errorf("%w: developer connection was rejected", errClientStream)
	default:
		return fmt.Errorf("%w: unexpected developer connection setup message %s", errClientStream, m.Code())
	}
}

type httpInterceptStreamConn struct {
	net.Conn
	cancel            context.CancelCauseFunc
	once              sync.Once
	err               error
	retryWriteBlocked atomic.Bool
}

func (c *httpInterceptStreamConn) Write(p []byte) (int, error) {
	if c.retryWriteBlocked.Load() {
		return 0, errHTTPInterceptRetryLimit
	}
	return c.Conn.Write(p)
}

func (c *httpInterceptStreamConn) Close() error {
	c.once.Do(func() {
		defer c.cancel(nil)
		c.err = c.Conn.Close()
	})
	return c.err
}

type metricsReportingConn struct {
	net.Conn
	once   sync.Once
	report func()
}

func (c *metricsReportingConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.report)
	return err
}

type observedResponseWriter struct {
	http.ResponseWriter
	statusCode int
	bytes      int64
}

func (w *observedResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *observedResponseWriter) WriteHeader(statusCode int) {
	if w.statusCode == 0 {
		w.statusCode = statusCode
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *observedResponseWriter) Write(p []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

func (w *observedResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (f *tcp) protocols(ctx context.Context, plainText bool) *http.Protocols {
	pr := new(http.Protocols)
	pr.SetHTTP1(true)
	if tm := f.tlsManager; tm != nil {
		tp := f.Target().Port()
		if tm.UseHTTP2(ctx, tp) {
			if !plainText && tm.UseTLS(ctx, tp) {
				pr.SetHTTP2(true)
			} else {
				pr.SetUnencryptedHTTP2(true)
			}
		}
	}
	return pr
}

func (f *tcp) configureTransport(ctx context.Context, plainText bool) *http.Transport {
	trn := http.DefaultTransport.(*http.Transport).Clone()
	trn.Protocols = f.protocols(ctx, plainText)
	if trn.Protocols.UnencryptedHTTP2() {
		trn.Protocols.SetHTTP1(false)
	}
	// A node-agent forwarder dials its pass-through target inside the target
	// pod's network namespace; without this the reverse proxy would dial the
	// target pod IP from the node-agent's own namespace and never traverse the
	// proxy-port DNAT.
	if d := f.Dialer(); d != nil {
		trn.DialContext = d.DialContext
	}
	return trn
}

func (f *tcp) targetUsesTLS(ctx context.Context) bool {
	if tm := f.tlsManager; tm != nil && tm.UseTLS(ctx, f.Target().Port()) {
		return true
	}
	return false
}

func (f *tcp) configureDownstreamTLS(ctx context.Context, server *http.Server, listener net.Listener) (net.Listener, error) {
	tm := f.tlsManager
	tp := f.Target().Port()
	if tm == nil || !tm.UseTLS(ctx, tp) {
		return listener, nil
	}
	cert := tm.GetDownstreamCertificate(tp)
	if cert == nil {
		return listener, nil
	}
	server.TLSConfig = &tls.Config{Certificates: []tls.Certificate{*cert}}
	listener = tls.NewListener(listener, server.TLSConfig)
	err := http2.ConfigureServer(server, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to configure HTTP2 server: %v", err)
	}
	return listener, nil
}

func (f *tcp) acceptHTTPLoop(ctx context.Context, listener net.Listener) {
	la := listener.Addr().(*net.TCPAddr)
	scheme := "http"
	if f.targetUsesTLS(ctx) {
		scheme = "https"
	}
	targetURL := &url.URL{Scheme: scheme, Host: f.Target().String()}
	defaultHandler := httputil.NewSingleHostReverseProxy(targetURL)
	defaultHandler.ErrorHandler = proxyErrorHandler
	defaultHandler.Transport = f.configureTransport(ctx, false)
	if f.permanentHTTP && scheme == "https" {
		// Permanent mediation also proxies non-intercepted TLS traffic. Preserve
		// the original downstream SNI when verifying the real application's
		// certificate and keep connection pools separate for different SNI names;
		// the actual socket destination remains pinned to the configured app.
		trn := f.configureUpstreamTransport(ctx, false)
		trn.Proxy = nil // the virtual SNI name must never turn the pinned app dial into an HTTP CONNECT proxy request
		dial := trn.DialContext
		trn.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dial(ctx, network, f.Target().String())
		}
		defaultHandler = &httputil.ReverseProxy{
			Transport:    trn,
			ErrorHandler: proxyErrorHandler,
			Rewrite: func(r *httputil.ProxyRequest) {
				host := r.In.Host
				if r.In.TLS != nil && r.In.TLS.ServerName != "" {
					host = r.In.TLS.ServerName
				} else if name, _, err := net.SplitHostPort(host); err == nil {
					host = name
				} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
					host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
				}
				r.SetURL(targetURL)
				r.Out.Host = r.In.Host
				r.Out.URL.RawQuery = r.In.URL.RawQuery
				preserveStagingForwardedHeaders(r.In, r.Out)
				if host != "" {
					r.Out.URL.Host = net.JoinHostPort(host, "443")
				}
			},
		}
	}

	server := &http.Server{
		BaseContext: func(_ net.Listener) context.Context {
			return f.lCtx
		},
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			f.handleHTTPRequest(writer, request, defaultHandler)
		}),
		Protocols: f.protocols(ctx, false),
	}

	var err error
	listener, err = f.configureDownstreamTLS(ctx, server, listener)
	if err != nil {
		return
	}

	go func() {
		clog.Debugf(ctx, "Starting HTTP intercept forwarder on %s", la)
		defer clog.Debugf(ctx, "Done HTTP interceptor forwarding from %s", la)

		if err := server.Serve(listener); err != nil {
			clog.Errorf(ctx, "Error serving HTTP intercept: %v", err)
		}
	}()

	<-ctx.Done()
	if err := server.Shutdown(context.WithoutCancel(ctx)); err != nil {
		clog.Errorf(ctx, "Error shutting down HTTP forwarder: %v", err)
	}
}

// Preserve the forwarding headers and original Host sent by the plain staging
// proxy when a TLS request requires per-request upstream SNI rewriting.
func preserveStagingForwardedHeaders(in, out *http.Request) {
	for _, key := range []string{"Forwarded", "X-Forwarded-Host", "X-Forwarded-Proto"} {
		if values, ok := in.Header[key]; ok && !stagingConnectionHeader(in, key) {
			out.Header[key] = append([]string(nil), values...)
		}
	}
	const forwardedFor = "X-Forwarded-For"
	prior, present := in.Header[forwardedFor]
	if stagingConnectionHeader(in, forwardedFor) {
		prior, present = nil, false
	} else if present {
		out.Header[forwardedFor] = append([]string(nil), prior...)
	}
	if clientIP, _, err := net.SplitHostPort(in.RemoteAddr); err == nil {
		if present && prior == nil {
			return
		}
		if len(prior) > 0 {
			clientIP = strings.Join(prior, ", ") + ", " + clientIP
		}
		out.Header.Set(forwardedFor, clientIP)
	}
}

func stagingConnectionHeader(req *http.Request, header string) bool {
	for _, value := range req.Header.Values("Connection") {
		for token := range strings.SplitSeq(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), header) {
				return true
			}
		}
	}
	return false
}

func (f *tcp) handleHTTPRequest(writer http.ResponseWriter, req *http.Request, defaultHandler http.Handler) {
	// Copy wiretaps and intercepts to avoid holding a lock during request processing
	f.mu.Lock()
	wtIntercepts := f.wiretaps.sorted()
	intercepts := f.intercepts.sorted()
	routingKey := req.Header.Get(localRoutingKeyHeader)
	guards := f.routeGuards[routingKey]
	guardRoutes := f.guardRoutes
	unknownStartupKey := f.permanentHTTP && !f.routeSnapshotInstalled && routingKey != ""
	pendingRoute := false
	if routingKey != "" {
		for _, pending := range f.pendingGuards {
			if pending.RoutingKey == routingKey {
				pendingRoute = true
				break
			}
		}
	}
	f.mu.Unlock()

	clog.Debugf(f.lCtx, "Handling %s %s %s", req.Proto, req.Method, req.URL.Path)
	if unknownStartupKey {
		// A new process cannot distinguish a developer key from an unrelated key
		// before its first authoritative snapshot. Cached mesh endpoints can still
		// reach this not-yet-Ready Pod; never send a keyed request on to staging.
		writeHTTPInterceptUnavailable(writer)
		return
	}
	src, err := netip.ParseAddrPort(req.RemoteAddr)
	if err != nil {
		src = netip.AddrPortFrom(netip.IPv4Unspecified(), 0)
	}

	if closeTaps := f.tapHTTPRequest(req, src, wtIntercepts); closeTaps != nil {
		defer closeTaps()
	}

	// A durable exact route takes priority over every other intercept and staging.
	// Its owner/incarnation must still match the live route; otherwise the local
	// target is recovering and forwarding to any other destination is incorrect.
	if len(guards) > 0 {
		for _, guard := range guards {
			for _, ic := range intercepts {
				if ic.Id == guard.InterceptID && ic.RouteIncarnation == guard.Incarnation &&
					shouldInterceptRequest(req, ic.Spec.HeaderFilters, ic.Spec.PathFilters) {
					f.serveHTTPIntercept(ic.ctx, src, writer, req, ic.InterceptInfo, true)
					return
				}
			}
		}
		writeHTTPInterceptUnavailable(writer)
		return
	}
	if pendingRoute {
		writeHTTPInterceptUnavailable(writer)
		return
	}

	// Pass 1: Check intercepts with headers (high-priority tier)
	for _, ic := range intercepts {
		if guardRoutes && ic.RouteIncarnation != "" {
			continue // this route was removed or was not in the authoritative list
		}
		spec := ic.Spec
		if len(spec.HeaderFilters) > 0 {
			if shouldInterceptRequest(req, spec.HeaderFilters, spec.PathFilters) {
				clog.Debugf(f.lCtx, "Intercepting HTTP request %s %s with header-based intercept %s",
					req.Method, req.URL.Path, ic.Id)
				f.serveHTTPIntercept(ic.ctx, src, writer, req, ic.InterceptInfo, false)
				return
			}
		}
	}

	// Pass 2: Check intercepts with only paths (low-priority tier)
	for _, ic := range intercepts {
		if guardRoutes && ic.RouteIncarnation != "" {
			continue
		}
		spec := ic.Spec
		if len(spec.HeaderFilters) == 0 && len(spec.PathFilters) > 0 {
			if shouldInterceptRequest(req, spec.HeaderFilters, spec.PathFilters) {
				clog.Debugf(f.lCtx, "Intercepting HTTP request %s %s with path-based intercept %s",
					req.Method, req.URL.Path, ic.Id)
				f.serveHTTPIntercept(ic.ctx, src, writer, req, ic.InterceptInfo, false)
				return
			}
		}
	}
	// A permanently HTTP-mediated port also accepts the historical full-port
	// intercepts. Its connections can no longer be diverted through raw TCP, so
	// apply an unfiltered intercept to every remaining HTTP request here.
	if f.permanentHTTP {
		for _, ic := range intercepts {
			if !ic.isHTTP() && (!guardRoutes || ic.RouteIncarnation == "") {
				f.serveHTTPIntercept(ic.ctx, src, writer, req, ic.InterceptInfo, false)
				return
			}
		}
	}
	defaultHandler.ServeHTTP(writer, req)
}

func (f *tcp) tapHTTPRequest(req *http.Request, src netip.AddrPort, wtIntercepts []*interceptController) func() {
	// Taps have no precedence because they are not conflicting.
	var matching []*interceptController
	for _, ic := range wtIntercepts {
		spec := ic.Spec
		if shouldInterceptRequest(req, spec.HeaderFilters, spec.PathFilters) {
			matching = append(matching, ic)
		}
	}
	if len(matching) == 0 {
		return nil
	}
	taps, tapReader, err := addRequestTaps(f.lCtx, req, len(matching), wiretapCacheSize)
	if err != nil {
		clog.Errorf(f.lCtx, "Failed to add request taps: %v", err)
		return nil
	}
	// Wiretaps must never delay the real request. The caller unconditionally
	// closes them after serving the request, even when the body is never read;
	// completing the body or closing it later independently is also harmless.
	// The goroutines finish after the handler returns and are bounded by the
	// intercept context and the pipe closing.
	for i, ii := range matching {
		go func(tap io.Reader, ii *interceptController) {
			f.serveTap(ii.ctx, src, tap, ii.InterceptInfo)
		}(taps[i], ii)
	}
	return tapReader.closeTaps
}

func shouldInterceptRequest(req *http.Request, headerFilters map[string]string, pathFilters []string) bool {
	return matcher.NewRequest(pathFilters, headerFilters).Matches(req)
}

func ensureGRPCTrailersHeader(request *http.Request) *http.Request {
	contentType := strings.ToLower(strings.TrimSpace(strings.SplitN(request.Header.Get("Content-Type"), ";", 2)[0]))
	if contentType != "application/grpc" && !strings.HasPrefix(contentType, "application/grpc+") {
		return request
	}
	for _, value := range request.Header.Values("Te") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "trailers") {
				return request
			}
		}
	}

	// TE is hop-by-hop, so an upstream proxy may remove it before the
	// traffic-agent sees the request. ReverseProxy only forwards Te: trailers
	// when it is present on the inbound request, and local gRPC servers require
	// it to accept the forwarded request.
	request = request.Clone(request.Context())
	request.Header.Set("Te", "trailers")
	return request
}

func (f *tcp) configureUpstreamTransport(ctx context.Context, plaintext bool) *http.Transport {
	tm := f.tlsManager
	tp := f.Target().Port()
	trn := f.configureTransport(ctx, plaintext)
	trn.MaxIdleConns = max(trn.MaxIdleConns, httpInterceptMaxIdleConns)
	trn.MaxIdleConnsPerHost = max(trn.MaxIdleConnsPerHost, httpInterceptMaxIdleConnsPerHost)
	trn.MaxConnsPerHost = max(trn.MaxConnsPerHost, httpInterceptMaxConnsPerHost)
	if plaintext {
		return trn
	}
	if tm == nil || !tm.UseTLS(ctx, tp) {
		return trn
	}
	cert, useISV := tm.GetUpstreamCertificate(tp)
	if !useISV && cert == nil {
		return trn
	}
	var certs []tls.Certificate
	if cert != nil {
		certs = []tls.Certificate{*cert}
	}
	trn.TLSClientConfig = &tls.Config{Certificates: certs, InsecureSkipVerify: useISV}
	return trn
}

func httpInterceptTransportKey(ii *manager.InterceptInfo, requestProtoMajor int) string {
	if ii == nil || ii.Spec == nil || ii.ClientSession == nil {
		return ""
	}
	protocol := "h2"
	if requestProtoMajor < 2 {
		protocol = "h1"
	}
	spec := ii.Spec
	return fmt.Sprintf(
		"%s|%s|%s|%d|%t|%s",
		ii.Id,
		ii.ClientSession.SessionId,
		spec.TargetHost,
		spec.TargetPort,
		spec.Plaintext,
		protocol,
	)
}

func (f *tcp) getHTTPInterceptTransport(
	ctx context.Context,
	ii *manager.InterceptInfo,
	requestProtoMajor int,
) *httpInterceptTransport {
	key := httpInterceptTransportKey(ii, requestProtoMajor)
	if cached, ok := f.httpTransportCache.Load(key); ok {
		return cached.(*httpInterceptTransport)
	}

	spec := ii.Spec
	trn := f.configureUpstreamTransport(ctx, spec.Plaintext)

	// Keep the protocol choice tied to the request being forwarded. In
	// particular, an HTTP/1 request must not reuse a transport configured for
	// h2c prior knowledge.
	if requestProtoMajor < 2 && trn.Protocols.UnencryptedHTTP2() {
		pr := new(http.Protocols)
		pr.SetHTTP1(true)
		trn.Protocols = pr
	}

	trn.DialContext = func(dialCtx context.Context, _, _ string) (net.Conn, error) {
		di, ok := dialCtx.Value(httpInterceptDialContextKey{}).(*httpInterceptDialContext)
		if !ok {
			return nil, fmt.Errorf("missing HTTP selected intercept dial context for intercept %s", ii.Id)
		}

		f.mu.Lock()
		sp := f.streamProvider
		f.mu.Unlock()

		metricsEnabled := sp != nil && sp.MetricsEnabled()
		var ingressBytes, egressBytes *tunnel.CounterProbe
		if metricsEnabled {
			ingressBytes = tunnel.NewCounterProbe("FromClientBytes")
			egressBytes = tunnel.NewCounterProbe("ToClientBytes")
		}

		tunnelSource := uniqueHTTPInterceptTunnelSource(di.src)
		clog.Debugf(dialCtx, "HTTP selected intercept tunnel for request source %s uses correlation source %s", di.src, tunnelSource)
		s, streamCtx, cancel, err := dialHTTPInterceptStream(dialCtx, httpInterceptConnectionTimeout, func(ctx context.Context) (tunnel.Stream, error) {
			stream, err := f.createStream(ctx, tunnelSource, di.ii)
			if err == nil {
				err = awaitHTTPInterceptDial(ctx, stream)
			}
			return stream, err
		})
		if err != nil {
			return nil, err
		}
		// Ingress and egress swap places here because this is a connection where the stream is attached to a connection *to* the client, not *from* the client.
		conn := &httpInterceptStreamConn{Conn: tunnel.NewStreamConn(streamCtx, s, egressBytes, ingressBytes), cancel: cancel}
		if !metricsEnabled {
			return conn, nil
		}
		return &metricsReportingConn{
			Conn: conn,
			report: func() {
				sp.ReportMetrics(f.lCtx, &manager.TunnelMetrics{
					ClientSessionId: di.ii.ClientSession.SessionId,
					IngressBytes:    ingressBytes.GetValue(),
					EgressBytes:     egressBytes.GetValue(),
				})
			},
		}, nil
	}

	scheme := "http"
	if trn.Protocols.HTTP2() || (f.permanentHTTP && !spec.Plaintext && f.targetUsesTLS(ctx)) {
		scheme = "https"
	}
	hit := &httpInterceptTransport{
		targetURL: &url.URL{Scheme: scheme, Host: iputil.JoinHostPort(spec.TargetHost, uint16(spec.TargetPort))},
		transport: trn,
	}
	actual, loaded := f.httpTransportCache.LoadOrStore(key, hit)
	if loaded {
		trn.CloseIdleConnections()
		return actual.(*httpInterceptTransport)
	}
	return hit
}

func (f *tcp) pruneHTTPInterceptTransports(intercepts []*manager.InterceptInfo) {
	if len(intercepts) == 0 {
		f.httpTransportCache.Range(func(key, value any) bool {
			if hit, ok := value.(*httpInterceptTransport); ok {
				hit.transport.CloseIdleConnections()
			}
			f.httpTransportCache.Delete(key)
			return true
		})
		return
	}

	active := make(map[string]struct{}, len(intercepts)*2)
	for _, ii := range intercepts {
		for _, protoMajor := range []int{1, 2} {
			if key := httpInterceptTransportKey(ii, protoMajor); key != "" {
				active[key] = struct{}{}
			}
		}
	}
	f.httpTransportCache.Range(func(key, value any) bool {
		if _, ok := active[key.(string)]; ok {
			return true
		}
		if hit, ok := value.(*httpInterceptTransport); ok {
			hit.transport.CloseIdleConnections()
		}
		f.httpTransportCache.Delete(key)
		return true
	})
}

func (f *tcp) serveHTTPIntercept(
	ctx context.Context,
	src netip.AddrPort,
	writer http.ResponseWriter,
	request *http.Request,
	ii *manager.InterceptInfo,
	protected bool,
) {
	hit := f.getHTTPInterceptTransport(ctx, ii, request.ProtoMajor)
	spec := ii.Spec
	requestID := f.httpRequestID.Add(1)
	requestStart := time.Now()
	method := request.Method
	path := request.URL.RequestURI()
	if path == "" {
		path = request.URL.Path
	}
	host := request.Host
	var slowLogged atomic.Bool
	slowTimer := time.AfterFunc(httpInterceptSlowAfter, func() {
		slowLogged.Store(true)
		clog.Warnf(
			ctx,
			"HTTP selected intercept request still active after %s: request=%d intercept=%s clientSession=%s method=%s host=%q path=%q src=%s target=%s:%d",
			time.Since(requestStart).Round(time.Millisecond),
			requestID,
			ii.Id,
			ii.ClientSession.SessionId,
			method,
			host,
			path,
			src,
			spec.TargetHost,
			spec.TargetPort,
		)
	})
	defer slowTimer.Stop()

	if tlsConfig := hit.transport.TLSClientConfig; tlsConfig != nil {
		if len(tlsConfig.Certificates) > 0 {
			clog.Debugf(ctx, "Using a client certificate when connecting to %s", hit.targetURL)
		} else {
			clog.Debugf(ctx, "Not using a client certificate when connecting to %s", hit.targetURL)
		}
		if tlsConfig.InsecureSkipVerify {
			clog.Warnf(ctx, "Skipping verification of server's certificate chain and host name when connecting to %s", hit.targetURL)
		}
	} else {
		clog.Debugf(ctx, "No TLS config used when connecting to %s", hit.targetURL)
	}
	targetProxy := httputil.NewSingleHostReverseProxy(hit.targetURL)
	targetProxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		if req.Context().Err() != nil {
			clog.Debugf(ctx, "HTTP selected intercept request canceled downstream for request=%d %s %s: %v", requestID, req.Method, req.URL.Path, err)
			return
		}
		if errors.Is(err, errClientStream) {
			clog.Warnf(ctx, "HTTP selected intercept tunnel unavailable for request=%d %s %s: %v", requestID, req.Method, req.URL.Path, err)
			writeHTTPInterceptUnavailable(rw)
			return
		}
		clog.Warnf(ctx, "HTTP selected intercept proxy error for request=%d %s %s: %v", requestID, req.Method, req.URL.Path, err)
		proxyErrorHandler(rw, req, err)
	}
	targetProxy.Transport = httpConnectionAcquisitionTransport{transport: hit.transport, timeout: httpInterceptConnectionTimeout, protected: protected}
	request = ensureGRPCTrailersHeader(request)
	request = request.WithContext(context.WithValue(request.Context(), httpInterceptDialContextKey{}, &httpInterceptDialContext{
		src: src,
		ii:  ii,
	}))
	observedWriter := &observedResponseWriter{ResponseWriter: writer}
	targetProxy.ServeHTTP(observedWriter, request)

	duration := time.Since(requestStart)
	statusCode := observedWriter.statusCode
	if statusCode == 0 {
		statusCode = -1
	}
	missingResponse := statusCode == -1 && request.Context().Err() == nil
	if slowLogged.Load() || duration > httpInterceptSlowAfter || missingResponse {
		logFn := clog.Infof
		if duration > httpInterceptVerySlow || missingResponse {
			logFn = clog.Warnf
		}
		logFn(
			ctx,
			"HTTP selected intercept request finished after %s: request=%d intercept=%s clientSession=%s method=%s host=%q path=%q src=%s target=%s status=%d responseBytes=%d",
			duration.Round(time.Millisecond),
			requestID,
			ii.Id,
			ii.ClientSession.SessionId,
			method,
			host,
			path,
			src,
			hit.targetURL,
			statusCode,
			observedWriter.bytes,
		)
	}

	clog.Debugf(ctx, "Request to %s ended", hit.targetURL)
}

func proxyErrorHandler(rw http.ResponseWriter, _ *http.Request, err error) {
	writeHTTPProxyError(rw, http.StatusBadGateway, err)
}

func writeHTTPInterceptUnavailable(rw http.ResponseWriter) {
	rw.Header().Set("Retry-After", "1")
	rw.Header().Set("Cache-Control", "no-store")
	writeHTTPProxyError(rw, http.StatusServiceUnavailable, errors.New("telepresence intercept is temporarily unavailable"))
}

func writeHTTPProxyError(rw http.ResponseWriter, status int, err error) {
	type httpError struct {
		Error string `json:"error"`
	}
	h := httpError{Error: err.Error()}
	b, _ := json.Marshal(h)
	rw.Header().Set("Content-Type", "application/json")
	rw.Header().Set("Content-Length", fmt.Sprintf("%d", len(b)))
	rw.WriteHeader(status)
	_, _ = rw.Write(b)
}

func (f *tcp) serveTap(ctx context.Context, src netip.AddrPort, tap io.Reader, ii *manager.InterceptInfo) {
	s, err := f.createStream(ctx, src, ii)
	if err != nil {
		return
	}
	buf := make([]byte, 4096)
	for {
		n, err := tap.Read(buf)
		if err != nil {
			if err != io.EOF {
				clog.Errorf(ctx, "Failed to read from tap: %v", err)
			}
			break
		}
		err = s.Send(ctx, tunnel.NewMessage(tunnel.Normal, buf[:n]))
		if err != nil {
			clog.Errorf(ctx, "Failed to send to stream: %v", err)
			break
		}
	}
}
