package fwd

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const hotHTTPRetryPoolSize = 12

type hotHTTPRetryArrival struct {
	remote, method, body, upgrade string
}

type hotHTTPRetryBackend struct {
	server                *httptest.Server
	transport             *http.Transport
	warmSeen              chan string
	warmRelease           chan struct{}
	slowSeen, slowRelease chan struct{}
	mu                    sync.Mutex
	requests              map[string][]hotHTTPRetryArrival
}

func newHotHTTPRetryBackend(t *testing.T) *hotHTTPRetryBackend {
	t.Helper()
	b := &hotHTTPRetryBackend{
		warmSeen: make(chan string, hotHTTPRetryPoolSize), warmRelease: make(chan struct{}),
		slowSeen: make(chan struct{}, 1), slowRelease: make(chan struct{}),
		requests:  make(map[string][]hotHTTPRetryArrival),
		transport: &http.Transport{MaxIdleConns: 32, MaxIdleConnsPerHost: 32, MaxConnsPerHost: 32},
	}
	b.server = httptest.NewServer(http.HandlerFunc(b.serveHTTP))
	t.Cleanup(b.server.Close)
	t.Cleanup(b.transport.CloseIdleConnections)
	return b
}

func (b *hotHTTPRetryBackend) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/warm" {
		b.warmSeen <- r.RemoteAddr
		select {
		case <-r.Context().Done():
		case <-b.warmRelease:
		}
		_, _ = io.WriteString(w, "warm")
		return
	}
	body, _ := io.ReadAll(r.Body)
	b.mu.Lock()
	b.requests[r.URL.Path] = append(b.requests[r.URL.Path], hotHTTPRetryArrival{
		remote: r.RemoteAddr, method: r.Method, body: string(body), upgrade: r.Header.Get("Upgrade"),
	})
	b.mu.Unlock()
	switch r.URL.Path {
	case "/drop":
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	case "/explicit":
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "legitimate developer 502")
	case "/slow":
		b.slowSeen <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-b.slowRelease:
		}
		_, _ = io.WriteString(w, "slow success")
	default:
		_, _ = io.WriteString(w, "healthy")
	}
}

func (b *hotHTTPRetryBackend) arrivals(path string) []hotHTTPRetryArrival {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]hotHTTPRetryArrival(nil), b.requests[path]...)
}

func (b *hotHTTPRetryBackend) useTunnelConnections(t *testing.T) {
	t.Helper()
	b.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		_, cancel := context.WithCancelCause(t.Context())
		return &httpInterceptStreamConn{Conn: conn, cancel: cancel}, nil
	}
}

func (b *hotHTTPRetryBackend) warm(t *testing.T, upgrade bool) map[string]struct{} {
	t.Helper()
	done := make(chan error, hotHTTPRetryPoolSize)
	for range hotHTTPRetryPoolSize {
		go func() {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, b.server.URL+"/warm", nil)
			if err != nil {
				done <- err
				return
			}
			if upgrade {
				// Go keeps upgrade connections in a separate onlyH1 pool even for
				// plaintext HTTP. A normal 200 response returns them to that pool.
				req.Header.Set("Connection", "Upgrade")
				req.Header.Set("Upgrade", "websocket")
			}
			resp, err := b.transport.RoundTrip(req)
			if resp != nil {
				_, bodyErr := io.Copy(io.Discard, resp.Body)
				err = errors.Join(err, bodyErr, resp.Body.Close())
			}
			done <- err
		}()
	}
	unique := make(map[string]struct{})
	for range hotHTTPRetryPoolSize {
		select {
		case remote := <-b.warmSeen:
			unique[remote] = struct{}{}
		case <-time.After(5 * time.Second):
			close(b.warmRelease)
			t.Fatal("did not establish all simultaneous real HTTP/1 connections")
		}
	}
	close(b.warmRelease)
	for range hotHTTPRetryPoolSize {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("warming a real HTTP/1 connection did not complete")
		}
	}
	require.Len(t, unique, hotHTTPRetryPoolSize)
	return unique
}

func TestProtectedHTTP1BoundsRetriesAcrossHotPool(t *testing.T) {
	for _, test := range []struct {
		name, method, body string
		upgrade, replay    bool
		wantProtected      int
	}{
		{name: "GET", method: http.MethodGet, wantProtected: 2},
		{name: "WebSocket", method: http.MethodGet, upgrade: true, wantProtected: 2},
		{name: "idempotent-body", method: http.MethodPost, body: "do not alter or drop this body", replay: true, wantProtected: 2},
		{name: "non-idempotent-body", method: http.MethodPost, body: "do not duplicate this body", wantProtected: 1},
	} {
		for _, protected := range []bool{false, true} {
			name := test.name + "/legacy"
			if protected {
				name = test.name + "/protected"
			}
			t.Run(name, func(t *testing.T) {
				backend := newHotHTTPRetryBackend(t)
				if protected {
					backend.useTunnelConnections(t)
				}
				backend.warm(t, test.upgrade)
				var body io.Reader
				if test.body != "" {
					body = strings.NewReader(test.body)
				}
				req, err := http.NewRequestWithContext(t.Context(), test.method, backend.server.URL+"/drop", body)
				require.NoError(t, err)
				if test.replay {
					req.Header.Set("Idempotency-Key", "explicitly-replayable")
					require.NotNil(t, req.GetBody)
				}
				if test.upgrade {
					req.Header.Set("Connection", "Upgrade")
					req.Header.Set("Upgrade", "websocket")
				}
				transport := httpConnectionAcquisitionTransport{transport: backend.transport, timeout: time.Second, protected: protected}
				resp, err := transport.RoundTrip(req)
				if resp != nil {
					_ = resp.Body.Close()
				}
				require.Error(t, err)
				require.Nil(t, resp)
				if protected {
					require.ErrorIs(t, err, errClientStream)
					if test.wantProtected > 1 {
						require.ErrorIs(t, err, errHTTPInterceptRetryLimit)
					}
				} else {
					require.NotErrorIs(t, err, errClientStream)
				}
				arrivals := backend.arrivals("/drop")
				if protected || test.wantProtected == 1 {
					require.Len(t, arrivals, test.wantProtected)
				} else {
					require.Len(t, arrivals, hotHTTPRetryPoolSize+1)
				}
				unique := make(map[string]struct{})
				for _, got := range arrivals {
					unique[got.remote] = struct{}{}
					require.Equal(t, test.method, got.method)
					require.Equal(t, test.body, got.body)
					if test.upgrade {
						require.Equal(t, "websocket", got.upgrade)
					}
				}
				require.Len(t, unique, len(arrivals))
			})
		}
	}
}

func TestProtectedHTTP1KeepsHealthyPoolAndSlowOrGenuineResponses(t *testing.T) {
	backend := newHotHTTPRetryBackend(t)
	backend.useTunnelConnections(t)
	warmRemotes := backend.warm(t, false)
	transport := httpConnectionAcquisitionTransport{transport: backend.transport, timeout: 80 * time.Millisecond, protected: true}
	for _, path := range []string{"/healthy", "/healthy", "/explicit"} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, backend.server.URL+path, nil)
		require.NoError(t, err)
		var reused atomic.Bool
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
			reused.Store(info.Reused)
		}}))
		resp, err := transport.RoundTrip(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, errors.Join(err, resp.Body.Close()))
		require.True(t, reused.Load())
		if path == "/explicit" {
			require.Equal(t, http.StatusBadGateway, resp.StatusCode)
			require.Equal(t, "legitimate developer 502", string(body))
		} else {
			require.Equal(t, http.StatusOK, resp.StatusCode)
		}
	}
	healthy := backend.arrivals("/healthy")
	explicit := backend.arrivals("/explicit")
	require.Len(t, healthy, 2)
	require.Len(t, explicit, 1)
	require.Equal(t, healthy[0].remote, healthy[1].remote)
	require.Equal(t, healthy[1].remote, explicit[0].remote)
	require.Contains(t, warmRemotes, healthy[0].remote)

	done := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, backend.server.URL+"/slow", nil)
		if err != nil {
			done <- err
			return
		}
		resp, err := transport.RoundTrip(req)
		if resp != nil {
			body, bodyErr := io.ReadAll(resp.Body)
			err = errors.Join(err, bodyErr, resp.Body.Close())
			if resp.StatusCode != http.StatusOK || string(body) != "slow success" {
				err = errors.Join(err, errors.New("unexpected slow developer response"))
			}
		}
		done <- err
	}()
	select {
	case <-backend.slowSeen:
	case <-time.After(5 * time.Second):
		close(backend.slowRelease)
		t.Fatal("slow developer request was never received")
	}
	select {
	case err := <-done:
		close(backend.slowRelease)
		t.Fatalf("waiting for response was canceled by the connection timeout: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(backend.slowRelease)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("slow developer response did not complete")
	}
	require.Len(t, backend.arrivals("/slow"), 1)
}

type hotHTTPRetryTLSConn struct {
	net.Conn
	closed atomic.Bool
}

func (*hotHTTPRetryTLSConn) ConnectionState() tls.ConnectionState {
	return tls.ConnectionState{NegotiatedProtocol: "h2"}
}
func (c *hotHTTPRetryTLSConn) NetConn() net.Conn { return c.Conn }
func (c *hotHTTPRetryTLSConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func TestProtectedHTTP1RetryFenceLeavesSharedHTTP2ConnectionOpen(t *testing.T) {
	one, two := net.Pipe()
	defer one.Close()
	defer two.Close()
	conn := &hotHTTPRetryTLSConn{Conn: one}
	abortHTTPInterceptH1RetryConnection(conn)
	require.False(t, conn.closed.Load())
}

func TestProtectedHTTP1RetryFencePreventsExtraTunnelWrite(t *testing.T) {
	one, two := net.Pipe()
	defer one.Close()
	defer two.Close()
	_, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	stream := &httpInterceptStreamConn{Conn: one, cancel: cancel}
	abortHTTPInterceptH1RetryConnection(stream)
	n, err := stream.Write([]byte("must not send a third request"))
	require.Zero(t, n)
	require.ErrorIs(t, err, errHTTPInterceptRetryLimit)
}

func TestProtectedHTTP1FenceDoesNotInterceptOtherLifecycleCancellation(t *testing.T) {
	one, two := net.Pipe()
	defer one.Close()
	defer two.Close()
	_, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	stream := &httpInterceptStreamConn{Conn: one, cancel: cancel}
	cancel(context.Canceled)
	require.NoError(t, two.SetReadDeadline(time.Now().Add(time.Second)))
	done := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("legacy underlying write"))
		done <- err
	}()
	buf := make([]byte, len("legacy underlying write"))
	_, err := io.ReadFull(two, buf)
	require.NoError(t, err)
	require.Equal(t, "legacy underlying write", string(buf))
	require.NoError(t, <-done)
}
