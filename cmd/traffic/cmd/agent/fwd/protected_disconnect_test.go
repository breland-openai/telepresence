package fwd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
)

type disconnectBackendRequest struct {
	path, remote, upgrade string
}

type disconnectBackend struct {
	server   *httptest.Server
	mu       sync.Mutex
	requests []disconnectBackendRequest
}

func newDisconnectBackend(t *testing.T) *disconnectBackend {
	t.Helper()
	b := new(disconnectBackend)
	b.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		b.mu.Lock()
		b.requests = append(b.requests, disconnectBackendRequest{req.URL.Path, req.RemoteAddr, req.Header.Get("Upgrade")})
		b.mu.Unlock()
		switch req.URL.Path {
		case "/drop-upgrade", "/drop-get", "/reset-get":
			conn, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				if req.URL.Path == "/reset-get" {
					if tcp, ok := conn.(*net.TCPConn); ok {
						_ = tcp.SetLinger(0)
					}
				}
				_ = conn.Close()
			}
		case "/developer-502":
			w.Header().Set("Cache-Control", "developer-cache")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, "developer's explicit 502")
		default:
			_, _ = io.WriteString(w, "developer listener active")
		}
	}))
	t.Cleanup(b.server.Close)
	return b
}

func (b *disconnectBackend) first(t *testing.T, path string) disconnectBackendRequest {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, request := range b.requests {
		if request.path == path {
			return request
		}
	}
	t.Fatalf("developer listener did not receive %s", path)
	return disconnectBackendRequest{}
}

func TestPermanentMediationDeveloperDisconnectBeforeHTTPHeaders(t *testing.T) {
	for _, durable := range []bool{true, false} {
		t.Run(fmt.Sprintf("durable=%t", durable), func(t *testing.T) {
			staging, stagingCalls := mediatedBackend(t, "staging", false)
			local := newDisconnectBackend(t)
			f, address := startMediatedForwarder(t, staging, false)
			guard := RouteGuard{RoutingKey: "developer", InterceptID: "client:web"}
			if durable {
				guard.Incarnation = "current"
				f.SetRouteGuards([]RouteGuard{guard}, nil)
			}
			f.SetIntercepting([]*manager.InterceptInfo{mediatedRuntime(local.server, guard, false)})
			conn, err := net.Dial("tcp", address.String())
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
			reader := bufio.NewReader(conn)
			frontendSocket := conn.LocalAddr().String()
			request := func(path, key string, upgrade bool) mediatedHTTPResponse {
				header := localRoutingKeyHeader + ": " + key + "\r\n"
				if upgrade {
					header += "Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: eHl6\r\nSec-WebSocket-Version: 13\r\n"
				}
				return mediatedH1CustomRequest(t, conn, reader, path, header)
			}
			mediatedResponse(t, request("/prime", guard.RoutingKey, false), 200, "developer listener active")
			mediatedResponse(t, request("/warm", "unrelated", false), 200, "staging:unrelated")
			assertDeveloperDisconnectResponse(t, request("/drop-upgrade", guard.RoutingKey, true), durable)
			first, upgraded := local.first(t, "/prime"), local.first(t, "/drop-upgrade")
			require.NotEmpty(t, first.remote)
			require.NotEmpty(t, upgraded.remote)
			require.Equal(t, frontendSocket, conn.LocalAddr().String(), "the warm request and upgrade must use the same hot frontend socket")
			require.Equal(t, "websocket", upgraded.upgrade)
			assertDeveloperDisconnectResponse(t, request("/drop-get", guard.RoutingKey, false), durable)
			assertDeveloperDisconnectResponse(t, request("/reset-get", guard.RoutingKey, false), durable)
			mediatedResponse(t, request("/warm-again", "unrelated", false), 200, "staging:unrelated")
			explicit := request("/developer-502", guard.RoutingKey, false)
			mediatedResponse(t, explicit, 502, "developer's explicit 502")
			require.Equal(t, "developer-cache", explicit.Header.Get("Cache-Control"))
			require.Empty(t, explicit.Header.Get("Retry-After"))
			mediatedResponse(t, request("/recovered", guard.RoutingKey, false), 200, "developer listener active")
			require.EqualValues(t, 2, stagingCalls.Load(), "neither a lost developer response nor a developer 502 may be forwarded to staging")
		})
	}
}

func assertDeveloperDisconnectResponse(t *testing.T, result mediatedHTTPResponse, durable bool) {
	t.Helper()
	if durable {
		mediatedResponse(t, result, http.StatusServiceUnavailable, "temporarily unavailable")
		return
	}
	require.Equal(t, http.StatusBadGateway, result.StatusCode)
	require.Empty(t, result.Header.Get("Retry-After"))
	require.Empty(t, result.Header.Get("Cache-Control"))
	require.True(t, strings.Contains(result.Body, "EOF") || strings.Contains(result.Body, "reset"), result.Body)
}

func TestHTTPInterceptConnectionLostClassification(t *testing.T) {
	for _, cause := range []error{io.EOF, io.ErrUnexpectedEOF, io.ErrClosedPipe, net.ErrClosed} {
		err := fmt.Errorf("HTTP transport: %w", cause)
		require.True(t, httpInterceptConnectionLost(err), cause.Error())
	}
	require.False(t, httpInterceptConnectionLost(errors.New("backend tried to switch to invalid protocol")))
	require.False(t, httpInterceptConnectionLost(context.Canceled))
}
