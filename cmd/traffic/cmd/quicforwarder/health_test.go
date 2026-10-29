package quicforwarder

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
)

func TestHealthHandler_ReadyReflectsAllowlist(t *testing.T) {
	allowlist := NewAllowlist(0)
	require.False(t, allowlist.Ready())

	handler := healthHandler(allowlist)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, 503, rec.Code)
	assert.Equal(t, "no backend allowlist snapshot received yet", rec.Body.String())

	managerIP := [4]byte{10, 1, 1, 1}
	allowlist.update(context.Background(), []*rpc.QuicBackend{{Ip: managerIP[:], Kind: "manager", Port: 7778}})
	require.True(t, allowlist.Ready())

	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, "ok", rec.Body.String())
}

func TestServeHealth(t *testing.T) {
	t.Run("cancellation shuts down cleanly", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.NoError(t, ServeHealth(ctx, 0, NewAllowlist(0)))
	})

	t.Run("listen failure is returned", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		port := listener.Addr().(*net.TCPAddr).Port
		require.Error(t, ServeHealth(ctx, uint16(port), NewAllowlist(0)))
	})
}
