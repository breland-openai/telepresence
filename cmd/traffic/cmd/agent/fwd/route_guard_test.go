package fwd

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func TestHTTPRouteGuardProtectsExactKeyAndIncarnation(t *testing.T) {
	f := NewTCPInterceptor(t.Context(), types.PortAndProto{Proto: types.ProtoTCP}, tunnel.AgentToClient, nil,
		netip.MustParseAddrPort("127.0.0.1:3000")).(*tcp)
	provider := &fakeStreamProvider{err: io.ErrClosedPipe}
	f.SetStreamProvider(provider)
	t.Cleanup(func() { f.SetIntercepting(nil) })
	stagingCalls := 0
	staging := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { stagingCalls++; w.WriteHeader(http.StatusAccepted) })
	check := func(key string, websocket bool, expected int) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "http://web.example/hmr", nil)
		if key != "" {
			req.Header.Set("x-local-routing-key", key)
		}
		if websocket {
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
		}
		start := time.Now()
		rec := httptest.NewRecorder()
		f.handleHTTPRequest(rec, req, staging)
		require.Equal(t, expected, rec.Code)
		if expected == http.StatusServiceUnavailable {
			require.Equal(t, "1", rec.Header().Get("Retry-After"))
			require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
			require.Less(t, time.Since(start), time.Second)
		}
	}
	runtime := func(id, incarnation, key string) *manager.InterceptInfo {
		return &manager.InterceptInfo{
			Id: id, RouteIncarnation: incarnation, ClientSession: &manager.SessionInfo{SessionId: "client"},
			Spec: &manager.InterceptSpec{TargetHost: "127.0.0.1", TargetPort: 8080, HeaderFilters: map[string]string{localRoutingKeyHeader: key}},
		}
	}
	guard := RouteGuard{RoutingKey: "developer", InterceptID: "client:web", Incarnation: "second"}
	f.SetRouteGuards([]RouteGuard{guard}, nil)
	require.True(t, f.IsHTTP(), "a guard must independently switch the port into HTTP mode")
	check("developer", false, http.StatusServiceUnavailable)
	check("developer", true, http.StatusServiceUnavailable)
	check("", false, http.StatusAccepted)
	check("different", false, http.StatusAccepted)
	check("developer-extra", false, http.StatusAccepted)
	require.Zero(t, provider.calls.Load())
	require.Equal(t, 3, stagingCalls)

	// Neither a previous incarnation, a different owner/id, nor a broader live
	// interception may hijack a key the durable route says belongs elsewhere.
	f.SetIntercepting([]*manager.InterceptInfo{
		runtime("client:web", "first", "developer"), runtime("other:web", "second", "developer"), runtime("legacy", "", "dev.*"),
	})
	check("developer", false, http.StatusServiceUnavailable)
	require.Zero(t, provider.calls.Load())
	require.Equal(t, 3, stagingCalls)

	f.SetIntercepting([]*manager.InterceptInfo{runtime("client:web", "second", "developer")})
	check("developer", false, http.StatusServiceUnavailable) // active route tries its currently unreachable tunnel
	require.EqualValues(t, 1, provider.calls.Load())
	require.Equal(t, 3, stagingCalls)

	// Durable explicit removal restores staging immediately even if the ordinary
	// active-intercept stream has not delivered its removal yet.
	f.SetRouteGuards(nil, []RouteGuard{guard})
	check("developer", false, http.StatusAccepted)
	require.EqualValues(t, 1, provider.calls.Load())
	require.Equal(t, 4, stagingCalls)
}

func TestHTTPRouteGuardCannotRaceRuntimeIdentityChanges(t *testing.T) {
	f := NewTCPInterceptor(t.Context(), types.PortAndProto{Proto: types.ProtoTCP}, tunnel.AgentToClient, nil,
		netip.MustParseAddrPort("127.0.0.1:3000")).(*tcp)
	f.SetStreamProvider(&fakeStreamProvider{err: io.ErrClosedPipe})
	const id = "client:web"
	f.SetRouteGuards([]RouteGuard{{RoutingKey: "developer", InterceptID: id, Incarnation: "current"}}, nil)
	makeIntercept := func(incarnation string) *manager.InterceptInfo {
		return &manager.InterceptInfo{
			Id: id, RouteIncarnation: incarnation, ClientSession: &manager.SessionInfo{SessionId: "client"},
			Spec: &manager.InterceptSpec{TargetHost: "127.0.0.1", TargetPort: 8080, HeaderFilters: map[string]string{localRoutingKeyHeader: "developer"}},
		}
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		for range 100 {
			f.SetIntercepting([]*manager.InterceptInfo{makeIntercept("stale")})
			f.SetIntercepting([]*manager.InterceptInfo{makeIntercept("current")})
		}
	})
	workers.Go(func() {
		<-start
		for range 100 {
			req := httptest.NewRequest(http.MethodGet, "http://web.example/hmr", nil)
			req.Header.Set(localRoutingKeyHeader, "developer")
			rw := httptest.NewRecorder()
			f.handleHTTPRequest(rw, req, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
			if rw.Code != http.StatusServiceUnavailable {
				t.Errorf("protected route returned %d", rw.Code)
			}
		}
	})
	close(start)
	workers.Wait()
	f.SetIntercepting(nil)
}

func TestHTTPRouteGuardPendingSurvivesOlderEmptySnapshotAndExplicitRemoval(t *testing.T) {
	f := NewTCPInterceptor(t.Context(), types.PortAndProto{Proto: types.ProtoTCP}, tunnel.AgentToClient, nil,
		netip.MustParseAddrPort("127.0.0.1:3000")).(*tcp)
	f.SetRouteGuards(nil, nil) // complete baseline before the developer creates a route
	guard := RouteGuard{RoutingKey: "new-developer", InterceptID: "client:web", Incarnation: "first"}
	check := func(expected int) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "http://web.example/hmr", nil)
		req.Header.Set(localRoutingKeyHeader, guard.RoutingKey)
		rw := httptest.NewRecorder()
		f.handleHTTPRequest(rw, req, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }))
		require.Equal(t, expected, rw.Code)
	}
	check(http.StatusAccepted)
	// Runtime WAITING arrives first; a previously queued durable empty snapshot
	// and an empty reconnect from the ordinary watcher must not undo its guard.
	f.SetPendingRouteGuards([]RouteGuard{guard})
	check(http.StatusServiceUnavailable)
	f.SetRouteGuards(nil, nil)
	f.SetPendingRouteGuards(nil)
	check(http.StatusServiceUnavailable)
	require.True(t, f.IsHTTP())

	f.SetRouteGuards([]RouteGuard{guard}, nil)
	check(http.StatusServiceUnavailable)
	// No longer selected by the service: absence after a desired snapshot may
	// restore staging. Reselection of the same still-desired incarnation works.
	f.SetRouteGuards(nil, nil)
	check(http.StatusAccepted)
	f.SetRouteGuards([]RouteGuard{guard}, nil)
	check(http.StatusServiceUnavailable)
	// An explicit tombstone prevents stale ordinary or durable snapshots from
	// resurrecting this creation. A fresh incarnation can protect the key again.
	f.SetRouteGuards(nil, []RouteGuard{guard})
	f.SetPendingRouteGuards([]RouteGuard{guard})
	f.SetRouteGuards([]RouteGuard{guard}, nil)
	check(http.StatusAccepted)
	guard.Incarnation = "second"
	f.SetPendingRouteGuards([]RouteGuard{guard})
	check(http.StatusServiceUnavailable)
}
