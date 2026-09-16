package fwd

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

type requestTapHandoff struct {
	ctx      context.Context
	id       tunnel.ConnID
	observer tunnel.Stream
}

type requestTapProvider struct {
	mu       sync.Mutex
	ids      map[tunnel.ConnID]struct{}
	calls    int
	release  <-chan struct{}
	handoffs chan requestTapHandoff
}

func newRequestTapProvider(release <-chan struct{}) *requestTapProvider {
	return &requestTapProvider{
		ids: make(map[tunnel.ConnID]struct{}), release: release, handoffs: make(chan requestTapHandoff, 16),
	}
}

func (p *requestTapProvider) CreateClientStream(ctx context.Context, _ tunnel.Tag, session tunnel.SessionID, id tunnel.ConnID, _, _ time.Duration) (tunnel.Stream, error) {
	p.mu.Lock()
	p.calls++
	_, duplicate := p.ids[id]
	p.ids[id] = struct{}{}
	p.mu.Unlock()
	if duplicate {
		// The real agent's pending reverse handoffs are keyed by client session and
		// ConnID; multiple in-flight watches with the same key cannot match separately.
		return nil, errors.New("duplicate in-flight reverse connection ID")
	}
	agent, observer := tunnel.NewPipe(id, session, tunnel.AgentToClient, tunnel.ClientToAgent)
	p.handoffs <- requestTapHandoff{ctx: ctx, id: id, observer: observer}
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return agent, nil
}

func (*requestTapProvider) ReportMetrics(context.Context, *manager.TunnelMetrics) {}

func (*requestTapProvider) MetricsEnabled() bool { return false }

func (p *requestTapProvider) counts() (calls, unique int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, len(p.ids)
}

func realHTTPWiretap(t *testing.T, ctx context.Context, provider *requestTapProvider, source func(string), dialTimeout ...time.Duration) *httptest.Server {
	t.Helper()
	f := &tcp{interceptor: &interceptor{
		lCtx: ctx, intercepts: make(interceptControllerMap), wiretaps: make(interceptControllerMap), streamProvider: provider,
	}}
	info := &manager.InterceptInfo{
		Id: "request-wiretap",
		Spec: &manager.InterceptSpec{
			Wiretap: true, TargetHost: "127.0.0.1", TargetPort: 8080,
			HeaderFilters: map[string]string{"X-Observed": "yes"},
		},
		ClientSession: &manager.SessionInfo{SessionId: "wiretap-client"},
	}
	if len(dialTimeout) > 0 {
		info.Spec.DialTimeout = int64(dialTimeout[0])
	}
	f.wiretaps.reconcile(ctx, []*manager.InterceptInfo{info})
	original := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "original:"+r.URL.Path)
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if source != nil {
			source(r.RemoteAddr)
		}
		f.handleHTTPRequest(w, r, original)
	}))
	t.Cleanup(server.Close)
	return server
}

func receiveRequestTap(t *testing.T, provider *requestTapProvider) requestTapHandoff {
	t.Helper()
	select {
	case handoff := <-provider.handoffs:
		return handoff
	case <-time.After(2 * time.Second):
		t.Fatal("wiretap did not open a reverse tunnel")
		return requestTapHandoff{}
	}
}

func requestWiretapOriginal(t *testing.T, client *http.Client, url, path, header string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url+path, nil)
	require.NoError(t, err)
	req.Header.Set("X-Observed", header)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "original:"+path, string(data))
}

func TestHTTPWiretapOverlappingRequestsOnOneRealHTTPConnectionHaveUniqueReverseTunnels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	release := make(chan struct{})
	provider := newRequestTapProvider(release)
	var sources []string
	var sourceMu sync.Mutex
	server := realHTTPWiretap(t, ctx, provider, func(address string) {
		sourceMu.Lock()
		sources = append(sources, address)
		sourceMu.Unlock()
	})
	transport := &http.Transport{MaxConnsPerHost: 1}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: time.Second}
	paths := []string{"/first", "/second", "/third", "/fourth"}
	for _, path := range paths {
		requestWiretapOriginal(t, client, server.URL, path, "yes")
	}
	requestWiretapOriginal(t, client, server.URL, "/wrong", "wrong")

	// The original application returned for every request while the reverse client
	// handoffs were still blocked, and all five requests used one physical socket.
	sourceMu.Lock()
	actualSources := append([]string(nil), sources...)
	sourceMu.Unlock()
	require.Len(t, actualSources, 5)
	for _, source := range actualSources[1:] {
		require.Equal(t, actualSources[0], source)
	}
	require.Eventually(t, func() bool { calls, _ := provider.counts(); return calls == len(paths) }, 2*time.Second, 5*time.Millisecond)
	_, unique := provider.counts()
	if !assert.Equal(t, len(paths), unique, "distinct matching HTTP requests must be routable while their reverse tunnels overlap") {
		return
	}

	close(release)
	observed := make([]string, 0, len(paths))
	for range paths {
		handoff := receiveRequestTap(t, provider)
		require.Equal(t, netip.MustParseAddrPort("127.0.0.1:8080"), handoff.id.Destination())
		readCtx, readCancel := context.WithTimeout(ctx, time.Second)
		message, err := handoff.observer.Receive(readCtx)
		readCancel()
		require.NoError(t, err)
		require.Equal(t, tunnel.Normal, message.Code())
		mirrored, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(message.Payload())))
		require.NoError(t, err)
		require.Equal(t, "yes", mirrored.Header.Get("X-Observed"))
		observed = append(observed, mirrored.URL.Path)
		require.NoError(t, handoff.observer.CloseSend(ctx))
	}
	require.ElementsMatch(t, paths, observed)
}

func TestHTTPWiretapSuccessfulRequestClosesReverseStreamAndReleasesItsOwnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	provider := newRequestTapProvider(nil)
	server := realHTTPWiretap(t, ctx, provider, nil)
	requestWiretapOriginal(t, server.Client(), server.URL, "/one", "yes")
	handoff := receiveRequestTap(t, provider)
	readCtx, readCancel := context.WithTimeout(ctx, time.Second)
	t.Cleanup(readCancel)
	message, err := handoff.observer.Receive(readCtx)
	require.NoError(t, err)
	require.Contains(t, string(message.Payload()), "GET /one HTTP/1.1")
	_, err = handoff.observer.Receive(readCtx)
	assert.ErrorIs(t, err, io.EOF, "the mirrored request has ended and its observer stream must close")
	require.NoError(t, handoff.ctx.Err(), "the reverse server must stay alive while the client consumes its request and sends its response")
	go func() {
		_ = handoff.observer.Send(readCtx, tunnel.NewMessage(tunnel.DialOK, nil))
		_ = handoff.observer.Send(readCtx, tunnel.NewMessage(tunnel.Normal, []byte("observer response")))
		_ = handoff.observer.CloseSend(readCtx)
	}()
	assert.Eventually(t, func() bool { return handoff.ctx.Err() != nil }, 300*time.Millisecond, 5*time.Millisecond,
		"the real agent waits for this per-request context to release its reverse tunnel")
	require.NoError(t, ctx.Err(), "closing this tap must not cancel the wiretap interception")
}

func TestHTTPWiretapUnresponsiveClientCannotHoldARequestTunnel(t *testing.T) {
	for _, test := range []struct {
		name          string
		establishment bool
	}{
		{name: "client-does-not-establish", establishment: true},
		{name: "observer-does-not-close-reply"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			var release <-chan struct{}
			if test.establishment {
				release = make(chan struct{})
			}
			provider := newRequestTapProvider(release)
			server := realHTTPWiretap(t, ctx, provider, nil, 200*time.Millisecond)
			requestWiretapOriginal(t, server.Client(), server.URL, "/unresponsive", "yes")
			handoff := receiveRequestTap(t, provider)
			if !test.establishment {
				readCtx, readCancel := context.WithTimeout(ctx, time.Second)
				t.Cleanup(readCancel)
				message, err := handoff.observer.Receive(readCtx)
				require.NoError(t, err)
				require.Contains(t, string(message.Payload()), "GET /unresponsive HTTP/1.1")
				_, err = handoff.observer.Receive(readCtx)
				require.ErrorIs(t, err, io.EOF)
				// The observer deliberately never sends its reply or closes its side.
			}
			require.Eventually(t, func() bool { return handoff.ctx.Err() != nil }, time.Second, 5*time.Millisecond)
			require.NoError(t, ctx.Err(), "a stalled request must not cancel the whole wiretap")
			requestWiretapOriginal(t, server.Client(), server.URL, "/after", "wrong")
			calls, _ := provider.counts()
			require.Equal(t, 1, calls)
		})
	}
}
