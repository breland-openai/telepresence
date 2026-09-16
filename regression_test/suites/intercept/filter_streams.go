package intercept

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/v2/regression_test/framework/cli"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/managers"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/rt"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/workloads"
)

const (
	filteredStreamMarkerHeader = "X-Rtest-Selected-Marker"
	filteredStreamConnHeader   = "X-Rtest-Selected-Connection"
	filteredStreamWait         = 30 * time.Second
	filteredH2ClusterImage     = "ghcr.io/telepresenceio/echo-server:0.3.1@sha256:1f37f32c236f88d01a00004051aae29d92cea5fdce95a2b480b6e7093e462906"
)

// HeaderFilterStreams exercises the selected sidecar HTTP reverse proxy.
type HeaderFilterStreams struct{ rt.Suite }

func init() {
	rt.Register(&HeaderFilterStreams{}, rt.InArea("intercept"), rt.NeedsManager(managers.Default))
}

func (s *HeaderFilterStreams) Test_HTTP1ConnectionReuseAndCancellation() { s.checkFilteredStreams(1) }

func (s *HeaderFilterStreams) Test_H2CConnectionReuseAndCancellation() { s.checkFilteredStreams(2) }

func (s *HeaderFilterStreams) checkFilteredStreams(protocol int) {
	t := s.T()
	ctx := s.Ctx()
	id := rand.Text()
	path := func(name string) string { return "/" + id + "-" + name }
	target := newFilteredStreamTarget(t, protocol, path("held"))
	connection := s.Connect()
	workload := workloads.Echo(fmt.Sprintf("filter-stream-http%d", protocol))
	if protocol == 2 {
		workload.AppProtocol = "kubernetes.io/h2c"
		workload.Image = filteredH2ClusterImage
	}
	wl := s.Workload(workload)
	base := wl.ServiceURL()

	direct := newFilteredStreamClient(t, protocol)
	directResult := requireFilteredStreamLocal(t, ctx, direct, target.url(), path("direct"), target, protocol)
	direct.CloseIdleConnections()
	clusterDirect := newFilteredStreamClient(t, protocol)
	clusterPath := path("cluster-before")
	require.Eventually(t, func() bool {
		result := boundedFilteredStreamRequest(ctx, clusterDirect, base+clusterPath, "")
		return result.isCluster(clusterPath, target.marker, protocol)
	}, filteredStreamWait, 250*time.Millisecond, "the Kubernetes echo must serve the direct baseline")
	clusterDirect.CloseIdleConnections()

	a := connection.Intercept(t, wl, cli.Port(target.port, "http"), cli.MountFalse(), cli.HTTPHeader(headerKey, headerVal))
	detached := false
	defer func() {
		if !detached {
			a.DetachWithin(t, 30*time.Second, time.Second)
		}
	}()
	selected := newFilteredStreamClient(t, protocol)
	readyPath := path("selected-ready")
	require.Eventually(t, func() bool {
		result := boundedFilteredStreamRequest(ctx, selected, base+readyPath, headerVal)
		return result.isLocal(readyPath, target, protocol)
	}, filteredStreamWait, 250*time.Millisecond, "the filtered sidecar must route to the local target")

	first := requireFilteredStreamLocal(t, ctx, selected, base, path("selected-first"), target, protocol)
	second := requireFilteredStreamLocal(t, ctx, selected, base, path("selected-second"), target, protocol)
	require.NotEqual(t, directResult.connection, first.connection, "the independent direct control cannot be the agent's selected connection")
	require.Equal(t, first.connection, second.connection, "selected requests must reach the same physical target connection")
	for _, control := range []struct{ path, value string }{
		{path("missing-before"), ""}, {path("wrong-before"), "wrong-" + id},
	} {
		requireFilteredStreamCluster(t, ctx, selected, base, control.path, control.value, target, protocol)
	}

	holdCtx, cancelHold := context.WithCancel(ctx)
	defer cancelHold()
	heldDone := make(chan filteredStreamResponse, 1)
	go func() { heldDone <- doFilteredStreamRequest(holdCtx, selected, base+target.holdPath, headerVal) }()
	held := waitForFilteredStreamHold(t, target, heldDone)
	require.Equal(t, target.marker, held.Marker)
	require.Equal(t, target.holdPath, held.Path)
	require.Equal(t, protocol, held.Protocol)
	require.Equal(t, first.connection, held.Connection, "the held request must use the established physical target connection")
	if protocol == 2 {
		parallel := requireFilteredStreamLocal(t, ctx, selected, base, path("selected-parallel"), target, protocol)
		require.Equal(t, first.connection, parallel.connection, "a second H2 stream must complete while the held stream uses the same physical target connection")
	}
	cancelHold()
	heldResult := waitForFilteredStreamRequest(t, heldDone)
	require.ErrorIs(t, heldResult.err, context.Canceled, "the originating client request must be canceled")
	canceled := waitForFilteredStreamEvent(t, target.canceled)
	require.Equal(t, held, canceled, "the same physical target request must observe cancellation")

	afterFirst := requireFilteredStreamLocal(t, ctx, selected, base, path("selected-after-first"), target, protocol)
	afterSecond := requireFilteredStreamLocal(t, ctx, selected, base, path("selected-after-second"), target, protocol)
	require.Equal(t, afterFirst.connection, afterSecond.connection, "healthy post-cancel requests must reuse their physical target connection")
	if protocol == 2 {
		require.Equal(t, first.connection, afterFirst.connection, "canceling one H2 stream must preserve the established physical target connection")
	}
	for _, control := range []struct{ path, value string }{
		{path("missing-after"), ""}, {path("wrong-after"), "wrong-" + id},
	} {
		requireFilteredStreamCluster(t, ctx, selected, base, control.path, control.value, target, protocol)
	}
	for _, unmatched := range []string{path("missing-before"), path("wrong-before"), path("missing-after"), path("wrong-after")} {
		require.Empty(t, target.observations(unmatched), "the selected local target must never observe an unmatched request")
	}
	t.Logf("selected target HTTP/%d physical connections: before=%d after=%d", protocol, first.connection, afterFirst.connection)
	a.Detach(t)
	detached = true
}

type filteredStreamObservation struct {
	Marker     string `json:"marker"`
	Path       string `json:"path"`
	Protocol   int    `json:"protocol"`
	Connection uint64 `json:"connection"`
}

type filteredStreamConnectionKey struct{}

type filteredStreamTarget struct {
	listener net.Listener
	port     int
	marker   string
	holdPath string
	started  chan filteredStreamObservation
	canceled chan filteredStreamObservation
	release  chan struct{}
	mu       sync.Mutex
	recorded map[string][]filteredStreamObservation
}

func newFilteredStreamTarget(t testing.TB, protocol int, holdPath string) *filteredStreamTarget {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address, err := netip.ParseAddrPort(listener.Addr().String())
	require.NoError(t, err)
	target := &filteredStreamTarget{
		listener: listener, port: int(address.Port()), marker: "rtest-selected-" + rand.Text(), holdPath: holdPath,
		started: make(chan filteredStreamObservation, 1), canceled: make(chan filteredStreamObservation, 1),
		release: make(chan struct{}), recorded: make(map[string][]filteredStreamObservation),
	}
	var nextConnection atomic.Uint64
	protocols := new(http.Protocols)
	if protocol == 2 {
		protocols.SetUnencryptedHTTP2(true)
	} else {
		protocols.SetHTTP1(true)
	}
	server := &http.Server{
		Handler:   http.HandlerFunc(target.handle),
		Protocols: protocols,
		ConnContext: func(ctx context.Context, _ net.Conn) context.Context {
			return context.WithValue(ctx, filteredStreamConnectionKey{}, nextConnection.Add(1))
		},
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		close(target.release)
		_ = server.Close()
	})
	return target
}

func (target *filteredStreamTarget) url() string { return "http://" + target.listener.Addr().String() }

func (target *filteredStreamTarget) observations(path string) []filteredStreamObservation {
	target.mu.Lock()
	defer target.mu.Unlock()
	return append([]filteredStreamObservation(nil), target.recorded[path]...)
}

func (target *filteredStreamTarget) handle(w http.ResponseWriter, r *http.Request) {
	connection, _ := r.Context().Value(filteredStreamConnectionKey{}).(uint64)
	observed := filteredStreamObservation{
		Marker: target.marker, Path: r.URL.Path, Protocol: r.ProtoMajor, Connection: connection,
	}
	target.mu.Lock()
	target.recorded[r.URL.Path] = append(target.recorded[r.URL.Path], observed)
	target.mu.Unlock()
	if r.URL.Path == target.holdPath {
		select {
		case target.started <- observed:
		default:
		}
		select {
		case <-r.Context().Done():
			select {
			case target.canceled <- observed:
			default:
			}
			return
		case <-target.release:
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(filteredStreamMarkerHeader, target.marker)
	w.Header().Set(filteredStreamConnHeader, strconv.FormatUint(connection, 10))
	_ = json.NewEncoder(w).Encode(observed)
}

func newFilteredStreamClient(t testing.TB, protocol int) *http.Client {
	t.Helper()
	var client *http.Client
	if protocol == 2 {
		client = newH2CClient()
		client.Timeout = 0
	} else {
		client = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1}}
	}
	t.Cleanup(client.CloseIdleConnections)
	return client
}

type filteredStreamResponse struct {
	status     int
	protocol   int
	marker     string
	connection uint64
	body       string
	err        error
}

func doFilteredStreamRequest(ctx context.Context, client *http.Client, url, headerValue string) filteredStreamResponse {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return filteredStreamResponse{err: err}
	}
	if headerValue != "" {
		req.Header.Set(headerKey, headerValue)
	}
	response, err := client.Do(req)
	if err != nil {
		return filteredStreamResponse{err: err}
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	result := filteredStreamResponse{
		status: response.StatusCode, protocol: response.ProtoMajor, marker: response.Header.Get(filteredStreamMarkerHeader),
		body: string(body), err: err,
	}
	if connHeader := response.Header.Get(filteredStreamConnHeader); connHeader != "" {
		result.connection, err = strconv.ParseUint(connHeader, 10, 64)
		if err != nil {
			result.err = err
		}
	}
	return result
}

func boundedFilteredStreamRequest(ctx context.Context, client *http.Client, url, headerValue string) filteredStreamResponse {
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return doFilteredStreamRequest(bounded, client, url, headerValue)
}

func (result filteredStreamResponse) isLocal(path string, target *filteredStreamTarget, protocol int) bool {
	if result.err != nil || result.status != http.StatusOK || result.protocol != protocol ||
		result.marker != target.marker || result.connection == 0 {
		return false
	}
	var observed filteredStreamObservation
	return json.Unmarshal([]byte(result.body), &observed) == nil && observed.Marker == target.marker &&
		observed.Path == path && observed.Protocol == protocol && observed.Connection == result.connection
}

func (result filteredStreamResponse) isCluster(path, targetMarker string, protocol int) bool {
	return result.err == nil && result.status == http.StatusOK && result.protocol == protocol &&
		result.marker == "" && result.connection == 0 && !strings.Contains(result.body, targetMarker) &&
		strings.Contains(result.body, "GET "+path+"\n") && strings.Contains(result.body, "Request served by ")
}

func requireFilteredStreamLocal(t testing.TB, ctx context.Context, client *http.Client, base, path string, target *filteredStreamTarget, protocol int) filteredStreamResponse {
	t.Helper()
	result := boundedFilteredStreamRequest(ctx, client, base+path, headerVal)
	require.True(t, result.isLocal(path, target, protocol), "selected %s: status=%d protocol=%d marker=%q connection=%d body=%q error=%v",
		path, result.status, result.protocol, result.marker, result.connection, result.body, result.err)
	return result
}

func requireFilteredStreamCluster(t testing.TB, ctx context.Context, client *http.Client, base, path, value string, target *filteredStreamTarget, protocol int) {
	t.Helper()
	result := boundedFilteredStreamRequest(ctx, client, base+path, value)
	require.True(t, result.isCluster(path, target.marker, protocol), "unselected %s: status=%d protocol=%d marker=%q connection=%d body=%q error=%v",
		path, result.status, result.protocol, result.marker, result.connection, result.body, result.err)
	require.Empty(t, target.observations(path), "the selected local target must never observe an unmatched request")
}

func waitForFilteredStreamHold(t testing.TB, target *filteredStreamTarget, completed <-chan filteredStreamResponse) filteredStreamObservation {
	t.Helper()
	timer := time.NewTimer(filteredStreamWait)
	defer timer.Stop()
	select {
	case observed := <-target.started:
		return observed
	case response := <-completed:
		t.Fatalf("held originating request ended before reaching the selected target: status=%d body=%q error=%v", response.status, response.body, response.err)
	case <-timer.C:
		t.Fatal("timed out waiting for the held request to reach the selected target")
	}
	return filteredStreamObservation{}
}

func waitForFilteredStreamEvent(t testing.TB, event <-chan filteredStreamObservation) filteredStreamObservation {
	t.Helper()
	timer := time.NewTimer(filteredStreamWait)
	defer timer.Stop()
	select {
	case observed := <-event:
		return observed
	case <-timer.C:
		t.Fatal("timed out waiting for the selected target to observe request cancellation")
	}
	return filteredStreamObservation{}
}

func waitForFilteredStreamRequest(t testing.TB, event <-chan filteredStreamResponse) filteredStreamResponse {
	t.Helper()
	timer := time.NewTimer(filteredStreamWait)
	defer timer.Stop()
	select {
	case response := <-event:
		return response
	case <-timer.C:
		t.Fatal("timed out waiting for originating request cancellation")
	}
	return filteredStreamResponse{}
}
