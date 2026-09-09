package trafficmgr

import (
	"context"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
)

func TestShortcutEligible(t *testing.T) {
	plain := &manager.InterceptSpec{Mechanism: "tcp"}
	filtered := &manager.InterceptSpec{Mechanism: "http", HeaderFilters: map[string]string{"x-route-key": "me"}}
	pathFiltered := &manager.InterceptSpec{Mechanism: "http", PathFilters: []string{"/api"}}
	wiretap := &manager.InterceptSpec{Mechanism: "tcp", Wiretap: true}

	// All intercepts qualify when the shortcut is global.
	assert.True(t, shortcutEligible(plain, true))
	assert.True(t, shortcutEligible(filtered, true))
	assert.True(t, shortcutEligible(pathFiltered, true))

	// Only unfiltered intercepts qualify when the shortcut is not global.
	assert.True(t, shortcutEligible(plain, false))
	assert.False(t, shortcutEligible(filtered, false))
	assert.False(t, shortcutEligible(pathFiltered, false))

	// A wiretap mirrors traffic rather than redirecting it, so it never qualifies.
	assert.False(t, shortcutEligible(wiretap, true))
	assert.False(t, shortcutEligible(wiretap, false))
}

type shortcutBlockingRoot struct {
	daemon.DaemonClient
	calls        atomic.Int32
	firstStarted chan struct{}
	releaseFirst chan struct{}
	received     chan *daemon.SetInterceptShortcutsRequest
}

func (r *shortcutBlockingRoot) SetInterceptShortcuts(
	ctx context.Context, request *daemon.SetInterceptShortcutsRequest, _ ...grpc.CallOption,
) (*emptypb.Empty, error) {
	r.received <- proto.Clone(request).(*daemon.SetInterceptShortcutsRequest)
	if r.calls.Add(1) == 1 {
		close(r.firstStarted)
		select {
		case <-r.releaseFirst:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &emptypb.Empty{}, nil
}

func TestDurableShortcutConfirmationRetractsOlderRootWriteInOrder(t *testing.T) {
	root := &shortcutBlockingRoot{firstStarted: make(chan struct{}), releaseFirst: make(chan struct{}), received: make(chan *daemon.SetInterceptShortcutsRequest, 4)}
	ctx := client.WithConfig(t.Context(), client.GetDefaultConfig())
	legacy := &manager.InterceptInfo{
		Id: "session:web", Disposition: manager.InterceptDispositionType_ACTIVE,
		Spec: &manager.InterceptSpec{
			Name: "web", Namespace: "apps", Agent: "web", Mechanism: "tcp", Protocol: "TCP", ContainerPort: 8080,
			ServicePort: 80, ServiceIps: [][]byte{netip.MustParseAddr("10.96.0.10").AsSlice()}, TargetHost: "127.0.0.1", TargetPort: 18080,
			HeaderFilters: map[string]string{"X-Local-Routing-Key": "developer-key"},
		},
	}
	confirmed := proto.Clone(legacy).(*manager.InterceptInfo)
	confirmed.RouteIncarnation = "confirmed"
	s := &session{
		Cluster: &k8s.Cluster{Kubeconfig: &k8s.Kubeconfig{Context: ctx}}, rootDaemon: root,
		currentIntercepts: map[string]*intercept{legacy.Id: {InterceptInfo: legacy}},
	}
	firstDone, confirmedDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(firstDone); s.pushInterceptShortcuts() }()
	<-root.firstStarted
	require.Len(t, (<-root.received).Shortcuts, 1, "the old manager snapshot initially qualified for the global shortcut")
	go func() { defer close(confirmedDone); s.rememberRouteIncarnation(confirmed, confirmed.RouteIncarnation) }()
	require.Eventually(t, func() bool { return s.requiresDurableRouteMediation(legacy) }, time.Second, time.Millisecond)
	require.EqualValues(t, 1, root.calls.Load(), "the correction cannot race ahead of the older root write")
	close(root.releaseFirst)
	<-firstDone
	<-confirmedDone
	require.Empty(t, (<-root.received).Shortcuts, "the correction must be the last applied complete table")

	s.currentInterceptsLock.Lock()
	s.currentIntercepts[legacy.Id].InterceptInfo = proto.Clone(legacy).(*manager.InterceptInfo)
	s.currentInterceptsLock.Unlock()
	s.pushInterceptShortcuts()
	require.Empty(t, (<-root.received).Shortcuts, "a future snapshot missing the incarnation cannot resurrect the shortcut")
}

func TestDurableHeaderRouteNeverBecomesRootDaemonConnectionShortcut(t *testing.T) {
	s := &session{}
	active := func(name string, port int32) *manager.InterceptInfo {
		return &manager.InterceptInfo{
			Id:          "session:" + name,
			Disposition: manager.InterceptDispositionType_ACTIVE,
			Spec: &manager.InterceptSpec{
				Name: name, Namespace: "ambassador", Agent: name, Mechanism: "tcp", Protocol: "TCP",
				ContainerPort: port, ServicePort: 80,
				ServiceIps: [][]byte{netip.MustParseAddr("10.96.0.10").AsSlice()}, TargetHost: "127.0.0.1", TargetPort: 18080,
			},
		}
	}
	durable := active("web", 8080)
	durable.Spec.HeaderFilters = map[string]string{"X-Local-Routing-Key": "developer-key"}
	durable.RouteIncarnation = "manager-confirmed-route"
	legacy := proto.Clone(durable).(*manager.InterceptInfo)
	legacy.RouteIncarnation = ""
	plain := active("raw", 5432)
	waiting := proto.Clone(durable).(*manager.InterceptInfo)
	waiting.Disposition = manager.InterceptDispositionType_WAITING
	replacement := proto.Clone(durable).(*manager.InterceptInfo)
	replacement.RouteIncarnation = "replacement-confirmed-route"

	for _, global := range []bool{false, true} {
		// Every reconnect snapshot, including a later creation at the same
		// workload and Service address, must continue to use agent-side HTTP
		// routing instead of publishing an unconditional shortcut to rootd.
		for _, snapshot := range [][]*manager.InterceptInfo{{waiting}, {legacy}, {durable}, {}, {legacy}, {replacement}, {legacy}} {
			require.Empty(t, s.interceptShortcuts(snapshot, global))
		}
		shortcuts := s.interceptShortcuts([]*manager.InterceptInfo{durable, plain}, global)
		require.Len(t, shortcuts, 1)
		require.Equal(t, "raw", shortcuts[0].Workload, "unfiltered TCP keeps its normal shortcut")
		require.EqualValues(t, 5432, shortcuts[0].ContainerPort)
	}
	shortcuts := (&session{}).interceptShortcuts([]*manager.InterceptInfo{legacy}, true)
	require.Len(t, shortcuts, 1, "the legacy manager did not enable durable routing, so explicit global semantics remain unchanged")
	require.Equal(t, "web", shortcuts[0].Workload)
	require.EqualValues(t, 8080, shortcuts[0].ContainerPort)
	require.Len(t, shortcuts[0].ServiceAddrs, 1)
	require.Empty(t, (&session{}).interceptShortcuts([]*manager.InterceptInfo{legacy}, false))

	// A creation response is enough to remember a durable route, even if no
	// watch entry has yet carried its incarnation to the new daemon.
	created := &session{}
	created.rememberRouteIncarnation(durable, durable.RouteIncarnation)
	require.Empty(t, created.interceptShortcuts([]*manager.InterceptInfo{legacy}, true))

	// A distinct predicate at a reused ID is not broadened by the sticky rule.
	other := proto.Clone(legacy).(*manager.InterceptInfo)
	other.Spec.HeaderFilters = map[string]string{"X-Another-Header": "developer-key"}
	require.Len(t, s.interceptShortcuts([]*manager.InterceptInfo{other}, true), 1)
	other.Spec.HeaderFilters = map[string]string{"x-local-routing-key": "another-developer"}
	require.Len(t, s.interceptShortcuts([]*manager.InterceptInfo{other}, true), 1)
	other.Spec.HeaderFilters = nil
	require.Len(t, s.interceptShortcuts([]*manager.InterceptInfo{other}, true), 1)
	other.Spec.HeaderFilters = map[string]string{"x-local-routing-key": "developer-key"}
	require.Empty(t, s.interceptShortcuts([]*manager.InterceptInfo{other}, true), "HTTP header names are case-insensitive")

	// Rootd cannot receive a connection shortcut while an exact local creation
	// is awaiting the manager's answer. Feature-off behavior is restored when
	// the manager does not confirm; one of overlapping requests cannot drop the
	// other request's protection.
	proposed := &session{}
	releaseFirst := proposed.beginDurableShortcutCandidate(legacy.Spec)
	releaseSecond := proposed.beginDurableShortcutCandidate(legacy.Spec)
	require.Empty(t, proposed.interceptShortcuts([]*manager.InterceptInfo{legacy}, true))
	releaseFirst()
	releaseFirst()
	require.Empty(t, proposed.interceptShortcuts([]*manager.InterceptInfo{legacy}, true))
	releaseSecond()
	require.Len(t, proposed.interceptShortcuts([]*manager.InterceptInfo{legacy}, true), 1)
	finishConfirmed := proposed.beginDurableShortcutCandidate(legacy.Spec)
	proposed.rememberRouteIncarnation(durable, durable.RouteIncarnation)
	finishConfirmed()
	require.Empty(t, proposed.interceptShortcuts([]*manager.InterceptInfo{legacy}, true))
}
