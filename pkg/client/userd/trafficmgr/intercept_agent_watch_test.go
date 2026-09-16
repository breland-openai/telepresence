package trafficmgr

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	authorization "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/connector"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

type interceptAgentWatchTestManager struct {
	manager.UnimplementedManagerServer
	prepareCalls atomic.Int32
	quicCalls    atomic.Int32
	quicEnabled  bool
	prepareStart chan struct{}
	prepareHold  chan struct{}
}

func (m *interceptAgentWatchTestManager) PrepareIntercept(ctx context.Context, _ *manager.CreateInterceptRequest) (*manager.PreparedIntercept, error) {
	m.prepareCalls.Add(1)
	if m.prepareStart != nil {
		m.prepareStart <- struct{}{}
		select {
		case <-m.prepareHold:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, status.Error(codes.FailedPrecondition, "test manager received PrepareIntercept")
}

func (m *interceptAgentWatchTestManager) GetQuicTunnelEndpoint(context.Context, *manager.SessionInfo) (*manager.QuicTunnelEndpoint, error) {
	m.quicCalls.Add(1)
	return &manager.QuicTunnelEndpoint{Enabled: m.quicEnabled}, nil
}

func TestCanInterceptRequiresTargetAgentWatch(t *testing.T) {
	for _, tt := range []struct {
		name            string
		namespace       string
		mapped          []string
		allowForward    []string
		namedForward    bool
		denyAccess      bool
		replace         bool
		wiretap         bool
		external        bool
		agentDisabled   bool
		quicEnabled     bool
		managerVersion  string
		wantError       string
		wantQuicCalls   int32
		wantReviewCalls int
	}{
		{name: "connected namespace omitted without permission", mapped: []string{"default", "target"}, allowForward: []string{"target"}, wantError: "intercept cannot receive traffic from namespace", wantReviewCalls: 2},
		{name: "connected namespace omitted from mapping", mapped: []string{"target"}, allowForward: []string{"target"}, wantError: "intercept cannot receive traffic from namespace", wantReviewCalls: 1},
		{name: "explicit mapped namespace without permission", namespace: "target", mapped: []string{"default", "target"}, allowForward: []string{"default"}, wantError: "intercept cannot receive traffic from namespace", wantReviewCalls: 2},
		{name: "explicit named-pod-only permission is not a namespace watch", namespace: "target", mapped: []string{"default", "target"}, allowForward: []string{"default"}, namedForward: true, wantError: "intercept cannot receive traffic from namespace", wantReviewCalls: 2},
		{name: "replacement without any watcher", mapped: []string{"default"}, replace: true, wantError: "replace cannot receive traffic from namespace", wantReviewCalls: 1},
		{name: "wiretap without target watcher", namespace: "target", mapped: []string{"default", "target"}, wiretap: true, wantError: "wiretap cannot receive traffic from namespace", wantReviewCalls: 2},
		{name: "namespace denied stays unmapped", namespace: "target", mapped: []string{"default", "target"}, allowForward: []string{"default", "target"}, denyAccess: true, wantError: "namespace \"target\" is not mapped or is not accessible"},
		{name: "authorized connected namespace", mapped: []string{"default", "target"}, allowForward: []string{"default"}, wantReviewCalls: 2},
		{name: "authorized explicit mapped namespace", namespace: "target", mapped: []string{"default", "target"}, allowForward: []string{"default", "target"}, wantReviewCalls: 2},
		{name: "authorized replacement", namespace: "target", mapped: []string{"default", "target"}, allowForward: []string{"target"}, replace: true, wantReviewCalls: 2},
		{name: "authorized wiretap", namespace: "target", mapped: []string{"default", "target"}, allowForward: []string{"target"}, wiretap: true, wantReviewCalls: 2},
		{name: "old manager still handles connected namespace", mapped: []string{"default", "target"}, allowForward: []string{"default", "target"}, managerVersion: "2.27.9", wantReviewCalls: 2},
		{name: "old manager cannot watch other authorized namespace", namespace: "target", mapped: []string{"default", "target"}, allowForward: []string{"default", "target"}, managerVersion: "2.27.9", wantError: "traffic-manager version 2.27.9 only watches", wantReviewCalls: 2},
		{name: "manager namespace-watch protocol boundary", namespace: "target", mapped: []string{"default", "target"}, allowForward: []string{"default", "target"}, managerVersion: "2.28.0", wantReviewCalls: 2},
		{name: "next4 manager handles explicit target", namespace: "target", mapped: []string{"default", "target"}, allowForward: []string{"default", "target"}, managerVersion: "2.31.2-breland.shared-service.37", wantReviewCalls: 2},
		{name: "external mapped namespace uses manager authorization and no Kubernetes", namespace: "target", mapped: []string{"default", "target"}, external: true, quicEnabled: true, wantQuicCalls: 1},
		{name: "external omitted connected namespace avoids Kubernetes guidance", mapped: []string{"target"}, external: true, quicEnabled: true, wantError: "intercept cannot receive traffic from namespace", wantQuicCalls: 1},
		{name: "external missing QUIC retains original guard", namespace: "target", mapped: []string{"default", "target"}, external: true, wantError: "the QUIC tunnel is not available", wantQuicCalls: 1},
		{name: "disabled agent forwarding retains original guard", namespace: "target", mapped: []string{"default", "target"}, external: true, agentDisabled: true, quicEnabled: true, wantError: "cluster.agentPortForward is disabled"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &interceptAgentWatchTestManager{quicEnabled: tt.quicEnabled}
			s := newIngestTestSession(t, dialTestManager(t, mgr))
			cfg := client.GetDefaultConfig()
			if tt.external {
				cfg.Cluster().ManagerAddress = "tls://traffic-manager.example.com:8443"
			}
			if tt.agentDisabled {
				cfg.Cluster().AgentPortForward = false
			}
			if tt.managerVersion == "" {
				tt.managerVersion = "2.32.0"
			}
			s.managerVersion = semver.MustParse(tt.managerVersion)
			grants := make(map[string]bool, len(tt.allowForward))
			for _, ns := range tt.allowForward {
				grants[ns] = true
			}
			clientset := fake.NewSimpleClientset()
			var reviewNamespaces []string
			var allReviews int
			clientset.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
				allReviews++
				review := action.(k8stesting.CreateAction).GetObject().(*authorization.SelfSubjectAccessReview)
				attr := review.Spec.ResourceAttributes
				if attr.Verb == "get" && attr.Resource == "pods" {
					review.Status.Allowed = !tt.denyAccess || attr.Namespace != tt.namespace
				} else if attr.Verb == "create" && attr.Resource == "pods" && attr.Subresource == "portforward" {
					require.Empty(t, attr.Name, "only an unnamed, namespace-wide permission can enable the watch")
					reviewNamespaces = append(reviewNamespaces, attr.Namespace)
					review.Status.Allowed = grants[attr.Namespace] || (tt.namedForward && attr.Name != "")
				}
				return true, review, nil
			})
			s.Context = k8sapi.WithK8sInterface(client.WithConfig(s.Context, cfg), clientset)
			s.SetMappedNamespaces(tt.mapped)
			req := &rpc.CreateInterceptRequest{Spec: &manager.InterceptSpec{
				Name: "test", Agent: "example", Namespace: tt.namespace, TargetHost: "127.0.0.1", Replace: tt.replace, Wiretap: tt.wiretap,
			}}

			_, err := s.CanIntercept(s, req)
			require.Error(t, err)
			if tt.wantError == "" {
				require.EqualValues(t, 1, mgr.prepareCalls.Load())
			} else {
				require.Zero(t, mgr.prepareCalls.Load(), "a permissive manager must never get a chance to create a dead intercept")
			}
			require.Equal(t, tt.wantQuicCalls, mgr.quicCalls.Load())
			require.Len(t, reviewNamespaces, tt.wantReviewCalls)
			if tt.external {
				require.Zero(t, allReviews, "external transport must never contact the Kubernetes API")
			}
			if tt.wantError == "" {
				require.EqualError(t, err, "test manager received PrepareIntercept")
				return
			}
			require.Equal(t, errcat.User, errcat.GetCategory(err))
			require.Contains(t, err.Error(), tt.wantError)
			attachment := "intercept"
			if tt.replace {
				attachment = "replacement"
			} else if tt.wiretap {
				attachment = "wiretap"
			}
			if tt.wantReviewCalls > 0 && tt.managerVersion != "2.27.9" {
				namespace := tt.namespace
				if namespace == "" {
					namespace = "default"
				}
				require.Contains(t, err.Error(), "kubectl auth can-i create pods/portforward --namespace "+namespace)
				require.Contains(t, err.Error(), "--mapped-namespaces including \""+namespace+"\"")
				require.Contains(t, err.Error(), "No "+attachment+" was created")
			}
			if tt.external {
				require.NotContains(t, err.Error(), "kubectl")
				if tt.wantError == "intercept cannot receive traffic from namespace" {
					require.Contains(t, err.Error(), "--mapped-namespaces including \"default\"")
					require.Contains(t, err.Error(), "No "+attachment+" was created")
				}
			}
			if tt.managerVersion == "2.27.9" {
				require.Contains(t, err.Error(), "connected namespace \"default\"")
				require.Contains(t, err.Error(), "Reconnect with --namespace \"target\"")
				require.Contains(t, err.Error(), "upgrade the traffic-manager to version 2.28 or newer")
				require.Contains(t, err.Error(), "No "+attachment+" was created")
			}
		})
	}
}

func TestCanInterceptDeniedTargetDoesNotJoinHeldManagerHandoff(t *testing.T) {
	mgr := &interceptAgentWatchTestManager{
		prepareStart: make(chan struct{}, 2),
		prepareHold:  make(chan struct{}),
	}
	s := newIngestTestSession(t, dialTestManager(t, mgr))
	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		review := action.(k8stesting.CreateAction).GetObject().(*authorization.SelfSubjectAccessReview)
		attr := review.Spec.ResourceAttributes
		review.Status.Allowed = attr.Verb == "get" || (attr.Namespace == "default" && attr.Verb == "create" && attr.Resource == "pods" && attr.Subresource == "portforward")
		return true, review, nil
	})
	s.Context = k8sapi.WithK8sInterface(s.Context, clientset)
	s.SetMappedNamespaces([]string{"default", "target"})
	start := func(namespace string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := s.CanIntercept(s, &rpc.CreateInterceptRequest{Spec: &manager.InterceptSpec{
				Name: namespace, Agent: "example", Namespace: namespace, TargetHost: "127.0.0.1",
			}})
			done <- err
		}()
		return done
	}
	allowed := start("default")
	select {
	case <-mgr.prepareStart:
	case <-time.After(5 * time.Second):
		t.Fatal("authorized baseline did not reach the real manager")
	}
	denied := start("target")
	select {
	case err := <-denied:
		require.ErrorContains(t, err, "intercept cannot receive traffic from namespace \"target\"")
		require.Equal(t, errcat.User, errcat.GetCategory(err))
	case <-time.After(5 * time.Second):
		t.Fatal("denied target joined the authorized request's manager handoff")
	}
	require.EqualValues(t, 1, mgr.prepareCalls.Load())
	select {
	case err := <-allowed:
		t.Fatalf("authorized baseline did not stay held: %v", err)
	default:
	}
	close(mgr.prepareHold)
	select {
	case err := <-allowed:
		require.EqualError(t, err, "test manager received PrepareIntercept")
	case <-time.After(5 * time.Second):
		t.Fatal("authorized baseline did not finish after the manager released it")
	}
	require.EqualValues(t, 1, mgr.prepareCalls.Load())
}
