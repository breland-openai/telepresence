package trafficmgr

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	authorization "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/connector"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

type interceptAgentWatchTestManager struct {
	manager.UnimplementedManagerServer
	prepareCalls atomic.Int32
}

func (m *interceptAgentWatchTestManager) PrepareIntercept(context.Context, *manager.CreateInterceptRequest) (*manager.PreparedIntercept, error) {
	m.prepareCalls.Add(1)
	return nil, status.Error(codes.FailedPrecondition, "test manager received PrepareIntercept")
}

func TestCanInterceptRequiresTargetAgentWatch(t *testing.T) {
	for _, tt := range []struct {
		name        string
		namespace   string
		watched     []string
		replace     bool
		shouldReach bool
	}{
		{name: "connected namespace omitted without watcher", watched: []string{"other"}},
		{name: "explicit mapped namespace without watcher", namespace: "target", watched: []string{"default"}},
		{name: "replace without any watcher", replace: true},
		{name: "authorized connected namespace", watched: []string{"default"}, shouldReach: true},
		{name: "authorized explicit mapped namespace", namespace: "target", watched: []string{"default", "target"}, shouldReach: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mgr := &interceptAgentWatchTestManager{}
			s := newIngestTestSession(t, dialTestManager(t, mgr))
			clientset := fake.NewSimpleClientset()
			clientset.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
				review := action.(k8stesting.CreateAction).GetObject().(*authorization.SelfSubjectAccessReview)
				review.Status.Allowed = review.Spec.ResourceAttributes.Verb == "get"
				return true, review, nil
			})
			s.Context = k8sapi.WithK8sInterface(s.Context, clientset)
			s.SetMappedNamespaces([]string{"default", "target"})
			s.agentPodWatchNamespacesValue = tt.watched
			s.agentPodWatchNamespacesOnce.Do(func() {})
			req := &rpc.CreateInterceptRequest{Spec: &manager.InterceptSpec{
				Name: "test", Agent: "example", Namespace: tt.namespace, TargetHost: "127.0.0.1", Replace: tt.replace,
			}}

			_, err := s.CanIntercept(s, req)
			require.Error(t, err)
			if tt.shouldReach {
				require.EqualValues(t, 1, mgr.prepareCalls.Load())
				require.EqualError(t, err, "test manager received PrepareIntercept")
				return
			}
			require.Zero(t, mgr.prepareCalls.Load(), "a permissive manager must never get a chance to create a dead intercept")
			require.Equal(t, errcat.User, errcat.GetCategory(err))
			namespace := tt.namespace
			if namespace == "" {
				namespace = "default"
			}
			kind := "intercept"
			if tt.replace {
				kind = "replace"
			}
			require.Contains(t, err.Error(), kind+" cannot receive traffic from namespace \""+namespace+"\"")
			require.Contains(t, err.Error(), "kubectl auth can-i create pods/portforward --namespace "+namespace)
			require.Contains(t, err.Error(), "--mapped-namespaces including \""+namespace+"\"")
			require.Contains(t, err.Error(), "No intercept was created")
		})
	}
}
