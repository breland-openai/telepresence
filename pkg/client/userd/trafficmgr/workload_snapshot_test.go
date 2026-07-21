package trafficmgr

import (
	"context"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
	authorization "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/connector"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func newWorkloadSnapshotTestSession(t *testing.T, intercepts map[string]*intercept) *session {
	t.Helper()

	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor("create", "selfsubjectaccessreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		review := action.(k8stesting.CreateAction).GetObject().(*authorization.SelfSubjectAccessReview)
		review.Status.Allowed = true
		return true, review, nil
	})
	ctx := k8sapi.WithK8sInterface(context.Background(), clientset)
	cluster := &k8s.Cluster{
		Kubeconfig: &k8s.Kubeconfig{
			Context:   ctx,
			Namespace: "flock-service",
		},
	}
	cluster.SetMappedNamespaces([]string{"flock-service", "other"})

	return &session{
		Cluster: cluster,
		// An initialized empty namespace models a watcher with no workload
		// metadata. The intercept-only path must not depend on those rows.
		workloads: map[string]map[workloadInfoKey]workloadInfo{
			"flock-service": {},
		},
		currentIngests:    xsync.NewMap[ingestKey, *ingest](),
		currentIntercepts: intercepts,
	}
}

func testIntercept(id, agent, namespace, kind string) *intercept {
	return &intercept{
		InterceptInfo: &manager.InterceptInfo{
			Id: id,
			Spec: &manager.InterceptSpec{
				Agent:        agent,
				Namespace:    namespace,
				WorkloadKind: kind,
			},
		},
	}
}

func TestInterceptOnlySnapshotDoesNotRequireWorkloadInfo(t *testing.T) {
	interceptInfo := testIntercept("intercept-id", "flock-deployment-hash", "flock-service", "ReplicaSet")
	s := newWorkloadSnapshotTestSession(t, map[string]*intercept{
		interceptInfo.Id: interceptInfo,
	})

	snapshot, err := s.WorkloadInfoSnapshot(
		[]string{"flock-service"},
		rpc.ListRequest_INTERCEPTS,
	)

	require.NoError(t, err)
	require.Len(t, snapshot.Workloads, 1)
	require.Equal(t, "flock-deployment-hash", snapshot.Workloads[0].Name)
	require.Equal(t, "flock-service", snapshot.Workloads[0].Namespace)
	require.Equal(t, "ReplicaSet", snapshot.Workloads[0].WorkloadResourceType)
	require.Equal(t, []*manager.InterceptInfo{interceptInfo.InterceptInfo}, snapshot.Workloads[0].InterceptInfo)
}

func TestInterceptOnlySnapshotGroupsAndFiltersIntercepts(t *testing.T) {
	first := testIntercept("a", "flock-deployment-hash", "flock-service", "ReplicaSet")
	second := testIntercept("b", "flock-deployment-hash", "flock-service", "ReplicaSet")
	anotherWorkload := testIntercept("c", "aardvark", "flock-service", "Deployment")
	otherNamespace := testIntercept("d", "flock-deployment-hash", "other", "ReplicaSet")
	replacement := testIntercept("e", "flock-deployment-hash", "flock-service", "ReplicaSet")
	replacement.Spec.NoDefaultPort = true
	wiretap := testIntercept("f", "flock-deployment-hash", "flock-service", "ReplicaSet")
	wiretap.Spec.Wiretap = true
	nilSpec := &intercept{InterceptInfo: &manager.InterceptInfo{Id: "g"}}

	s := newWorkloadSnapshotTestSession(t, map[string]*intercept{
		first.Id:           first,
		second.Id:          second,
		anotherWorkload.Id: anotherWorkload,
		otherNamespace.Id:  otherNamespace,
		replacement.Id:     replacement,
		wiretap.Id:         wiretap,
		nilSpec.Id:         nilSpec,
		"nil":              nil,
	})

	snapshot, err := s.WorkloadInfoSnapshot(
		[]string{"flock-service"},
		rpc.ListRequest_INTERCEPTS,
	)

	require.NoError(t, err)
	require.Len(t, snapshot.Workloads, 2)
	require.Equal(t, "aardvark", snapshot.Workloads[0].Name)
	require.Equal(t, "flock-deployment-hash", snapshot.Workloads[1].Name)
	require.Equal(
		t,
		[]*manager.InterceptInfo{first.InterceptInfo, second.InterceptInfo},
		snapshot.Workloads[1].InterceptInfo,
	)
}

func TestMixedInterceptFilterKeepsWorkloadSnapshotPath(t *testing.T) {
	normal := testIntercept("a", "flock-deployment-hash", "flock-service", "ReplicaSet")
	replacement := testIntercept("b", "flock-deployment-hash", "flock-service", "ReplicaSet")
	replacement.Spec.NoDefaultPort = true
	s := newWorkloadSnapshotTestSession(t, map[string]*intercept{
		normal.Id:      normal,
		replacement.Id: replacement,
	})
	s.workloads["flock-service"][workloadInfoKey{
		kind: manager.WorkloadInfo_REPLICASET,
		name: "flock-deployment-hash",
	}] = workloadInfo{}

	snapshot, err := s.WorkloadInfoSnapshot(
		[]string{"flock-service"},
		rpc.ListRequest_INTERCEPTS|rpc.ListRequest_REPLACEMENTS,
	)

	require.NoError(t, err)
	require.Len(t, snapshot.Workloads, 1)
	require.Equal(
		t,
		[]*manager.InterceptInfo{normal.InterceptInfo, replacement.InterceptInfo},
		snapshot.Workloads[0].InterceptInfo,
	)
}
