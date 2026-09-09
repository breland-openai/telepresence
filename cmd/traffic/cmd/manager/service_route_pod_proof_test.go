package manager

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/utils/ptr"

	"github.com/telepresenceio/clog/testutil"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/auth"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/mutator"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/state"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func TestStoredRoutePodProofRequiresEstablishedAndExactRunningContainer(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	healthy := routeActivationPod("healthy", "healthy-uid")
	terminating := routeActivationPod("terminating", "terminating-uid")
	terminating.DeletionTimestamp = ptr.To(metav1.Now())
	terminating.Finalizers = []string{"test/retain"}
	terminating.Status.Conditions[0].Status = core.ConditionFalse
	fresh := routeActivationPod("fresh", "fresh-uid")
	conn, mgr, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t, []runtime.Object{healthy, terminating, fresh})
	t.Cleanup(func() { _ = conn.Close() })
	svc := mgr.(*service)
	process, other := uuid.NewString(), uuid.NewString()
	prefix := func(pod *core.Pod) string {
		t.Helper()
		value, _, err := runningRouteAgentContainer(pod)
		require.NoError(t, err)
		return value
	}
	healthyProof, terminatingProof := prefix(healthy)+process, prefix(terminating)+process
	acks := map[string]bool{healthyProof: true, terminatingProof: true, "pod:fresh-uid": true, prefix(fresh) + "malformed": true}
	lookup := func(pod *core.Pod, established, want bool, expected string) {
		t.Helper()
		consumer, ok := svc.acknowledgedRoutePod(ctx, pod.Namespace, pod.Name, string(pod.UID), acks, established)
		require.Equal(t, want, ok)
		if want {
			require.Equal(t, expected, consumer)
		}
	}
	lookup(healthy, false, false, "")
	lookup(terminating, false, false, "")
	lookup(healthy, true, true, healthyProof)
	lookup(terminating, true, true, terminatingProof)
	lookup(fresh, true, false, "")
	_, ok := svc.acknowledgedRoutePod(ctx, healthy.Namespace, healthy.Name, "different-uid", acks, true)
	require.False(t, ok)
	_, ok = svc.acknowledgedRoutePod(ctx, healthy.Namespace, "", string(healthy.UID), acks, true)
	require.False(t, ok)
	acks[prefix(fresh)+other] = false
	lookup(fresh, true, false, "")
	acks[prefix(fresh)+other] = true
	lookup(fresh, true, true, prefix(fresh)+other)
	acks[prefix(fresh)+process] = true
	lookup(fresh, true, false, "") // two distinct valid process claims for one container are ambiguous
	delete(acks, prefix(fresh)+process)
	kube := k8sapi.GetK8sInterface(ctx)
	healthy.Status.ContainerStatuses[0].RestartCount++
	healthy.Status.ContainerStatuses[0].ContainerID = "containerd://replacement"
	_, err := kube.CoreV1().Pods(healthy.Namespace).UpdateStatus(ctx, healthy, metav1.UpdateOptions{})
	require.NoError(t, err)
	lookup(healthy, true, false, "") // current Kubernetes container cannot inherit the retired process evidence
	newProof := prefix(healthy) + other
	acks[newProof] = true
	lookup(healthy, true, true, newProof)
	healthy.Status.ContainerStatuses[0].State = core.ContainerState{Waiting: &core.ContainerStateWaiting{Reason: "restarting"}}
	_, err = kube.CoreV1().Pods(healthy.Namespace).UpdateStatus(ctx, healthy, metav1.UpdateOptions{})
	require.NoError(t, err)
	lookup(healthy, true, false, "")
}

func TestRecordedCurrentRouteAgentPreventsFallbackToAnotherProcess(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	pod := routeActivationPod("agent", "agent-uid")
	conn, mgr, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t, []runtime.Object{pod})
	t.Cleanup(func() { _ = conn.Close() })
	svc := mgr.(*service)
	older, newer := uuid.NewString(), uuid.NewString()
	prefix, _, err := runningRouteAgentContainer(pod)
	require.NoError(t, err)
	oldProof, newProof := prefix+older, prefix+newer
	acks := map[string]bool{oldProof: true}
	principal := &auth.Principal{Username: "system:serviceaccount:default:web", PodName: pod.Name, PodUID: string(pod.UID)}
	agent := &rpc.AgentInfo{
		Name: "web", Kind: "Deployment", Namespace: pod.Namespace, PodName: pod.Name, PodUid: string(pod.UID), PodIp: "10.0.0.1",
		RouteGuardInstance: newer, RouteGuardStartedAt: timestamppb.New(time.Now().Add(-time.Minute)),
	}
	_, err = svc.state.RestoreAgent(ctx, "agent:agent-uid", agent, principal, time.Now())
	require.NoError(t, err)
	for _, established := range []bool{false, true} {
		_, ok := svc.acknowledgedRoutePod(ctx, pod.Namespace, pod.Name, string(pod.UID), acks, established)
		require.False(t, ok, "a recorded new process cannot use old evidence even if the Kubernetes container is unchanged")
	}
	mutator.GetMap(ctx).Inactivate(pod.UID)
	require.Nil(t, svc.state.GetAgent("agent:agent-uid"), "ordinary session lookup hides an explicitly inactive Pod")
	require.Len(t, svc.state.RouteAgentSessionsForPod(string(pod.UID)), 1)
	_, ok := svc.acknowledgedRoutePod(ctx, pod.Namespace, pod.Name, string(pod.UID), acks, true)
	require.False(t, ok, "a recorded session stays authoritative even when normal lookup filters it")
	acks[newProof] = true
	consumer, ok := svc.acknowledgedRoutePod(ctx, pod.Namespace, pod.Name, string(pod.UID), acks, true)
	require.True(t, ok)
	require.Equal(t, newProof, consumer)
}

func TestLatestRouteAgentSessionRejectsAmbiguityAndFencesOlderSession(t *testing.T) {
	now := time.Now()
	makeSession := func(process string, at time.Time) *state.AgentSession {
		return &state.AgentSession{AgentInfo: &rpc.AgentInfo{PodName: "pod", RouteGuardInstance: process, RouteGuardStartedAt: timestamppb.New(at)}}
	}
	old := makeSession(uuid.NewString(), now.Add(-time.Minute))
	newer := makeSession(uuid.NewString(), now)
	require.Same(t, newer, latestRouteAgentSession([]*state.AgentSession{old, newer}))
	require.Same(t, newer, latestRouteAgentSession([]*state.AgentSession{newer, old}))
	require.Nil(t, latestRouteAgentSession([]*state.AgentSession{newer, makeSession(uuid.NewString(), now)}))
	require.Nil(t, latestRouteAgentSession([]*state.AgentSession{old, makeSession("", now)}))
	unknown := makeSession(uuid.NewString(), now)
	unknown.RouteGuardStartedAt = nil
	require.Nil(t, latestRouteAgentSession([]*state.AgentSession{old, unknown}))
}
