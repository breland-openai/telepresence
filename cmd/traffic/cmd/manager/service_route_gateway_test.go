package manager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/utils/ptr"

	"github.com/telepresenceio/clog/testutil"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/auth"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/routeintent"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

func TestGatewayTopologyRequiresFreshProofAfterUnsupportedAndNewWorkloads(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	const controller = "system:serviceaccount:ambassador:gateway"
	web := gatewayTestDeployment("web", "shared", 8080)
	kubeService := &core.Service{ObjectMeta: meta.ObjectMeta{Name: "shared", Namespace: "default", UID: "service-uid"}, Spec: core.ServiceSpec{
		Selector: map[string]string{"app": "shared"}, Ports: []core.ServicePort{{Port: 80, Protocol: core.ProtocolTCP, TargetPort: intstr.FromString("app-port"), AppProtocol: ptr.To("http")}},
	}}
	webPod := gatewayTestPod("web-one", "web", "shared")
	conn, mgr, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t, []runtime.Object{kubeService, web, webPod},
		func(env *managerutil.Env) { env.EnabledWorkloadKinds = k8sapi.Kinds{k8sapi.DeploymentKind} })
	t.Cleanup(func() { _ = conn.Close() })
	svc := mgr.(*service)
	_, store := newManagerRouteStore()
	svc.routeIntents = newRouteIntentManager(store, []string{controller})
	_, _, owner := managerRouteIdentity(t)
	key := routeintent.Key{Namespace: "default", Owner: owner, Name: "route"}
	predicate := routeintent.Predicate{Namespace: "default", WorkloadKind: "Deployment", Workload: "web", ContainerPort: 8080, Service: "shared", ServiceUID: "service-uid", ServicePort: 80, RoutingKey: "key"}
	record, err := store.Create(ctx, key, "one", predicate, time.Time{})
	require.NoError(t, err)
	ackAgent := func(pod, workload string, containerPort int32) {
		t.Helper()
		agent := &rpc.AgentInfo{
			Name: workload, Kind: "Deployment", Namespace: "default", PodName: pod, PodUid: pod, PodIp: "10.0.0.1", RouteGuardInstance: "process-" + pod, RouteGuardStartedAt: timestamppb.New(time.Now().Add(-time.Minute)),
			InterceptTargets: []*rpc.AgentInfo_InterceptTarget{{ContainerPort: containerPort, AgentPort: 9900, Protocol: "TCP", AppProtocol: "http", ServiceUid: "service-uid", ServicePort: 80}},
		}
		sid := tunnel.SessionID("agent:" + pod)
		_, e := svc.state.RestoreAgent(ctx, sid, agent, &auth.Principal{Username: "system:serviceaccount:default:web", PodName: pod, PodUID: pod}, time.Now())
		require.NoError(t, e)
		consumer, e := routeAgentConsumer(ctx, svc.state.GetAgent(sid))
		require.NoError(t, e)
		require.NoError(t, store.Acknowledge(ctx, key, record.Revision, consumer))
	}
	currentToken := func(want string, same bool) string {
		t.Helper()
		var token string
		require.Eventually(t, func() bool {
			var e error
			token, e = svc.gatewayRouteToken(ctx, record)
			return e == nil && ((same && token == want) || (!same && token != "" && token != want))
		}, 5*time.Second, 15*time.Millisecond)
		return token
	}
	proof := func(wantAgents, wantGateway bool) {
		t.Helper()
		require.Eventually(t, func() bool {
			agents, gateway, e := svc.routeActivationEvidence(ctx, record)
			return e == nil && agents == wantAgents && gateway == wantGateway
		}, 5*time.Second, 15*time.Millisecond)
	}
	ackGateway := func(token string) {
		t.Helper()
		req := &rpc.RouteIntentAckRequest{Applied: []*rpc.RouteIntentAck{{InterceptId: durableRouteInterceptID(record), Incarnation: "one", Revision: 1, GatewayTopology: token}}}
		_, e := svc.AcknowledgeRouteIntents(auth.WithPrincipal(ctx, &auth.Principal{Username: controller}), req)
		require.NoError(t, e)
	}
	ackAgent("web-one", "web", 8080)
	initial := currentToken("", false)
	proof(true, false)
	ackGateway(initial)
	proof(true, true)
	svc.routeIntents = newRouteIntentManager(store, []string{controller})
	require.Equal(t, initial, currentToken(initial, true), "unchanged manager restart keeps its persistent gateway generation")
	proof(true, true)
	kube := k8sapi.GetK8sInterface(ctx)
	setProtocol := func(protocol string) {
		t.Helper()
		latest, e := kube.CoreV1().Services("default").Get(ctx, "shared", meta.GetOptions{})
		require.NoError(t, e)
		latest.Spec.Ports[0].AppProtocol = ptr.To(protocol)
		_, e = kube.CoreV1().Services("default").Update(ctx, latest, meta.UpdateOptions{})
		require.NoError(t, e)
	}
	setProtocol("h2c")
	currentToken("", true)
	proof(true, false)
	ackGateway(initial)
	proof(true, false)
	setProtocol("http")
	restored := currentToken(initial, false)
	proof(true, false)
	ackGateway(initial)
	proof(true, false)
	ackGateway(restored)
	proof(true, true)
	_, err = kube.CoreV1().Pods("default").Create(ctx, gatewayTestPod("web-two", "web", "shared"), meta.CreateOptions{})
	require.NoError(t, err)
	ackAgent("web-two", "web", 8080)
	require.Equal(t, restored, currentToken(restored, true), "agent-only addition/replacement does not alter the target mapping")
	proof(true, true)
	_, err = kube.AppsV1().Deployments("default").Create(ctx, gatewayTestDeployment("canary", "shared", 8081), meta.CreateOptions{})
	require.NoError(t, err)
	_, err = kube.CoreV1().Pods("default").Create(ctx, gatewayTestPod("canary-one", "canary", "shared"), meta.CreateOptions{})
	require.NoError(t, err)
	ackAgent("canary-one", "canary", 8081)
	expanded := currentToken(restored, false)
	proof(true, false)
	ackGateway(restored)
	proof(true, false)
	ackGateway(expanded)
	proof(true, true)
}

func gatewayTestPod(name, workload, label string) *core.Pod {
	pod := routeActivationPod(name, name)
	pod.Labels = map[string]string{"app": label}
	pod.OwnerReferences = []meta.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: workload, UID: types.UID(workload + "-uid"), Controller: ptr.To(true)}}
	return pod
}

func TestGatewayTopologyHashIsOrderedAndIncludesBothPortsAndProtocol(t *testing.T) {
	a := gatewayTopologyTarget{"ns", "Deployment", "a", 8080, 9900, "http"}
	b := gatewayTopologyTarget{"ns", "StatefulSet", "b", 8081, 9901, "ws"}
	initial := hashGatewayTopology([]gatewayTopologyTarget{a, b})
	require.Equal(t, initial, hashGatewayTopology([]gatewayTopologyTarget{b, a, a}))
	for _, mutate := range []func(*gatewayTopologyTarget){
		func(c *gatewayTopologyTarget) { c.protocol = "h2c" }, func(c *gatewayTopologyTarget) { c.agent++ },
		func(c *gatewayTopologyTarget) { c.container++ }, func(c *gatewayTopologyTarget) { c.name += "-new" },
	} {
		modified := a
		mutate(&modified)
		require.NotEqual(t, initial, hashGatewayTopology([]gatewayTopologyTarget{modified, b}))
	}
}
