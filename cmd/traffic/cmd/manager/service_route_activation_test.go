package manager

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/utils/ptr"

	"github.com/telepresenceio/clog/testutil"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/auth"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/routeintent"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/state"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

func routeActivationPod(name, uid string) *core.Pod {
	return &core.Pod{ObjectMeta: meta.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(uid)}, Status: core.PodStatus{
		Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}},
		ContainerStatuses: []core.ContainerStatus{{
			Name: agentconfig.ContainerName, ContainerID: "containerd://" + uid + "-1",
			State: core.ContainerState{Running: &core.ContainerStateRunning{StartedAt: meta.NewTime(time.Now().Add(-time.Minute))}},
		}},
	}}
}

func TestRouteActivationWaitsForSlowRoutablePodAndGatewayAcrossRestart(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	const controller = "system:serviceaccount:ambassador:gateway"
	kubeService := &core.Service{ObjectMeta: meta.ObjectMeta{Name: "web", Namespace: "default", UID: "service-uid"}, Spec: core.ServiceSpec{Selector: map[string]string{"app": "web"}, Ports: []core.ServicePort{{Port: 80, Protocol: core.ProtocolTCP, AppProtocol: ptr.To("http")}}}}
	endpoint := func(uid string, ready, serving bool) discovery.Endpoint {
		return discovery.Endpoint{Addresses: []string{"10.0.0.1"}, TargetRef: &core.ObjectReference{Kind: "Pod", Name: uid, Namespace: "default", UID: types.UID(uid)}, Conditions: discovery.EndpointConditions{Ready: ptr.To(ready), Serving: ptr.To(serving)}}
	}
	slice := &discovery.EndpointSlice{ObjectMeta: meta.ObjectMeta{Name: "web-endpoints", Namespace: "default", Labels: map[string]string{discovery.LabelServiceName: "web"}}, AddressType: discovery.AddressTypeIPv4, Endpoints: []discovery.Endpoint{endpoint("fast", true, true), endpoint("slow", true, true), endpoint("not-routable", false, false)}}
	objects := []runtime.Object{kubeService, slice}
	deployment := gatewayTestDeployment("web", "web", 8080)
	objects = append(objects, deployment)
	for _, uid := range []string{"fast", "slow", "joining", "not-routable", "replacement", "old-terminating"} {
		pod := routeActivationPod(uid, uid)
		if uid == "fast" || uid == "slow" || uid == "joining" {
			pod.Labels = map[string]string{"app": "web"}
			pod.OwnerReferences = []meta.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "web", UID: deployment.UID, Controller: ptr.To(true)}}
		}
		if uid == "joining" {
			pod.Status.Phase = core.PodPending
			pod.Status.Conditions[0].Status = core.ConditionFalse
		}
		objects = append(objects, pod)
	}
	conn, mgr, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t, objects,
		func(env *managerutil.Env) { env.EnabledWorkloadKinds = k8sapi.Kinds{k8sapi.DeploymentKind} })
	t.Cleanup(func() { _ = conn.Close() })
	svc := mgr.(*service)
	agents := make(map[string]*state.AgentSession)
	for _, uid := range []string{"fast", "slow", "joining", "not-routable", "replacement", "old-terminating"} {
		sid := tunnel.SessionID("agent:" + uid)
		info := &rpc.AgentInfo{Name: "web", Kind: "Deployment", Namespace: "default", PodName: uid, PodUid: uid, PodIp: "10.0.0.1", RouteGuardInstance: "process-" + uid, RouteGuardStartedAt: timestamppb.New(time.Now().Add(-time.Minute))}
		info.InterceptTargets = []*rpc.AgentInfo_InterceptTarget{{ContainerPort: 8080, AgentPort: 9900, Protocol: "TCP", AppProtocol: "http", ServiceUid: "service-uid", ServicePort: 80}}
		_, err := svc.state.RestoreAgent(ctx, sid, info, &auth.Principal{Username: "system:serviceaccount:default:web", PodName: uid, PodUID: uid}, time.Now())
		require.NoError(t, err)
		agents[uid] = svc.state.GetAgent(sid)
	}
	_, store := newManagerRouteStore()
	svc.routeIntents = newRouteIntentManager(store, []string{controller})
	_, _, owner := managerRouteIdentity(t)
	key := routeintent.Key{Namespace: "default", Owner: owner, Name: "route"}
	predicate := routeintent.Predicate{Namespace: "default", WorkloadKind: "Deployment", Workload: "web", ContainerPort: 8080, Service: "web", ServiceUID: "service-uid", ServicePort: 80, RoutingKey: "key"}
	record, err := store.Create(ctx, key, "one", predicate, time.Time{})
	require.NoError(t, err)
	ack := func(uid string) {
		t.Helper()
		consumer, err := routeAgentConsumer(ctx, agents[uid])
		require.NoError(t, err)
		require.NoError(t, store.Acknowledge(ctx, key, record.Revision, consumer))
	}
	check := func(wantAgent, wantGateway bool) {
		t.Helper()
		agents, gateway, err := svc.routeActivationEvidence(ctx, record)
		require.NoError(t, err)
		require.Equal(t, wantAgent, agents)
		require.Equal(t, wantGateway, gateway)
	}
	check(false, false)
	ack("fast")
	topology, err := svc.gatewayRouteToken(ctx, record)
	require.NoError(t, err)
	require.NotEmpty(t, topology)
	require.NoError(t, store.AcknowledgeGateway(ctx, key, record.Revision, "gateway:"+controller, topology))
	check(false, true)
	svc.routeIntents = newRouteIntentManager(store, []string{controller})
	check(false, true)
	ack("not-routable")
	check(false, true)
	ack("slow")
	check(false, true)
	ack("joining")
	check(true, true)
	client := k8sapi.GetK8sInterface(ctx)
	slice, err = client.DiscoveryV1().EndpointSlices("default").Get(ctx, "web-endpoints", meta.GetOptions{})
	require.NoError(t, err)
	slice.Endpoints = []discovery.Endpoint{endpoint("fast", true, true), endpoint("replacement", true, true), endpoint("old-terminating", false, true)}
	_, err = client.DiscoveryV1().EndpointSlices("default").Update(ctx, slice, meta.UpdateOptions{})
	require.NoError(t, err)
	check(false, true)
	ack("replacement")
	check(false, true)
	ack("old-terminating")
	check(true, true)
	pod, err := client.CoreV1().Pods("default").Get(ctx, "fast", meta.GetOptions{})
	require.NoError(t, err)
	pod.Status.ContainerStatuses[0].RestartCount++
	pod.Status.ContainerStatuses[0].ContainerID = "containerd://fast-2"
	_, err = client.CoreV1().Pods("default").UpdateStatus(ctx, pod, meta.UpdateOptions{})
	require.NoError(t, err)
	check(false, true)
	fastOld := proto.CloneOf(agents["fast"].AgentInfo)
	fastNew := proto.CloneOf(fastOld)
	fastNew.RouteGuardInstance = "process-fast-restarted"
	fastNew.RouteGuardStartedAt = timestamppb.Now()
	fastPrincipal := &auth.Principal{Username: "system:serviceaccount:default:web", PodName: "fast", PodUID: "fast"}
	_, err = svc.state.RestoreAgent(ctx, "agent:fast", fastNew, fastPrincipal, time.Now())
	require.NoError(t, err)
	agents["fast"] = svc.state.GetAgent("agent:fast")
	require.Equal(t, fastNew.RouteGuardInstance, agents["fast"].RouteGuardInstance)
	_, err = svc.state.RestoreAgent(ctx, "agent:fast", fastOld, fastPrincipal, time.Now())
	require.NoError(t, err)
	require.Equal(t, fastNew.RouteGuardInstance, svc.state.GetAgent("agent:fast").RouteGuardInstance, "late previous process arrival must not replace the current guard identity")
	check(false, true)
	ack("fast")
	check(true, true)
	slice.Endpoints[0].TargetRef = nil
	_, err = client.DiscoveryV1().EndpointSlices("default").Update(ctx, slice, meta.UpdateOptions{})
	require.NoError(t, err)
	_, _, err = svc.routeActivationEvidence(ctx, record)
	require.ErrorContains(t, err, "no verifiable Pod UID")
}

func TestEstablishedRouteIgnoresOnlyNonservingPodsAndReestablishesTopology(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	const controller = "system:serviceaccount:ambassador:gateway"
	deployment := gatewayTestDeployment("web", "web", 8080)
	web := &core.Service{ObjectMeta: meta.ObjectMeta{Name: "web", Namespace: "default", UID: "service-uid"}, Spec: core.ServiceSpec{
		Selector: map[string]string{"app": "web"}, Ports: []core.ServicePort{{Port: 80, Protocol: core.ProtocolTCP, AppProtocol: ptr.To("http")}},
	}}
	endpoint := func(uid string, ready, serving *bool) discovery.Endpoint {
		return discovery.Endpoint{
			Addresses: []string{"10.0.0.1"}, TargetRef: &core.ObjectReference{Kind: "Pod", Namespace: "default", UID: types.UID(uid)},
			Conditions: discovery.EndpointConditions{Ready: ready, Serving: serving},
		}
	}
	yes, no := ptr.To(true), ptr.To(false)
	slice := &discovery.EndpointSlice{
		ObjectMeta:  meta.ObjectMeta{Name: "web", Namespace: "default", Labels: map[string]string{discovery.LabelServiceName: "web"}},
		AddressType: discovery.AddressTypeIPv4, Endpoints: []discovery.Endpoint{endpoint("healthy", yes, yes)},
	}
	healthy := gatewayTestPod("healthy", "web", "web")
	initialCold := gatewayTestPod("initial-cold", "web", "web")
	initialCold.Status.Conditions[0].Status = core.ConditionFalse
	conn, mgr, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t, []runtime.Object{web, deployment, slice, healthy, initialCold},
		func(env *managerutil.Env) { env.EnabledWorkloadKinds = k8sapi.Kinds{k8sapi.DeploymentKind} })
	t.Cleanup(func() { _ = conn.Close() })
	svc := mgr.(*service)
	_, store := newManagerRouteStore()
	svc.routeIntents = newRouteIntentManager(store, []string{controller})
	_, _, owner := managerRouteIdentity(t)
	key := routeintent.Key{Namespace: "default", Owner: owner, Name: "route"}
	predicate := routeintent.Predicate{
		Namespace: "default", WorkloadKind: "Deployment", Workload: "web", ContainerPort: 8080,
		Service: "web", ServiceUID: "service-uid", ServicePort: 80, RoutingKey: "key",
	}
	record, err := store.Create(ctx, key, "one", predicate, time.Time{})
	require.NoError(t, err)
	ackAgent := func(uid string) {
		t.Helper()
		sid := tunnel.SessionID("agent:" + uid)
		info := &rpc.AgentInfo{
			Name: "web", Kind: "Deployment", Namespace: "default", PodName: uid, PodUid: uid, PodIp: "10.0.0.1",
			RouteGuardInstance: uuid.NewSHA1(uuid.NameSpaceDNS, []byte(uid)).String(), RouteGuardStartedAt: timestamppb.New(time.Now().Add(-time.Minute)),
			InterceptTargets: []*rpc.AgentInfo_InterceptTarget{{ContainerPort: 8080, AgentPort: 9900, Protocol: "TCP", AppProtocol: "http", ServiceUid: "service-uid", ServicePort: 80}},
		}
		_, e := svc.state.RestoreAgent(ctx, sid, info, &auth.Principal{Username: "system:serviceaccount:default:web", PodName: uid, PodUID: uid}, time.Now())
		require.NoError(t, e)
		consumer, e := routeAgentConsumer(ctx, svc.state.GetAgent(sid))
		require.NoError(t, e)
		require.NoError(t, store.Acknowledge(ctx, key, record.Revision, consumer))
	}
	proof := func(wantAgent, wantGateway bool) {
		t.Helper()
		require.Eventually(t, func() bool {
			agents, gateway, e := svc.routeActivationEvidence(ctx, record)
			return e == nil && agents == wantAgent && gateway == wantGateway
		}, 5*time.Second, 15*time.Millisecond)
	}
	ackAgent("healthy")
	topology, err := svc.gatewayRouteToken(ctx, record)
	require.NoError(t, err)
	require.NotEmpty(t, topology)
	require.NoError(t, store.AcknowledgeGateway(ctx, key, record.Revision, "gateway:"+controller, topology))
	proof(false, true)
	svc.routeIntents = newRouteIntentManager(store, []string{controller})
	proof(false, true) // a restart must not mistake a never-proven initial route for established
	ackAgent("initial-cold")
	proof(true, true)
	_, established, err := store.ActivationEvidence(ctx, key, record.Revision, topology)
	require.NoError(t, err)
	require.True(t, established)
	kube := k8sapi.GetK8sInterface(ctx)
	newCold := gatewayTestPod("new-cold", "web", "web")
	newCold.Status.Conditions[0].Status = core.ConditionFalse
	newCold.Status.Phase = core.PodPending
	_, err = kube.CoreV1().Pods("default").Create(ctx, newCold, meta.CreateOptions{})
	require.NoError(t, err)
	drained := gatewayTestPod("old-drained", "web", "web")
	drained.Status.Conditions[0].Status = core.ConditionFalse
	drained.DeletionTimestamp = ptr.To(meta.Now())
	drained.Finalizers = []string{"test/keep"}
	_, err = kube.CoreV1().Pods("default").Create(ctx, drained, meta.CreateOptions{})
	require.NoError(t, err)
	setEndpoints := func(extra discovery.Endpoint) {
		t.Helper()
		current, e := kube.DiscoveryV1().EndpointSlices("default").Get(ctx, "web", meta.GetOptions{})
		require.NoError(t, e)
		current.Endpoints = []discovery.Endpoint{endpoint("healthy", yes, yes), extra}
		_, e = kube.DiscoveryV1().EndpointSlices("default").Update(ctx, current, meta.UpdateOptions{})
		require.NoError(t, e)
	}
	setEndpoints(endpoint("old-drained", no, no))
	proof(true, true)
	svc.routeIntents = newRouteIntentManager(store, []string{controller})
	proof(true, true) // persisted initial proof survives restart with both excluded Pods still unproven
	setEndpoints(endpoint("old-drained", no, yes))
	proof(false, true) // a terminating endpoint that is still Serving remains mandatory
	setEndpoints(endpoint("new-cold", yes, no))
	proof(false, true) // publishNotReadyAddresses can expose an unready Pod as Ready
	setEndpoints(endpoint("new-cold", nil, no))
	proof(false, true) // an unknown endpoint Ready condition remains conservative
	setEndpoints(endpoint("new-cold", no, no))
	proof(true, true)
	newCold, err = kube.CoreV1().Pods("default").Get(ctx, "new-cold", meta.GetOptions{})
	require.NoError(t, err)
	newCold.Status.Conditions[0].Status = core.ConditionTrue
	_, err = kube.CoreV1().Pods("default").UpdateStatus(ctx, newCold, meta.UpdateOptions{})
	require.NoError(t, err)
	proof(false, true) // Pod Ready independently of its not-ready EndpointSlice also remains mandatory
	newCold.Status.Conditions[0].Status = core.ConditionFalse
	_, err = kube.CoreV1().Pods("default").UpdateStatus(ctx, newCold, meta.UpdateOptions{})
	require.NoError(t, err)
	proof(true, true)
	setProtocol := func(protocol string) {
		t.Helper()
		current, e := kube.CoreV1().Services("default").Get(ctx, "web", meta.GetOptions{})
		require.NoError(t, e)
		current.Spec.Ports[0].AppProtocol = ptr.To(protocol)
		_, e = kube.CoreV1().Services("default").Update(ctx, current, meta.UpdateOptions{})
		require.NoError(t, e)
	}
	setProtocol("h2c")
	proof(false, false)
	setProtocol("http")
	var restoredTopology string
	require.Eventually(t, func() bool {
		restoredTopology, err = svc.gatewayRouteToken(ctx, record)
		return err == nil && restoredTopology != "" && restoredTopology != topology
	}, 5*time.Second, 15*time.Millisecond)
	require.NoError(t, store.AcknowledgeGateway(ctx, key, record.Revision, "gateway:"+controller, restoredTopology))
	proof(false, true) // a changed/restored gateway topology must first prove every selected Pod again
	ackAgent("new-cold")
	ackAgent("old-drained")
	proof(true, true)
	svc.state.RemoveSession(ctx, "agent:healthy")
	proof(true, true) // the exact established healthy Running container keeps its installed guard after manager session loss
	svc.routeIntents = newRouteIntentManager(store, []string{controller})
	proof(true, true) // reloading persistent route evidence does not make the same disconnected process unsafe
}

func TestRouteAcknowledgmentAuthenticatesPodAndIgnoresOldVersions(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	conn, mgr, ctx := getTestClientConnAndService(testutil.NewContext(t, false), t, []runtime.Object{routeActivationPod("web-pod", "pod-uid")})
	t.Cleanup(func() { _ = conn.Close() })
	svc := mgr.(*service)
	_, store := newManagerRouteStore()
	const controller = "system:serviceaccount:ambassador:gateway"
	svc.routeIntents = newRouteIntentManager(store, []string{controller})
	_, _, owner := managerRouteIdentity(t)
	key := routeintent.Key{Namespace: "default", Owner: owner, Name: "route"}
	predicate := routeintent.Predicate{Namespace: "default", WorkloadKind: "Deployment", Workload: "web", ContainerPort: 8080, RoutingKey: "key"}
	record, err := store.Create(ctx, key, "one", predicate, time.Time{})
	require.NoError(t, err)
	id := durableRouteInterceptID(record)
	agent := &rpc.AgentInfo{Name: "web", Kind: "Deployment", Namespace: "default", PodName: "web-pod", PodUid: "pod-uid", PodIp: "10.0.0.1", RouteGuardInstance: "process-web", RouteGuardStartedAt: timestamppb.New(time.Now().Add(-time.Minute))}
	agent.InterceptTargets = []*rpc.AgentInfo_InterceptTarget{{ContainerPort: 8080, AgentPort: 9900, Protocol: "TCP", AppProtocol: "http"}}
	const sid tunnel.SessionID = "agent:pod-uid"
	_, err = svc.state.RestoreAgent(ctx, sid, agent, nil, time.Now())
	require.NoError(t, err)
	request := &rpc.RouteIntentAckRequest{Session: &rpc.SessionInfo{SessionId: string(sid)}, Applied: []*rpc.RouteIntentAck{{InterceptId: id, Incarnation: "one", Revision: 1}}, AgentGuardInstance: "process-web", AppliedAt: timestamppb.Now()}
	_, err = svc.AcknowledgeRouteIntents(ctx, request)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	wrong := auth.WithPrincipal(ctx, &auth.Principal{Username: "system:serviceaccount:default:web", PodName: agent.PodName, PodUID: "other-uid"})
	_, err = svc.AcknowledgeRouteIntents(wrong, request)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	bound := auth.WithPrincipal(ctx, &auth.Principal{Username: "system:serviceaccount:default:web", PodName: agent.PodName, PodUID: agent.PodUid})
	request.AppliedAt = timestamppb.New(time.Now().Add(-2 * time.Minute))
	_, err = svc.AcknowledgeRouteIntents(bound, request)
	require.Equal(t, codes.Unavailable, status.Code(err))
	request.AppliedAt = timestamppb.Now()
	request.AgentGuardInstance = "old-process"
	_, err = svc.AcknowledgeRouteIntents(bound, request)
	require.NoError(t, err)
	before, err := store.Acknowledgments(ctx, key, record.Revision)
	require.NoError(t, err)
	require.Empty(t, before)
	request.AgentGuardInstance = "process-web"
	_, err = svc.AcknowledgeRouteIntents(bound, request)
	require.NoError(t, err)
	request.Applied[0].Incarnation = "old"
	_, err = svc.AcknowledgeRouteIntents(bound, request)
	require.NoError(t, err)
	request.Session = nil
	request.Applied[0].Incarnation = "one"
	_, err = svc.AcknowledgeRouteIntents(bound, request)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	topology, err := svc.gatewayRouteToken(ctx, record)
	require.NoError(t, err)
	require.NotEmpty(t, topology)
	controllerContext := auth.WithPrincipal(ctx, &auth.Principal{Username: controller})
	_, err = svc.AcknowledgeRouteIntents(controllerContext, request)
	require.NoError(t, err)
	request.Applied[0].GatewayTopology = "stale"
	_, err = svc.AcknowledgeRouteIntents(controllerContext, request)
	require.NoError(t, err)
	before, err = store.Acknowledgments(ctx, key, record.Revision)
	require.NoError(t, err)
	require.Len(t, before, 1)
	request.Applied[0].GatewayTopology = topology
	_, err = svc.AcknowledgeRouteIntents(controllerContext, request)
	require.NoError(t, err)
	acks, err := store.Acknowledgments(ctx, key, record.Revision)
	require.NoError(t, err)
	consumer, err := routeAgentConsumer(ctx, svc.state.GetAgent(sid))
	require.NoError(t, err)
	require.Equal(t, map[string]bool{consumer: true, "gateway:" + controller + ":" + topology: true}, acks)
}

func gatewayTestDeployment(name, label string, port int32) *apps.Deployment {
	return &apps.Deployment{ObjectMeta: meta.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name + "-uid")}, Spec: apps.DeploymentSpec{
		Selector: &meta.LabelSelector{MatchLabels: map[string]string{"app": label}},
		Template: core.PodTemplateSpec{ObjectMeta: meta.ObjectMeta{Labels: map[string]string{"app": label}}, Spec: core.PodSpec{Containers: []core.Container{{
			Name: "app", Ports: []core.ContainerPort{{Name: "app-port", ContainerPort: port, Protocol: core.ProtocolTCP}},
		}}}},
	}}
}
