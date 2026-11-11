package manager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"

	"github.com/telepresenceio/clog/testutil"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/mutator"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func TestReconnectClientRestoresVerifiedSharedServiceThroughRPC(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx, cancel := context.WithTimeout(testutil.NewContext(t, true), 15*time.Second)
	defer cancel()
	ctx = managerutil.WithResolvedAgentImageRetriever(ctx, managerutil.ImageFromEnv("ghcr.io/telepresenceio/tel2:2.32.0"))

	const namespace = "default"
	const serviceName = "shared-service"
	const serviceUID = "shared-service-uid"
	const stableName = "stable"
	const canaryName = "canary"
	const outsiderName = "outsider"
	deployment := func(name, app string) *appsv1.Deployment {
		labels := map[string]string{"app": app, "workload": name}
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name: "app", Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
					}}},
				},
			},
		}
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespace, UID: k8stypes.UID(serviceUID)},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "shared"},
			Ports: []corev1.ServicePort{{
				Name: "http", Port: 80, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP,
			}},
		},
	}
	conn, mgr, sctx := getTestClientConnAndService(ctx, t, []runtime.Object{
		service, deployment(stableName, "shared"), deployment(canaryName, "shared"), deployment(outsiderName, "different"),
	}, func(e *managerutil.Env) { e.EnabledWorkloadKinds = k8sapi.Kinds{k8sapi.DeploymentKind} })
	defer conn.Close()
	client := rpc.NewManagerClient(conn)
	mutator.GetMap(sctx).Store(&agentconfig.Sidecar{
		AgentName: canaryName, WorkloadName: canaryName, Namespace: namespace, WorkloadKind: k8sapi.DeploymentKind,
		Containers: []*agentconfig.Container{{Name: "app", Intercepts: []*agentconfig.Intercept{{
			ServiceName: serviceName, ServiceUID: k8stypes.UID(serviceUID), ServicePortName: "http", ServicePort: 80,
			Protocol: types.ProtoTCP, ContainerPortName: "http", ContainerPort: 8080, AgentPort: 9900,
		}}}},
	})

	agent := func(name, ip string) *rpc.AgentInfo {
		return &rpc.AgentInfo{
			Name: name, Kind: string(k8sapi.DeploymentKind), Namespace: namespace,
			PodName: name + "-pod", PodUid: name + "-pod-uid", PodIp: ip, Product: "telepresence-agent", Version: "2.32.0",
			Mechanisms: []*rpc.AgentInfo_Mechanism{{Name: "http", Product: "telepresence", Version: "2.32.0"}},
			Containers: map[string]*rpc.AgentInfo_ContainerInfo{"app": {}},
			InterceptTargets: []*rpc.AgentInfo_InterceptTarget{{
				ServiceUid: serviceUID, ServiceName: serviceName, ServicePortName: "http", ServicePort: 80,
				Protocol: "TCP", ContainerName: "app", ContainerPort: 8080,
			}},
		}
	}
	stable := agent(stableName, "10.0.0.1")
	canary := agent(canaryName, "10.0.0.2")
	outsider := agent(outsiderName, "10.0.0.3")
	stableSession, err := client.ArriveAsAgent(ctx, stable)
	require.NoError(t, err)
	canarySession, err := client.ArriveAsAgent(ctx, canary)
	require.NoError(t, err)
	outsiderSession, err := client.ArriveAsAgent(ctx, outsider)
	require.NoError(t, err)

	const interceptName = "shared-intercept"
	const sessionID = "lost-next5-session"
	const interceptID = sessionID + ":" + interceptName
	clientInfo := &rpc.ClientInfo{Name: "alice", Namespace: namespace, InstallId: "alice-install", Product: "telepresence", Version: "2.32.0"}
	forged := &rpc.InterceptInfo{
		Id: "forged-id", ClientSession: &rpc.SessionInfo{SessionId: "another-client"},
		Disposition: rpc.InterceptDispositionType_ACTIVE, PodName: outsider.PodName, PodIp: outsider.PodIp,
		Message: "forged approval", Environment: map[string]string{"SOURCE": "forged"},
		ServiceWorkloads: []*rpc.InterceptWorkload{{
			Namespace: namespace, WorkloadKind: string(k8sapi.DeploymentKind), WorkloadName: outsiderName,
		}},
		Spec: &rpc.InterceptSpec{
			Name: interceptName, Client: clientInfo.Name, Agent: stableName, WorkloadKind: string(k8sapi.DeploymentKind),
			Namespace: namespace, Mechanism: "http", ServiceUid: serviceUID, ServiceName: serviceName,
			ServicePortName: "http", ServicePort: 80, Protocol: "TCP", PortIdentifier: "http",
			ContainerName: "app", ContainerPort: 8080, TargetHost: "127.0.0.1", TargetPort: 8080,
		},
	}
	_, err = client.ReconnectClient(ctx, &rpc.ReconnectClientRequest{
		Session: &rpc.SessionInfo{SessionId: sessionID}, Client: clientInfo, Intercepts: []*rpc.InterceptInfo{forged},
	})
	require.NoError(t, err)
	_, found := mgr.State().GetIntercept(forged.Id)
	require.False(t, found)
	require.Eventually(t, func() bool {
		intercept, ok := mgr.State().GetIntercept(interceptID)
		if !ok || len(intercept.ServiceWorkloads) != 2 {
			return false
		}
		return intercept.ServiceWorkloads[0].WorkloadName == canaryName && intercept.ServiceWorkloads[1].WorkloadName == stableName
	}, 5*time.Second, 10*time.Millisecond, "the manager must discover stable and canary from Kubernetes without trusting the supplied outsider")
	stored, found := mgr.State().GetIntercept(interceptID)
	require.True(t, found)
	require.Equal(t, sessionID, stored.ClientSession.SessionId)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, stored.Disposition)
	require.Empty(t, stored.PodName)
	require.Empty(t, stored.PodIp)
	require.Empty(t, stored.Environment)
	require.NotEqual(t, forged.Message, stored.Message)

	watch := func(session *rpc.SessionInfo) *rpc.InterceptInfoDelta {
		stream, err := client.WatchInterceptsDelta(ctx, session)
		require.NoError(t, err)
		delta, err := stream.Recv()
		require.NoError(t, err)
		return delta
	}
	require.Contains(t, watch(stableSession).Upserts, interceptID)
	require.Contains(t, watch(canarySession).Upserts, interceptID)
	require.NotContains(t, watch(outsiderSession).Upserts, interceptID)
	review := func(session *rpc.SessionInfo, agent *rpc.AgentInfo) {
		_, err := client.ReviewIntercept(ctx, &rpc.ReviewInterceptRequest{
			Session: session, Id: interceptID, Disposition: rpc.InterceptDispositionType_ACTIVE,
			PodIp: agent.PodIp, Environment: map[string]string{"SOURCE": agent.Name},
		})
		require.NoError(t, err)
	}
	review(outsiderSession, outsider)
	stored, found = mgr.State().GetIntercept(interceptID)
	require.True(t, found)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, stored.Disposition)
	require.Empty(t, stored.Environment)
	review(stableSession, stable)
	stored, found = mgr.State().GetIntercept(interceptID)
	require.True(t, found)
	require.Equal(t, rpc.InterceptDispositionType_WAITING, stored.Disposition, "fresh approval from the secondary is required")
	review(canarySession, canary)
	stored, found = mgr.State().GetIntercept(interceptID)
	require.True(t, found)
	require.Equal(t, rpc.InterceptDispositionType_ACTIVE, stored.Disposition)
	require.Equal(t, stable.PodName, stored.PodName)
	require.Equal(t, stable.PodIp, stored.PodIp)
	require.Equal(t, map[string]string{"SOURCE": stableName}, stored.Environment)
}
