package agent

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	k8sTypes "k8s.io/apimachinery/pkg/types"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func TestAdvertisedRouteGuardInstanceIsStrictAndStableForThisProcess(t *testing.T) {
	legacy := &agentconfig.Sidecar{}
	require.Empty(t, advertisedRouteGuardInstance(legacy))
	strict := &agentconfig.Sidecar{RequireAuthoritativeRoutes: true}
	first, err := uuid.Parse(advertisedRouteGuardInstance(strict))
	require.NoError(t, err)
	require.Equal(t, uuid.Version(4), first.Version())
	require.Equal(t, first.String(), advertisedRouteGuardInstance(strict))
	require.Equal(t, first.String(), advertisedRouteGuardInstance(&agentconfig.Sidecar{RequireAuthoritativeRoutes: true}))
}

func TestAdvertisedInterceptTargets(t *testing.T) {
	ac := &agentconfig.Sidecar{Containers: []*agentconfig.Container{{
		Name: "app",
		Intercepts: []*agentconfig.Intercept{
			{
				ServiceUID:      k8sTypes.UID("shared-service"),
				ServiceName:     "app",
				ServicePortName: "http",
				ServicePort:     80,
				Protocol:        types.ProtoTCP,
				AppProtocol:     "http",
				ContainerPort:   8080,
				AgentPort:       9901,
			},
			{
				// Duplicates are intentionally omitted from AgentInfo.
				ServiceUID:      k8sTypes.UID("shared-service"),
				ServiceName:     "app",
				ServicePortName: "http",
				ServicePort:     80,
				Protocol:        types.ProtoTCP,
				ContainerPort:   8080,
			},
			{
				// Container-only targets are still workload-scoped.
				Protocol:      types.ProtoTCP,
				ContainerPort: 9090,
			},
		},
	}}}

	require.Equal(t, []*rpc.AgentInfo_InterceptTarget{{
		ServiceUid:      "shared-service",
		ServiceName:     "app",
		ServicePortName: "http",
		ServicePort:     80,
		Protocol:        "TCP",
		ContainerName:   "app",
		ContainerPort:   8080,
		AppProtocol:     "http",
		AgentPort:       9901,
	}}, advertisedInterceptTargets(ac))
	ac.Containers[0].Replace = agentconfig.ReplacePolicyContainer
	require.EqualValues(t, 8080, advertisedInterceptTargets(ac)[0].AgentPort, "replacement listeners bind the original container port")
}
