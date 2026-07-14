package trafficmgr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/telepresenceio/clog/testutil"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
)

type ensureAgentManager struct {
	manager.UnimplementedManagerServer
	response *manager.AgentInfoSnapshot
	request  *manager.EnsureAgentRequest
	calls    int
}

func (s *ensureAgentManager) EnsureAgent(_ context.Context, request *manager.EnsureAgentRequest) (*manager.AgentInfoSnapshot, error) {
	s.calls++
	s.request = request
	return s.response, nil
}

func TestFullAgentInfoFetchesOmittedEnvironmentOnDemand(t *testing.T) {
	full := &manager.AgentInfo{
		Name:      "echo",
		Namespace: "default",
		PodName:   "echo-123",
		Containers: map[string]*manager.AgentInfo_ContainerInfo{
			"app": {Environment: map[string]string{"TOKEN": "large-value"}},
		},
	}
	server := &ensureAgentManager{response: &manager.AgentInfoSnapshot{Agents: []*manager.AgentInfo{full}}}
	conn, cleanup := dialRemainTestManager(t, server)
	defer cleanup()

	session := &session{
		Cluster: &k8s.Cluster{Kubeconfig: &k8s.Kubeconfig{
			Context: client.WithConfig(testutil.NewContext(t, false), client.GetDefaultConfig()),
		}},
		managerConn: conn,
		sessionInfo: &manager.SessionInfo{
			SessionId: "client-session",
		},
	}
	compact := &manager.AgentInfo{
		Name:                        full.Name,
		Namespace:                   full.Namespace,
		PodName:                     full.PodName,
		ContainerEnvironmentOmitted: true,
		Containers: map[string]*manager.AgentInfo_ContainerInfo{
			"app": {},
		},
	}

	got, err := session.fullAgentInfo(context.Background(), compact)
	require.NoError(t, err)
	require.True(t, proto.Equal(full, got))
	require.Equal(t, 1, server.calls)
	require.True(t, proto.Equal(session.sessionInfo, server.request.Session))
	require.Equal(t, full.Name, server.request.Name)
	require.Equal(t, full.Namespace, server.request.Namespace)
	require.Equal(t, map[string]string{"TOKEN": "large-value"}, got.Containers["app"].Environment)
}

func TestFullAgentInfoKeepsOldFullSnapshotWithoutRPC(t *testing.T) {
	server := &ensureAgentManager{}
	conn, cleanup := dialRemainTestManager(t, server)
	defer cleanup()

	session := &session{
		Cluster: &k8s.Cluster{Kubeconfig: &k8s.Kubeconfig{
			Context: client.WithConfig(testutil.NewContext(t, false), client.GetDefaultConfig()),
		}},
		managerConn: conn,
		sessionInfo: &manager.SessionInfo{
			SessionId: "client-session",
		},
	}
	full := &manager.AgentInfo{
		Name:      "echo",
		Namespace: "default",
		Containers: map[string]*manager.AgentInfo_ContainerInfo{
			"app": {Environment: map[string]string{"TOKEN": "large-value"}},
		},
	}

	got, err := session.fullAgentInfo(context.Background(), full)
	require.NoError(t, err)
	require.Same(t, full, got)
	require.Zero(t, server.calls)
}
