package manager

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"

	"github.com/telepresenceio/clog/testutil"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	managerstate "github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/state"
	testdata "github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/test"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

const (
	legacyForkTestVersion  = "2.29.2-breland.shared-service.13"
	currentForkTestVersion = "2.31.2-breland.shared-service.15"
)

func legacyForkAgentDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	file := protodesc.ToFileDescriptorProto(rpc.File_manager_manager_proto)
	file.Name = proto.String("legacy-fork-manager.proto")
	for _, message := range file.MessageType {
		if message.GetName() != "AgentInfo" {
			continue
		}
		fields := message.Field[:0]
		for _, field := range message.Field {
			switch field.GetName() {
			case "node_agent", "quic_port":
				continue
			case "intercept_targets":
				field.Number = proto.Int32(15)
			case "container_environment_omitted":
				field.Number = proto.Int32(16)
			}
			fields = append(fields, field)
		}
		message.Field = fields
		break
	}

	descriptor, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	require.NoError(t, err)
	agent := descriptor.Messages().ByName("AgentInfo")
	require.NotNil(t, agent)
	return agent
}

func decodeLegacyForkAgent(t *testing.T, descriptor protoreflect.MessageDescriptor, agent *rpc.AgentInfo) *dynamicpb.Message {
	t.Helper()

	data, err := proto.Marshal(agent)
	require.NoError(t, err)
	decoded := dynamicpb.NewMessage(descriptor)
	require.NoError(t, proto.Unmarshal(data, decoded))
	return decoded
}

func encodeLegacyForkAgent(
	t *testing.T,
	descriptor protoreflect.MessageDescriptor,
	agent *rpc.AgentInfo,
	targets []*rpc.AgentInfo_InterceptTarget,
	omitted bool,
) *rpc.AgentInfo {
	t.Helper()

	legacy := decodeLegacyForkAgent(t, descriptor, agent)
	legacy.SetUnknown(nil)
	targetField := descriptor.Fields().ByName("intercept_targets")
	for _, target := range targets {
		data, err := proto.Marshal(target)
		require.NoError(t, err)
		legacyTarget := dynamicpb.NewMessage(targetField.Message())
		require.NoError(t, proto.Unmarshal(data, legacyTarget))
		legacy.Mutable(targetField).List().Append(protoreflect.ValueOfMessage(legacyTarget.ProtoReflect()))
	}
	if omitted {
		legacy.Set(descriptor.Fields().ByName("container_environment_omitted"), protoreflect.ValueOfBool(true))
	}

	data, err := proto.Marshal(legacy)
	require.NoError(t, err)
	decoded := new(rpc.AgentInfo)
	require.NoError(t, proto.Unmarshal(data, decoded))
	return decoded
}

func legacyForkTestAgent(t *testing.T, version string) *rpc.AgentInfo {
	t.Helper()

	agent := proto.Clone(testdata.GetTestAgents(t)["hello"]).(*rpc.AgentInfo)
	agent.Version = version
	agent.PodIp = "10.1.2.3"
	agent.Containers = map[string]*rpc.AgentInfo_ContainerInfo{
		"app": {
			Environment: map[string]string{"TOKEN": "large-value"},
			MountPoint:  "/tel_app_mounts/app",
			Mounts:      map[string]int32{"/tmp": 1},
		},
	}
	return agent
}

func legacyForkTestTarget(name string) *rpc.AgentInfo_InterceptTarget {
	return &rpc.AgentInfo_InterceptTarget{
		ServiceUid:      "uid-" + name,
		ServiceName:     name,
		ServicePortName: "http",
		ServicePort:     8080,
		Protocol:        "TCP",
		ContainerName:   "app",
		ContainerPort:   8080,
	}
}

func requireLegacyForkTargetsEqual(t *testing.T, expected, actual []*rpc.AgentInfo_InterceptTarget) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for index := range expected {
		require.True(t, proto.Equal(expected[index], actual[index]), "target %d does not match", index)
	}
}

func TestLegacyForkAgentInfoWireCollisions(t *testing.T) {
	descriptor := legacyForkAgentDescriptor(t)
	omittedField := descriptor.Fields().ByName("container_environment_omitted")
	targetField := descriptor.Fields().ByName("intercept_targets")

	t.Run("current QUIC port aliases the legacy compact marker", func(t *testing.T) {
		legacy := decodeLegacyForkAgent(t, descriptor, &rpc.AgentInfo{
			QuicPort:                    7787,
			ContainerEnvironmentOmitted: true,
			InterceptTargets:            []*rpc.AgentInfo_InterceptTarget{legacyForkTestTarget("echo")},
		})
		require.True(t, legacy.Get(omittedField).Bool())
		require.Zero(t, legacy.Get(targetField).List().Len())
	})

	t.Run("current compact marker is unknown to a legacy client", func(t *testing.T) {
		legacy := decodeLegacyForkAgent(t, descriptor, &rpc.AgentInfo{ContainerEnvironmentOmitted: true})
		require.False(t, legacy.Get(omittedField).Bool())
	})

	t.Run("legacy compact marker aliases the current QUIC port", func(t *testing.T) {
		agent := encodeLegacyForkAgent(t, descriptor, &rpc.AgentInfo{Version: legacyForkTestVersion},
			[]*rpc.AgentInfo_InterceptTarget{legacyForkTestTarget("echo")}, true)
		require.Equal(t, int32(1), agent.QuicPort)
		require.False(t, agent.ContainerEnvironmentOmitted)
		require.Empty(t, agent.InterceptTargets)
		require.NotEmpty(t, agent.ProtoReflect().GetUnknown())
	})
}

func TestLegacyForkClientDetection(t *testing.T) {
	tests := []struct {
		name    string
		version string
		compact bool
		legacy  bool
	}{
		{name: "released fork", version: legacyForkTestVersion, compact: true, legacy: true},
		{name: "prefixed fork", version: "v" + legacyForkTestVersion, compact: true, legacy: true},
		{name: "stock upstream macOS", version: "2.28.0", compact: false},
		{name: "stock upstream", version: "2.29.3", compact: false},
		{name: "current fork", version: currentForkTestVersion, compact: true},
		{name: "upstream node-agent release", version: "2.30.1", compact: true},
		{name: "invalid version", version: "broken", compact: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.legacy, legacyForkClient(&rpc.ClientInfo{
				Version:                  test.version,
				SupportsCompactAgentInfo: test.compact,
			}))
		})
	}
	require.False(t, legacyForkClient(nil))
}

func TestWatchAgentsLegacyForkCompatibility(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	descriptor := legacyForkAgentDescriptor(t)

	for _, quicPort := range []int32{0, 7787} {
		t.Run(fmt.Sprintf("agent_quic_port_%d", quicPort), func(t *testing.T) {
			ctx := testutil.NewContext(t, true)
			conn, manager, _ := getTestClientConnAndService(ctx, t, nil, func(env *managerutil.Env) {
				if quicPort != 0 {
					env.TunnelQuicPort = 7778
				}
			})
			defer conn.Close()
			client := rpc.NewManagerClient(conn)

			legacyClient := proto.Clone(testdata.GetTestClients(t)["alice"]).(*rpc.ClientInfo)
			legacyClient.Name = "legacy-fork-client"
			legacyClient.InstallId = "legacy-fork-client"
			legacyClient.Version = legacyForkTestVersion
			legacyClient.SupportsCompactAgentInfo = true
			legacySession, err := client.ArriveAsClient(ctx, legacyClient)
			require.NoError(t, err)
			legacyDeltaWatch, err := client.WatchAgentsDelta(ctx, legacySession)
			require.NoError(t, err)
			_, err = legacyDeltaWatch.Recv()
			require.NoError(t, err)
			legacySnapshotWatch, err := client.WatchAgents(ctx, legacySession)
			require.NoError(t, err)
			_, err = legacySnapshotWatch.Recv()
			require.NoError(t, err)

			stockClient := proto.Clone(legacyClient).(*rpc.ClientInfo)
			stockClient.Name = "stock-upstream-client"
			stockClient.InstallId = "stock-upstream-client"
			stockClient.Version = "2.28.0"
			stockClient.SupportsCompactAgentInfo = false
			stockSession, err := client.ArriveAsClient(ctx, stockClient)
			require.NoError(t, err)
			stockWatch, err := client.WatchAgentsDelta(ctx, stockSession)
			require.NoError(t, err)
			_, err = stockWatch.Recv()
			require.NoError(t, err)

			currentClient := proto.Clone(legacyClient).(*rpc.ClientInfo)
			currentClient.Name = "current-client"
			currentClient.InstallId = "current-client"
			currentClient.Version = currentForkTestVersion
			currentSession, err := client.ArriveAsClient(ctx, currentClient)
			require.NoError(t, err)
			currentWatch, err := client.WatchAgentsDelta(ctx, currentSession)
			require.NoError(t, err)
			_, err = currentWatch.Recv()
			require.NoError(t, err)

			agent := legacyForkTestAgent(t, currentForkTestVersion)
			agent.QuicPort = quicPort
			agent.InterceptTargets = []*rpc.AgentInfo_InterceptTarget{legacyForkTestTarget("echo")}
			agentSession, err := client.ArriveAsAgent(ctx, agent)
			require.NoError(t, err)

			legacyDelta, err := legacyDeltaWatch.Recv()
			require.NoError(t, err)
			legacyDeltaAgent := onlyAgent(t, legacyDelta.Upserts)
			require.Equal(t, int32(1), legacyDeltaAgent.QuicPort)
			require.Nil(t, legacyDeltaAgent.Containers["app"].Environment)
			decodedLegacyDelta := decodeLegacyForkAgent(t, descriptor, legacyDeltaAgent)
			require.True(t, decodedLegacyDelta.Get(descriptor.Fields().ByName("container_environment_omitted")).Bool())

			legacySnapshot, err := legacySnapshotWatch.Recv()
			require.NoError(t, err)
			require.Len(t, legacySnapshot.Agents, 1)
			decodedLegacySnapshot := decodeLegacyForkAgent(t, descriptor, legacySnapshot.Agents[0])
			require.True(t, decodedLegacySnapshot.Get(descriptor.Fields().ByName("container_environment_omitted")).Bool())

			stockDelta, err := stockWatch.Recv()
			require.NoError(t, err)
			stockAgent := onlyAgent(t, stockDelta.Upserts)
			require.False(t, stockAgent.ContainerEnvironmentOmitted)
			require.Equal(t, map[string]string{"TOKEN": "large-value"}, stockAgent.Containers["app"].Environment)
			require.Equal(t, quicPort, stockAgent.QuicPort)

			currentDelta, err := currentWatch.Recv()
			require.NoError(t, err)
			currentAgent := onlyAgent(t, currentDelta.Upserts)
			require.True(t, currentAgent.ContainerEnvironmentOmitted)
			require.Nil(t, currentAgent.Containers["app"].Environment)
			require.Equal(t, quicPort, currentAgent.QuicPort)
			requireLegacyForkTargetsEqual(t, agent.InterceptTargets, currentAgent.InterceptTargets)

			storedAgent := manager.State().GetAgent(tunnel.SessionID(agentSession.SessionId))
			require.NotNil(t, storedAgent)
			require.Equal(t, quicPort, storedAgent.QuicPort)
			require.Equal(t, map[string]string{"TOKEN": "large-value"}, storedAgent.Containers["app"].Environment)
		})
	}
}

func TestLegacyForkAgentRegistrationRecoversTargets(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	descriptor := legacyForkAgentDescriptor(t)
	targets := []*rpc.AgentInfo_InterceptTarget{
		legacyForkTestTarget("stable"),
		legacyForkTestTarget("canary"),
	}

	tests := []struct {
		name      string
		reconnect bool
	}{
		{name: "arrival"},
		{name: "reconnect", reconnect: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := testutil.NewContext(t, true)
			conn, manager, _ := getTestClientConnAndService(ctx, t, nil)
			defer conn.Close()
			client := rpc.NewManagerClient(conn)
			wireAgent := encodeLegacyForkAgent(t, descriptor, legacyForkTestAgent(t, legacyForkTestVersion), targets, false)
			require.Empty(t, wireAgent.InterceptTargets)
			require.NotEmpty(t, wireAgent.ProtoReflect().GetUnknown())

			var sessionID string
			if test.reconnect {
				sessionID = managerstate.AgentSessionIDPrefix + wireAgent.PodUid
				_, err := client.ReconnectAgent(ctx, &rpc.ReconnectAgentRequest{
					Session: &rpc.SessionInfo{SessionId: sessionID},
					Agent:   wireAgent,
				})
				require.NoError(t, err)
			} else {
				session, err := client.ArriveAsAgent(ctx, wireAgent)
				require.NoError(t, err)
				sessionID = session.SessionId
			}

			stored := manager.State().GetAgent(tunnel.SessionID(sessionID))
			require.NotNil(t, stored)
			requireLegacyForkTargetsEqual(t, targets, stored.InterceptTargets)
			require.Zero(t, stored.QuicPort)
			require.Equal(t, map[string]string{"TOKEN": "large-value"}, stored.Containers["app"].Environment)
		})
	}
}

func TestLegacyForkReconnectClientSkipsPoisonedAgents(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx := testutil.NewContext(t, true)
	conn, manager, _ := getTestClientConnAndService(ctx, t, nil)
	defer conn.Close()
	client := rpc.NewManagerClient(conn)
	descriptor := legacyForkAgentDescriptor(t)

	clientInfo := proto.Clone(testdata.GetTestClients(t)["alice"]).(*rpc.ClientInfo)
	clientInfo.Name = "reconnecting-legacy-fork-client"
	clientInfo.InstallId = "reconnecting-legacy-fork-client"
	clientInfo.Version = legacyForkTestVersion
	clientInfo.SupportsCompactAgentInfo = true
	agent := legacyForkTestAgent(t, legacyForkTestVersion)
	agent.Containers["app"].Environment = nil
	targets := []*rpc.AgentInfo_InterceptTarget{legacyForkTestTarget("echo")}
	poisoned := encodeLegacyForkAgent(t, descriptor, agent, targets, true)
	require.Equal(t, int32(1), poisoned.QuicPort)
	require.False(t, poisoned.ContainerEnvironmentOmitted)

	_, err := client.ReconnectClient(ctx, &rpc.ReconnectClientRequest{
		Session: &rpc.SessionInfo{SessionId: "reconnecting-legacy-fork-session"},
		Client:  clientInfo,
		Agents:  []*rpc.AgentInfo{poisoned},
	})
	require.NoError(t, err)
	require.Zero(t, manager.State().CountAgents())

	actualAgent := encodeLegacyForkAgent(t, descriptor, legacyForkTestAgent(t, legacyForkTestVersion), targets, false)
	agentSession, err := client.ArriveAsAgent(ctx, actualAgent)
	require.NoError(t, err)
	restored := manager.State().GetAgent(tunnel.SessionID(agentSession.SessionId))
	require.NotNil(t, restored)
	requireLegacyForkTargetsEqual(t, targets, restored.InterceptTargets)
	require.Zero(t, restored.QuicPort)
	require.Equal(t, map[string]string{"TOKEN": "large-value"}, restored.Containers["app"].Environment)
}

func TestCurrentAgentRegistrationPreservesUpstreamFields(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx := testutil.NewContext(t, true)
	conn, manager, _ := getTestClientConnAndService(ctx, t, nil)
	defer conn.Close()
	client := rpc.NewManagerClient(conn)
	agent := legacyForkTestAgent(t, currentForkTestVersion)
	agent.NodeAgent = true
	agent.QuicPort = 7787
	agent.InterceptTargets = []*rpc.AgentInfo_InterceptTarget{legacyForkTestTarget("echo")}

	session, err := client.ArriveAsAgent(ctx, agent)
	require.NoError(t, err)
	stored := manager.State().GetAgent(tunnel.SessionID(session.SessionId))
	require.NotNil(t, stored)
	require.True(t, stored.NodeAgent)
	require.Equal(t, int32(7787), stored.QuicPort)
	requireLegacyForkTargetsEqual(t, agent.InterceptTargets, stored.InterceptTargets)
}

func TestLegacyForkAgentRegistrationRejectsMalformedTarget(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx := testutil.NewContext(t, true)
	conn := getTestClientConn(ctx, t, nil)
	defer conn.Close()
	client := rpc.NewManagerClient(conn)
	agent := legacyForkTestAgent(t, legacyForkTestVersion)
	unknown := protowire.AppendTag(nil, legacyInterceptTargetsField, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, []byte{0xff})
	agent.ProtoReflect().SetUnknown(unknown)

	_, err := client.ArriveAsAgent(ctx, agent)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "invalid legacy agent information")
}
