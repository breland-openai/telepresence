package manager

import (
	"fmt"
	"strings"

	"github.com/blang/semver/v4"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
)

const legacyInterceptTargetsField protowire.Number = 15

// legacyForkClient identifies clients that adopted compact agent snapshots
// before upstream assigned the same AgentInfo field numbers to other features.
func legacyForkClient(client *rpc.ClientInfo) bool {
	return client != nil && client.SupportsCompactAgentInfo && legacyForkVersion(client.Version)
}

func legacyForkVersion(version string) bool {
	parsed, err := semver.Parse(strings.TrimPrefix(version, "v"))
	return err == nil && parsed.Major == 2 && parsed.Minor < 30
}

// normalizeLegacyAgentInfo recovers service targets encoded at the field number
// now occupied by node_agent. Their different wire type preserves them as
// unknown fields, while the old compact marker aliases the current QUIC port.
func normalizeLegacyAgentInfo(agent *rpc.AgentInfo) error {
	if agent == nil || !legacyForkVersion(agent.Version) {
		return nil
	}
	agent.QuicPort = 0
	if len(agent.InterceptTargets) != 0 {
		return nil
	}

	unknown := agent.ProtoReflect().GetUnknown()
	for len(unknown) != 0 {
		number, wireType, consumed := protowire.ConsumeTag(unknown)
		if consumed < 0 {
			return fmt.Errorf("parse legacy agent field: %w", protowire.ParseError(consumed))
		}
		unknown = unknown[consumed:]

		if number == legacyInterceptTargetsField && wireType == protowire.BytesType {
			encoded, consumed := protowire.ConsumeBytes(unknown)
			if consumed < 0 {
				return fmt.Errorf("parse legacy intercept target: %w", protowire.ParseError(consumed))
			}
			target := new(rpc.AgentInfo_InterceptTarget)
			if err := proto.Unmarshal(encoded, target); err != nil {
				return fmt.Errorf("decode legacy intercept target: %w", err)
			}
			agent.InterceptTargets = append(agent.InterceptTargets, target)
			unknown = unknown[consumed:]
			continue
		}

		consumed = protowire.ConsumeFieldValue(number, wireType, unknown)
		if consumed < 0 {
			return fmt.Errorf("skip legacy agent field: %w", protowire.ParseError(consumed))
		}
		unknown = unknown[consumed:]
	}
	return nil
}

// agentInfoForClientWatch preserves the original compact marker for clients
// whose schema interprets upstream's QUIC-port field as that boolean.
func agentInfoForClientWatch(agent *rpc.AgentInfo, compact, legacy bool) *rpc.AgentInfo {
	projected := agentInfoForWatch(agent, compact)
	if compact && legacy {
		// A legacy client reads field 16 as container_environment_omitted. Such
		// clients cannot use QUIC, so a nonzero sentinel is safe and ensures
		// they hydrate this compact snapshot through EnsureAgent.
		projected.QuicPort = 1
	}
	return projected
}
