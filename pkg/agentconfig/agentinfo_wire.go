package agentconfig

import (
	"fmt"
	"strings"

	"github.com/blang/semver/v4"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
)

const (
	forkTargetField  protowire.Number = 17 // bytes, upstream uses a varint
	forkOmittedField protowire.Number = 18 // varint, upstream uses bytes
)

// UsesForkAgentFields identifies the release line with swapped AgentInfo tags.
// Stock builds can share a version with the fork and read the canonical tags.
func UsesForkAgentFields(version string) bool {
	v, err := semver.Parse(strings.TrimPrefix(version, "v"))
	return err == nil && v.Major == 2 && v.Minor >= 30 && v.Minor < 33
}

// AgentInfoForPeer adds the fork's wire types alongside the canonical fields.
// Full agent records can be shared by several sessions, so projection is immutable.
func AgentInfoForPeer(agent *manager.AgentInfo, forkFields bool) (*manager.AgentInfo, error) {
	if !forkFields || agent == nil || len(agent.InterceptTargets) == 0 && !agent.ContainerEnvironmentOmitted {
		return agent, nil
	}
	out := proto.CloneOf(agent)
	if err := NormalizeAgentInfo(out); err != nil {
		return nil, err
	}
	unknown := out.ProtoReflect().GetUnknown()
	for _, target := range agent.InterceptTargets {
		data, err := proto.Marshal(target)
		if err != nil {
			return nil, err
		}
		unknown = protowire.AppendTag(unknown, forkTargetField, protowire.BytesType)
		unknown = protowire.AppendBytes(unknown, data)
	}
	if agent.ContainerEnvironmentOmitted {
		unknown = protowire.AppendTag(unknown, forkOmittedField, protowire.VarintType)
		unknown = protowire.AppendVarint(unknown, 1)
	}
	out.ProtoReflect().SetUnknown(unknown)
	return out, nil
}

// NormalizeAgentInfo recovers next5's wire types at the RPC boundary. Canonical
// targets take precedence if a peer sends both encodings during an upgrade.
func NormalizeAgentInfo(agent *manager.AgentInfo) error {
	if agent == nil {
		return nil
	}
	raw := agent.ProtoReflect().GetUnknown()
	if len(raw) == 0 {
		return nil
	}
	var preserved []byte
	var targets []*manager.AgentInfo_InterceptTarget
	omitted := agent.ContainerEnvironmentOmitted
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeField(raw)
		if n < 0 {
			return protowire.ParseError(n)
		}
		_, _, tagSize := protowire.ConsumeTag(raw)
		switch {
		case num == forkTargetField && typ == protowire.BytesType:
			if len(agent.InterceptTargets) == 0 {
				data, _ := protowire.ConsumeBytes(raw[tagSize:n])
				target := new(manager.AgentInfo_InterceptTarget)
				if err := proto.Unmarshal(data, target); err != nil {
					return fmt.Errorf("decoding fork intercept target: %w", err)
				}
				targets = append(targets, target)
			}
		case num == forkOmittedField && typ == protowire.VarintType:
			value, _ := protowire.ConsumeVarint(raw[tagSize:n])
			omitted = omitted || value != 0
		default:
			preserved = append(preserved, raw[:n]...)
		}
		raw = raw[n:]
	}
	if len(targets) > 0 {
		agent.InterceptTargets = targets
	}
	agent.ContainerEnvironmentOmitted = omitted
	agent.ProtoReflect().SetUnknown(preserved)
	return nil
}
