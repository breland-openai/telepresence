package state

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

// interceptParticipant tracks one workload that must approve a service-scoped
// intercept before the logical intercept can become active. A workload may
// have many agent pods, but one approving pod is sufficient because all pods
// receive the active intercept snapshot.
type interceptParticipant struct {
	namespace string
	kind      string
	name      string
	podName   string
	review    *rpc.ReviewInterceptRequest
}

func (p *interceptParticipant) clone() *interceptParticipant {
	cp := &interceptParticipant{
		namespace: p.namespace,
		kind:      p.kind,
		name:      p.name,
		podName:   p.podName,
	}
	if p.review != nil {
		cp.review = proto.Clone(p.review).(*rpc.ReviewInterceptRequest)
	}
	return cp
}

func participantKey(namespace, kind, name string) string {
	return namespace + "/" + kind + "/" + name
}

func agentParticipantKey(agent *rpc.AgentInfo) string {
	return participantKey(agent.Namespace, agent.Kind, agent.Name)
}

func serviceScopedIntercept(spec *rpc.InterceptSpec) bool {
	return spec != nil && spec.ServiceUid != "" && (spec.ServicePort > 0 || spec.ServicePortName != "")
}

func servicePortMatches(target *rpc.AgentInfo_InterceptTarget, spec *rpc.InterceptSpec) bool {
	if target.ServiceUid != spec.ServiceUid || target.Protocol != spec.Protocol {
		return false
	}
	if spec.ServicePort > 0 {
		return target.ServicePort == spec.ServicePort
	}
	return target.ServicePortName == spec.ServicePortName
}

func agentClaimsService(agent *rpc.AgentInfo, spec *rpc.InterceptSpec) bool {
	for _, target := range agent.InterceptTargets {
		if servicePortMatches(target, spec) {
			return true
		}
	}
	return false
}

// AgentMatchesIntercept reports whether an agent is eligible to receive and
// review an intercept. Service-backed intercepts are matched using the
// agent's advertised Service UID and port claims. All other intercepts retain
// the historical workload-name behavior.
func AgentMatchesIntercept(agent *rpc.AgentInfo, spec *rpc.InterceptSpec) bool {
	if agent == nil || spec == nil || agent.Namespace != spec.Namespace {
		return false
	}
	if !serviceScopedIntercept(spec) {
		return agent.Name == spec.Agent
	}
	if agentClaimsService(agent, spec) {
		return true
	}
	// Older agents do not advertise targets. Preserve the historical path for
	// the explicitly requested workload so ordinary one-workload Service
	// intercepts remain compatible during a rolling upgrade.
	return len(agent.InterceptTargets) == 0 && agent.Name == spec.Agent
}

func participantsEqual(a, b map[string]*interceptParticipant) bool {
	if len(a) != len(b) {
		return false
	}
	for key, ap := range a {
		bp, ok := b[key]
		if !ok || ap.namespace != bp.namespace || ap.kind != bp.kind || ap.name != bp.name ||
			ap.podName != bp.podName || !proto.Equal(ap.review, bp.review) {
			return false
		}
	}
	return true
}

func (is *Intercept) cloneParticipants() map[string]*interceptParticipant {
	if len(is.participants) == 0 {
		return nil
	}
	participants := make(map[string]*interceptParticipant, len(is.participants))
	for key, participant := range is.participants {
		participants[key] = participant.clone()
	}
	return participants
}

func (s *State) initializeParticipants(intercept *Intercept) {
	if !serviceScopedIntercept(intercept.Spec) {
		return
	}
	intercept.participants = make(map[string]*interceptParticipant)
	s.EachAgent(func(_ tunnel.SessionID, agent *AgentSession) bool {
		if AgentMatchesIntercept(agent.AgentInfo, intercept.Spec) {
			key := agentParticipantKey(agent.AgentInfo)
			intercept.participants[key] = &interceptParticipant{
				namespace: agent.Namespace,
				kind:      agent.Kind,
				name:      agent.Name,
			}
		}
		return true
	})
	intercept.syncServiceWorkloads()
}

func (is *Intercept) addParticipant(agent *rpc.AgentInfo) *interceptParticipant {
	key := agentParticipantKey(agent)
	if participant, ok := is.participants[key]; ok {
		return participant
	}
	participant := &interceptParticipant{
		namespace: agent.Namespace,
		kind:      agent.Kind,
		name:      agent.Name,
	}
	if is.participants == nil {
		is.participants = make(map[string]*interceptParticipant)
	}
	is.participants[key] = participant
	is.syncServiceWorkloads()
	return participant
}

func (is *Intercept) syncServiceWorkloads() {
	if len(is.participants) == 0 {
		is.ServiceWorkloads = nil
		return
	}
	keys := is.participantKeys()
	workloads := make([]*rpc.InterceptWorkload, 0, len(keys))
	for _, key := range keys {
		participant := is.participants[key]
		workloads = append(workloads, &rpc.InterceptWorkload{
			Namespace:    participant.namespace,
			WorkloadKind: participant.kind,
			WorkloadName: participant.name,
		})
	}
	is.ServiceWorkloads = workloads
}

func (is *Intercept) pendingParticipants() []string {
	pending := make([]string, 0, len(is.participants))
	for _, participant := range is.participants {
		if participant.review == nil {
			pending = append(pending, participant.name)
		}
	}
	sort.Strings(pending)
	return pending
}

func (is *Intercept) participantKeys() []string {
	keys := make([]string, 0, len(is.participants))
	for key := range is.participants {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (s *State) agentsByParticipant(intercept *Intercept) map[string][]*rpc.AgentInfo {
	groups := make(map[string][]*rpc.AgentInfo)
	if !serviceScopedIntercept(intercept.Spec) {
		key := participantKey(intercept.Spec.Namespace, intercept.Spec.WorkloadKind, intercept.Spec.Agent)
		s.EachAgent(func(_ tunnel.SessionID, agent *AgentSession) bool {
			if AgentMatchesIntercept(agent.AgentInfo, intercept.Spec) {
				groups[key] = append(groups[key], agent.AgentInfo)
			}
			return true
		})
		return groups
	}

	for key := range intercept.participants {
		groups[key] = nil
	}
	s.EachAgent(func(_ tunnel.SessionID, agent *AgentSession) bool {
		if !AgentMatchesIntercept(agent.AgentInfo, intercept.Spec) {
			return true
		}
		key := agentParticipantKey(agent.AgentInfo)
		if _, ok := intercept.participants[key]; ok {
			groups[key] = append(groups[key], agent.AgentInfo)
		}
		return true
	})
	return groups
}

func (is *Intercept) primaryParticipant() *interceptParticipant {
	var first *interceptParticipant
	for _, participant := range is.participants {
		if first == nil || participantKey(participant.namespace, participant.kind, participant.name) <
			participantKey(first.namespace, first.kind, first.name) {
			first = participant
		}
		if participant.namespace == is.Spec.Namespace && participant.name == is.Spec.Agent &&
			(is.Spec.WorkloadKind == "" || participant.kind == is.Spec.WorkloadKind) {
			return participant
		}
	}
	return first
}

func applyReview(intercept *Intercept, review *rpc.ReviewInterceptRequest) {
	intercept.Disposition = review.Disposition
	intercept.Message = review.Message
	intercept.PodIp = review.PodIp
	intercept.PodName = ""
	intercept.FtpPort = review.FtpPort
	intercept.SftpPort = review.SftpPort
	intercept.MountPoint = review.MountPoint
	intercept.MechanismArgsDesc = review.MechanismArgsDesc
	intercept.Environment = review.Environment
	intercept.Mounts = review.Mounts
}

func (is *Intercept) applyServiceReview(agent *rpc.AgentInfo, review *rpc.ReviewInterceptRequest) {
	participant := is.addParticipant(agent)
	if review.Disposition != rpc.InterceptDispositionType_ACTIVE {
		applyReview(is, review)
		if is.Message == "" {
			is.Message = fmt.Sprintf("Workload %q rejected the intercept", participant.name)
		}
		return
	}

	participant.review = proto.Clone(review).(*rpc.ReviewInterceptRequest)
	participant.podName = agent.PodName
	pending := is.pendingParticipants()
	if len(pending) > 0 {
		is.Disposition = rpc.InterceptDispositionType_WAITING
		is.Message = fmt.Sprintf("Waiting for Agent approval from workloads: %s", strings.Join(pending, ", "))
		return
	}

	primary := is.primaryParticipant()
	if primary == nil || primary.review == nil {
		is.Disposition = rpc.InterceptDispositionType_WAITING
		is.Message = "Waiting for primary workload Agent approval"
		return
	}
	applyReview(is, primary.review)
	is.PodName = primary.podName
	is.Disposition = rpc.InterceptDispositionType_ACTIVE
	is.Message = ""
}
