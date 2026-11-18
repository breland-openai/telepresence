package state

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/telepresenceio/clog"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
)

const agentWorkloadLookupTimeout = 2 * time.Second

type agentInterceptChange struct {
	intercept *Intercept
	selected  map[string]*interceptParticipant
	known     bool
	canAdd    bool
}

func (s *State) prepareAgentInterceptChanges(ctx context.Context, agent *rpc.AgentInfo, departing bool) []agentInterceptChange {
	// Discovery needs the manager's informer and Kubernetes client context.
	lookupCtx, cancel := context.WithTimeout(s.backgroundCtx, agentWorkloadLookupTimeout)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()

	var changes []agentInterceptChange
	s.intercepts.RangeRelaxed(func(_ string, intercept *Intercept) bool {
		if ctx.Err() != nil {
			return false
		}
		if !agentAffectsIntercept(agent, intercept, departing) {
			return true
		}
		change := agentInterceptChange{intercept: intercept}
		if serviceScopedIntercept(intercept.Spec) && lookupCtx.Err() == nil {
			workloads, known, err := discoverServiceWorkloads(lookupCtx, intercept.Spec)
			switch {
			case err != nil:
				clog.Debugf(lookupCtx, "Unable to reconcile participants for intercept %q: %v", intercept.Id, err)
			case known:
				change.selected = serviceParticipantsFromWorkloads(workloads)
				change.known = true
			default:
				change.canAdd = lookupCtx.Err() == nil
			}
		}
		changes = append(changes, change)
		return true
	})
	return changes
}

func agentAffectsIntercept(agent *rpc.AgentInfo, intercept *Intercept, departing bool) bool {
	if intercept.Disposition == rpc.InterceptDispositionType_REMOVED {
		return false
	}
	matches := AgentMatchesIntercept(agent, intercept.Spec)
	if !serviceScopedIntercept(intercept.Spec) {
		return matches && (!departing || intercept.PodIp == agent.PodIp)
	}
	_, participant := intercept.participants[agentParticipantKey(agent)]
	return participant || (!departing && matches)
}

func (s *State) consolidateAgentSessionIntercepts(agent *AgentSession) {
	changes := s.prepareAgentInterceptChanges(s.backgroundCtx, agent.AgentInfo, true)
	s.reconcileAgentIntercepts(agent, true, changes)
}

func (s *State) reconcileAgentIntercepts(agent *AgentSession, departing bool, changes []agentInterceptChange) {
	for _, change := range changes {
		s.reconcileAgentIntercept(agent, departing, change)
	}
}

func (s *State) reconcileAgentIntercept(agent *AgentSession, departing bool, change agentInterceptChange) {
	for {
		generation := s.agentSessionGeneration.Load()
		currentSession, _ := s.agents.Load(agent.sessionID())
		if (departing && currentSession != nil) || (!departing && currentSession != agent) {
			return
		}
		current, ok := s.intercepts.Load(change.intercept.Id)
		if !ok || !proto.Equal(current.Spec, change.intercept.Spec) || !agentAffectsIntercept(agent.AgentInfo, current, departing) {
			return
		}
		next := current.Clone()
		serviceScoped := serviceScopedIntercept(next.Spec)
		key := agentParticipantKey(agent.AgentInfo)
		if serviceScoped {
			// Only use owner data prepared for the intercept revision being updated.
			sameRevision := current == change.intercept
			s.applyServiceParticipantSelection(next, change.selected, sameRevision && change.known, false)
			if next.participants[key] == nil {
				if departing || AgentMatchesIntercept(agent.AgentInfo, next.Spec) {
					if departing || !sameRevision || !change.canAdd {
						if s.publishAgentIntercept(current, next, generation) {
							return
						}
						continue
					}
					next.addParticipant(agent.AgentInfo)
					if next.Disposition == rpc.InterceptDispositionType_ACTIVE {
						waitForAgentApproval(next)
					}
				}
			}
		}
		if departing {
			s.applyDepartedAgent(next, key, agent)
		} else if code, message := s.checkAgentsForIntercept(next); code != rpc.InterceptDispositionType_UNSPECIFIED {
			next.Disposition, next.Message = code, message
		} else if next.Disposition == rpc.InterceptDispositionType_NO_AGENT {
			next.Disposition, next.Message = rpc.InterceptDispositionType_WAITING, ""
		}
		if s.publishAgentIntercept(current, next, generation) {
			return
		}
	}
}

// Take the intercept bucket before the session lock. The session lock fences
// publication against session changes and is never held while waiting for an intercept bucket.
func (s *State) publishAgentIntercept(previous, next *Intercept, generation uint64) bool {
	if previous != nil {
		if interceptEqual(previous, next) {
			return true
		}
		next.ModifiedAt = timestamppb.Now()
	}
	locked := false
	defer func() {
		if locked {
			s.agentSessionMu.Unlock()
		}
	}()
	published := false
	s.intercepts.Compute(next.Id, func(current *Intercept, found bool) (*Intercept, xsync.ComputeOp) {
		if found != (previous != nil) || previous != current {
			return current, xsync.CancelOp
		}
		s.agentSessionMu.Lock()
		locked = true
		if s.agentSessionGeneration.Load() != generation {
			return current, xsync.CancelOp
		}
		published = true
		return next, xsync.UpdateOp
	})
	return published
}

func (s *State) applyDepartedAgent(intercept *Intercept, key string, agent *AgentSession) {
	serviceScoped := serviceScopedIntercept(intercept.Spec)
	participant := intercept.participants[key]
	reviewed := participant != nil && participant.podName == agent.PodName
	if code, message := s.checkAgentsForIntercept(intercept); code != rpc.InterceptDispositionType_UNSPECIFIED {
		if reviewed {
			participant.review = nil
			participant.podName = ""
		} else if !serviceScoped {
			intercept.PodIp, intercept.PodName = "", ""
		}
		intercept.Disposition, intercept.Message = code, message
	} else if serviceScoped && reviewed {
		if !s.transferServiceParticipantReview(intercept, key, agent) {
			participant.review = nil
			participant.podName = ""
			waitForAgentApproval(intercept)
		}
	} else if !serviceScoped || intercept.Disposition == rpc.InterceptDispositionType_NO_AGENT {
		if !serviceScoped {
			intercept.PodIp, intercept.PodName = "", ""
		}
		intercept.Disposition, intercept.Message = rpc.InterceptDispositionType_WAITING, ""
	}
}

func waitForAgentApproval(intercept *Intercept) {
	intercept.Disposition = rpc.InterceptDispositionType_WAITING
	intercept.Message = fmt.Sprintf("Waiting for Agent approval from workloads: %s", strings.Join(intercept.pendingParticipants(), ", "))
}
