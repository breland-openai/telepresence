package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/state"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func (s *service) acknowledgedRoutePod(ctx context.Context, namespace, name, uid string, acks map[string]bool, established bool) (string, bool) {
	var sessions []*state.AgentSession
	if s.state != nil {
		sessions = s.state.RouteAgentSessionsForPod(uid)
	}
	if len(sessions) != 0 {
		session := latestRouteAgentSession(sessions)
		if session == nil {
			return "", false
		}
		consumer, err := routeAgentConsumer(ctx, session)
		return consumer, err == nil && acks[consumer]
	}
	if !established || name == "" {
		return "", false
	}
	pod, err := k8sapi.GetK8sInterface(ctx).CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil || string(pod.UID) != uid {
		return "", false
	}
	prefix, _, err := runningRouteAgentContainer(pod)
	if err != nil {
		return "", false
	}
	// An established agent keeps its guards over manager-session loss. Kubernetes
	// identifies its direct PID 1 through this running container and restart count;
	// a new container cannot inherit its proof and initially rejects every keyed
	// request until its own authoritative snapshot has installed the route guards.
	return storedRouteAgentConsumer(prefix, acks)
}

func latestRouteAgentSession(sessions []*state.AgentSession) *state.AgentSession {
	var latest *state.AgentSession
	for _, candidate := range sessions {
		if candidate.RouteGuardInstance == "" {
			return nil
		}
		if latest == nil {
			latest = candidate
			continue
		}
		if candidate.RouteGuardInstance == latest.RouteGuardInstance {
			if candidate.PodName != latest.PodName {
				return nil
			}
			continue
		}
		currentTime, latestTime := candidate.RouteGuardStartedAt, latest.RouteGuardStartedAt
		if currentTime == nil || latestTime == nil || currentTime.CheckValid() != nil || latestTime.CheckValid() != nil || currentTime.AsTime().Equal(latestTime.AsTime()) {
			return nil
		}
		if currentTime.AsTime().After(latestTime.AsTime()) {
			latest = candidate
		}
	}
	return latest
}

func runningRouteAgentContainer(pod *core.Pod) (string, time.Time, error) {
	for _, container := range pod.Status.ContainerStatuses {
		if container.Name == agentconfig.ContainerName && container.ContainerID != "" &&
			container.State.Running != nil && !container.State.Running.StartedAt.IsZero() {
			identity := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s", container.RestartCount, container.ContainerID)))
			return "pod:" + string(pod.UID) + ":" + hex.EncodeToString(identity[:]) + ":", container.State.Running.StartedAt.Time, nil
		}
	}
	return "", time.Time{}, fmt.Errorf("agent pod has no running traffic-agent container identity")
}

func storedRouteAgentConsumer(prefix string, acks map[string]bool) (string, bool) {
	var found string
	for consumer, acknowledged := range acks {
		instance, matches := strings.CutPrefix(consumer, prefix)
		if !matches || !acknowledged {
			continue
		}
		parsed, err := uuid.Parse(instance)
		if err != nil || parsed == uuid.Nil || parsed.String() != instance {
			continue
		}
		if found != "" {
			return "", false // multiple claimed processes for one container are not trustworthy
		}
		found = consumer
	}
	return found, found != ""
}
