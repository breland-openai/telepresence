package agentconfig

import (
	"errors"
	"fmt"
	"slices"
	"strconv"

	core "k8s.io/api/core/v1"

	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
)

func InitContainer(config *Sidecar, agentSecurityContext *core.SecurityContext, coverDir string) *core.Container {
	ic := &core.Container{
		Name:  InitContainerName,
		Image: config.AgentImage,
		Args:  []string{"agent-init"},
		Env: []core.EnvVar{
			{
				Name:  "LOG_LEVEL",
				Value: config.LogLevel.String(),
			},
			{
				Name: "AGENT_CONFIG",
				ValueFrom: &core.EnvVarSource{
					FieldRef: &core.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  fmt.Sprintf("metadata.annotations['%s']", annotation.Config),
					},
				},
			},
			{
				Name: "POD_IP",
				ValueFrom: &core.EnvVarSource{
					FieldRef: &core.ObjectFieldSelector{
						APIVersion: "v1",
						FieldPath:  "status.podIP",
					},
				},
			},
		},
		SecurityContext: &core.SecurityContext{
			Capabilities: &core.Capabilities{
				Add: []core.Capability{"NET_ADMIN"},
			},
		},
	}
	if agentSecurityContext != nil {
		if uid := agentSecurityContext.RunAsUser; uid != nil {
			ic.Env = append(ic.Env, core.EnvVar{
				Name:  EnvAgentUID,
				Value: strconv.FormatInt(*uid, 10),
			})
		}
		if gid := agentSecurityContext.RunAsGroup; gid != nil {
			ic.Env = append(ic.Env, core.EnvVar{
				Name:  EnvAgentGID,
				Value: strconv.FormatInt(*gid, 10),
			})
		}
	}
	if coverDir != "" {
		ic.Env = append(ic.Env, core.EnvVar{Name: "GOCOVERDIR", Value: coverDir})
		ic.VolumeMounts = append(ic.VolumeMounts, core.VolumeMount{Name: CoverVolumeName, MountPath: coverDir})
	}
	if r := config.InitResources; r != nil {
		ic.Resources = *r
	}
	if s := config.InitSecurityContext; s != nil {
		ic.SecurityContext = s
	}
	if config.RequireAuthoritativeRoutes {
		ic.SecurityContext = routeIntentInitSecurityContext(config.InitSecurityContext)
	}
	return ic
}

// ValidateRouteIntentInitSecurity rejects an explicit configuration that would
// leave protected workloads unable to program the required network redirect.
func (s *Sidecar) ValidateRouteIntentInitSecurity() error {
	if !s.RequireAuthoritativeRoutes || s.InitSecurityContext == nil {
		return nil
	}
	configured := s.InitSecurityContext
	if (configured.RunAsUser != nil && *configured.RunAsUser != 0) || (configured.RunAsNonRoot != nil && *configured.RunAsNonRoot) {
		return errors.New("authoritative HTTP routing requires the traffic-agent init container to run as UID 0")
	}
	if configured.Capabilities != nil && !slices.Contains(configured.Capabilities.Add, core.Capability("NET_ADMIN")) {
		return errors.New("authoritative HTTP routing requires NET_ADMIN for the traffic-agent init container")
	}
	return nil
}

func routeIntentInitSecurityContext(configured *core.SecurityContext) *core.SecurityContext {
	s := &core.SecurityContext{}
	if configured != nil {
		s = configured.DeepCopy()
	}
	// Kubernetes propagates the pod's runAsUser into containers unless it is
	// overridden. On containerd a non-root exec does not retain this capability
	// as effective, even when NET_ADMIN is present in its bounding set.
	if s.RunAsUser == nil {
		s.RunAsUser = new(int64(0))
	}
	if s.RunAsGroup == nil {
		s.RunAsGroup = new(int64(0))
	}
	if s.RunAsNonRoot == nil {
		s.RunAsNonRoot = new(false)
	}
	if s.AllowPrivilegeEscalation == nil {
		s.AllowPrivilegeEscalation = new(false)
	}
	if s.Privileged == nil {
		s.Privileged = new(false)
	}
	if s.Capabilities == nil {
		s.Capabilities = &core.Capabilities{Add: []core.Capability{"NET_ADMIN"}, Drop: []core.Capability{"ALL"}}
	}
	return s
}
