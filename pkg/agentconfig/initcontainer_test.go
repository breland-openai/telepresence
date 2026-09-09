package agentconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
)

func envValue(ic *core.Container, name string) (string, bool) {
	for _, env := range ic.Env {
		if env.Name == name {
			return env.Value, true
		}
	}
	return "", false
}

func volumeMount(ic *core.Container, name string) (core.VolumeMount, bool) {
	for _, vm := range ic.VolumeMounts {
		if vm.Name == name {
			return vm, true
		}
	}
	return core.VolumeMount{}, false
}

func TestInitContainerSetsAgentUIDAndGID(t *testing.T) {
	uid := int64(1000)
	gid := int64(7439)
	ic := InitContainer(&Sidecar{AgentImage: "traffic-agent"}, &core.SecurityContext{RunAsUser: &uid, RunAsGroup: &gid}, "")

	if v, ok := envValue(ic, EnvAgentUID); !ok || v != "1000" {
		t.Fatalf("%s = %q, want 1000", EnvAgentUID, v)
	}
	if v, ok := envValue(ic, EnvAgentGID); !ok || v != "7439" {
		t.Fatalf("%s = %q, want 7439", EnvAgentGID, v)
	}
}

func TestInitContainerSetsCoverDir(t *testing.T) {
	ic := InitContainer(&Sidecar{AgentImage: "traffic-agent"}, nil, "/rtest-coverage")

	if v, ok := envValue(ic, "GOCOVERDIR"); !ok || v != "/rtest-coverage" {
		t.Fatalf("GOCOVERDIR = %q, want /rtest-coverage", v)
	}
	vm, ok := volumeMount(ic, CoverVolumeName)
	if !ok || vm.MountPath != "/rtest-coverage" {
		t.Fatalf("%s mount = %+v, want MountPath /rtest-coverage", CoverVolumeName, vm)
	}
}

func TestInitContainerOmitsCoverDirWhenUnset(t *testing.T) {
	ic := InitContainer(&Sidecar{AgentImage: "traffic-agent"}, nil, "")

	if v, ok := envValue(ic, "GOCOVERDIR"); ok {
		t.Fatalf("GOCOVERDIR = %q, want unset", v)
	}
	if _, ok := volumeMount(ic, CoverVolumeName); ok {
		t.Fatalf("%s mount present, want absent", CoverVolumeName)
	}
}

func TestInitContainerOmitsAgentUIDWhenUnknown(t *testing.T) {
	ic := InitContainer(&Sidecar{AgentImage: "traffic-agent"}, nil, "")

	if v, ok := envValue(ic, EnvAgentUID); ok {
		t.Fatalf("%s = %q, want unset", EnvAgentUID, v)
	}
	if v, ok := envValue(ic, EnvAgentGID); ok {
		t.Fatalf("%s = %q, want unset", EnvAgentGID, v)
	}
}

func TestInitContainerOmitsAgentUIDWhenOnlyGroupIsKnown(t *testing.T) {
	gid := int64(7439)
	ic := InitContainer(&Sidecar{AgentImage: "traffic-agent"}, &core.SecurityContext{RunAsGroup: &gid}, "")

	if v, ok := envValue(ic, EnvAgentUID); ok {
		t.Fatalf("%s = %q, want unset", EnvAgentUID, v)
	}
	if v, ok := envValue(ic, EnvAgentGID); !ok || v != "7439" {
		t.Fatalf("%s = %q, want 7439", EnvAgentGID, v)
	}
}

func TestAuthoritativeInitDoesNotInheritNonRootAgentIdentity(t *testing.T) {
	sidecar := &Sidecar{AgentImage: "traffic-agent", RequireAuthoritativeRoutes: true}
	agent := &core.SecurityContext{RunAsUser: new(int64(1000)), RunAsGroup: new(int64(7439)), RunAsNonRoot: new(true)}
	init := InitContainer(sidecar, agent, "")
	require.NoError(t, sidecar.ValidateRouteIntentInitSecurity())
	require.Equal(t, int64(0), *init.SecurityContext.RunAsUser)
	require.Equal(t, int64(0), *init.SecurityContext.RunAsGroup)
	require.False(t, *init.SecurityContext.RunAsNonRoot)
	require.False(t, *init.SecurityContext.AllowPrivilegeEscalation)
	require.False(t, *init.SecurityContext.Privileged)
	require.Equal(t, &core.Capabilities{Add: []core.Capability{"NET_ADMIN"}, Drop: []core.Capability{"ALL"}}, init.SecurityContext.Capabilities)
	uid, _ := envValue(init, EnvAgentUID)
	gid, _ := envValue(init, EnvAgentGID)
	require.Equal(t, "1000", uid)
	require.Equal(t, "7439", gid)
	require.Equal(t, int64(1000), *agent.RunAsUser)
	legacy := InitContainer(&Sidecar{}, agent, "")
	require.Nil(t, legacy.SecurityContext.RunAsUser)
	require.Nil(t, legacy.SecurityContext.RunAsNonRoot)
}

func TestAuthoritativeInitSecurityOverridesAreValidatedAndNotMutated(t *testing.T) {
	for name, context := range map[string]*core.SecurityContext{
		"non-root user":      {RunAsUser: new(int64(1000))},
		"require non-root":   {RunAsNonRoot: new(true)},
		"missing capability": {Capabilities: &core.Capabilities{Drop: []core.Capability{"ALL"}}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, (&Sidecar{RequireAuthoritativeRoutes: true, InitSecurityContext: context}).ValidateRouteIntentInitSecurity())
			require.NoError(t, (&Sidecar{InitSecurityContext: context}).ValidateRouteIntentInitSecurity())
		})
	}
	custom := &core.SecurityContext{RunAsGroup: new(int64(42)), ReadOnlyRootFilesystem: new(true)}
	sidecar := &Sidecar{RequireAuthoritativeRoutes: true, InitSecurityContext: custom}
	require.NoError(t, sidecar.ValidateRouteIntentInitSecurity())
	init := InitContainer(sidecar, nil, "")
	require.Equal(t, int64(42), *init.SecurityContext.RunAsGroup)
	require.True(t, *init.SecurityContext.ReadOnlyRootFilesystem)
	require.Nil(t, custom.Capabilities)
	require.Nil(t, custom.RunAsUser)
	custom.Capabilities = &core.Capabilities{Add: []core.Capability{"NET_ADMIN"}, Drop: []core.Capability{"ALL"}}
	require.NoError(t, sidecar.ValidateRouteIntentInitSecurity())
	init = InitContainer(sidecar, nil, "")
	init.SecurityContext.Capabilities.Add[0] = "NET_RAW"
	require.Equal(t, core.Capability("NET_ADMIN"), custom.Capabilities.Add[0])
}
