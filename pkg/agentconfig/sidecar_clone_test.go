package agentconfig

import (
	"net/netip"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func cloneTestSidecar() *Sidecar {
	return &Sidecar{
		AgentName: "echo", Namespace: "testing",
		PullSecrets:     []core.LocalObjectReference{{Name: "pull"}},
		MountPolicies:   types.MountPolicies{"outer": types.MountPolicyLocal},
		MeshDialSubnets: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/24")},
		Resources: &core.ResourceRequirements{
			Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("200m")},
			Claims: []core.ResourceClaim{{Name: "agent-claim"}},
		},
		InitResources: &core.ResourceRequirements{
			Requests: core.ResourceList{core.ResourceMemory: resource.MustParse("32Mi")},
			Claims:   []core.ResourceClaim{{Name: "init-claim"}},
		},
		SecurityContext: &core.SecurityContext{
			RunAsUser:      ptr.To(int64(1000)),
			Capabilities:   &core.Capabilities{Add: []core.Capability{"NET_ADMIN"}},
			SeccompProfile: &core.SeccompProfile{Type: core.SeccompProfileTypeLocalhost, LocalhostProfile: ptr.To("agent-profile")},
		},
		InitSecurityContext: &core.SecurityContext{
			RunAsGroup:     ptr.To(int64(1001)),
			Capabilities:   &core.Capabilities{Drop: []core.Capability{"ALL"}},
			SeccompProfile: &core.SeccompProfile{Type: core.SeccompProfileTypeLocalhost, LocalhostProfile: ptr.To("init-profile")},
		},
		Containers: []*Container{{
			Name: "app", Replace: ReplacePolicyIntercept,
			Mounts:     types.MountPolicies{"inner": types.MountPolicyRemoteReadOnly},
			MountPaths: []string{"/data"},
			Intercepts: []*Intercept{{ServiceName: "echo", ContainerPort: 8080, ServicePort: 80}},
		}},
	}
}

func TestSidecarCloneLeavesSourceAndAllMutableFieldsIndependent(t *testing.T) {
	t.Parallel()
	source := cloneTestSidecar()
	before := cloneTestSidecar()
	container := source.Containers[0]
	intercept := container.Intercepts[0]
	cloned := source.Clone()
	require.NotNil(t, cloned)
	assert.Equal(t, before, cloned)
	assert.Same(t, container, source.Containers[0])
	assert.Same(t, intercept, source.Containers[0].Intercepts[0])
	assert.NotSame(t, source.Containers[0], cloned.Containers[0])
	assert.NotSame(t, source.Containers[0].Intercepts[0], cloned.Containers[0].Intercepts[0])
	assert.NotSame(t, source.Resources, cloned.Resources)
	assert.NotSame(t, source.InitResources, cloned.InitResources)
	assert.NotSame(t, source.SecurityContext, cloned.SecurityContext)
	assert.NotSame(t, source.InitSecurityContext, cloned.InitSecurityContext)

	cloned.PullSecrets[0].Name = "changed-pull"
	cloned.MountPolicies["outer"] = types.MountPolicyIgnore
	cloned.MeshDialSubnets[0] = netip.MustParsePrefix("192.0.2.0/24")
	cloned.Resources.Limits[core.ResourceCPU] = resource.MustParse("500m")
	cloned.Resources.Claims[0].Name = "changed-agent-claim"
	cloned.InitResources.Requests[core.ResourceMemory] = resource.MustParse("64Mi")
	cloned.InitResources.Claims[0].Name = "changed-init-claim"
	*cloned.SecurityContext.RunAsUser = 2000
	cloned.SecurityContext.Capabilities.Add[0] = "NET_RAW"
	*cloned.SecurityContext.SeccompProfile.LocalhostProfile = "changed-agent-profile"
	*cloned.InitSecurityContext.RunAsGroup = 2001
	cloned.InitSecurityContext.Capabilities.Drop[0] = "SYS_ADMIN"
	*cloned.InitSecurityContext.SeccompProfile.LocalhostProfile = "changed-init-profile"
	cloned.Containers[0].Replace = ReplacePolicyContainer
	cloned.Containers[0].Mounts["inner"] = types.MountPolicyIgnore
	cloned.Containers[0].MountPaths[0] = "/changed"
	cloned.Containers[0].Intercepts[0].ContainerPort = 9090
	assert.Equal(t, before, source)
	assert.NotEqual(t, source, cloned)
}

func TestSidecarClonePreservesNilAndAllocatedEmptyFields(t *testing.T) {
	t.Parallel()
	t.Run("nil receiver", func(t *testing.T) {
		var source, cloned *Sidecar
		if assert.NotPanics(t, func() { cloned = source.Clone() }) {
			assert.Nil(t, cloned)
		}
	})
	t.Run("nil fields", func(t *testing.T) {
		source := &Sidecar{Containers: []*Container{{Name: "app"}}}
		assert.Equal(t, source, source.Clone())
	})
	t.Run("allocated empty fields", func(t *testing.T) {
		source := &Sidecar{
			PullSecrets: []core.LocalObjectReference{}, MeshDialSubnets: []netip.Prefix{}, MountPolicies: types.MountPolicies{},
			Resources: &core.ResourceRequirements{}, InitResources: &core.ResourceRequirements{},
			SecurityContext: &core.SecurityContext{}, InitSecurityContext: &core.SecurityContext{},
			Containers: []*Container{{Intercepts: []*Intercept{}, Mounts: types.MountPolicies{}, MountPaths: []string{}}},
		}
		cloned := source.Clone()
		assert.Equal(t, source, cloned)
		cloned.MountPolicies["outer"] = types.MountPolicyIgnore
		cloned.Containers[0].Mounts["inner"] = types.MountPolicyIgnore
		assert.Empty(t, source.MountPolicies)
		assert.Empty(t, source.Containers[0].Mounts)
	})
	t.Run("nil elements", func(t *testing.T) {
		source := &Sidecar{Containers: []*Container{nil, {Intercepts: []*Intercept{nil, {ServiceName: "echo"}}}}}
		var cloned *Sidecar
		if assert.NotPanics(t, func() { cloned = source.Clone() }) {
			assert.Equal(t, source, cloned)
			assert.Nil(t, cloned.Containers[0])
			assert.Nil(t, cloned.Containers[1].Intercepts[0])
			assert.NotSame(t, source.Containers[1].Intercepts[1], cloned.Containers[1].Intercepts[1])
		}
	})
}

func TestSidecarCloneIsSafeWhileOtherReadersUseSource(t *testing.T) {
	t.Parallel()
	source := cloneTestSidecar()
	before := cloneTestSidecar()
	const workers = 6
	const iterations = 60
	start := make(chan struct{})
	failures := make(chan error, iterations)
	var group sync.WaitGroup
	group.Go(func() {
		<-start
		for range iterations {
			if _, err := MarshalTight(source); err != nil {
				failures <- err
			}
		}
	})
	for range workers {
		group.Go(func() {
			<-start
			for range iterations {
				cloned := source.Clone()
				cloned.Containers[0].Replace = ReplacePolicyContainer
				cloned.Containers[0].Intercepts[0].ServicePort = 443
			}
		})
	}
	close(start)
	group.Wait()
	close(failures)
	for failure := range failures {
		assert.NoError(t, failure)
	}
	assert.Equal(t, before, source)
}
