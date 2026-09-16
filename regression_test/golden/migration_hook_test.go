package golden

import (
	"cmp"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/releaseutil"
	batchv1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/yaml"
)

func TestMigrationAuthorizationRunsBeforeUpgradeJob(t *testing.T) {
	out := renderChart(t, map[string]any{})
	hooks, manifests, err := releaseutil.SortManifests(
		map[string]string{preUpgradeHookTpl: out[preUpgradeHookTpl]},
		chartutil.DefaultCapabilities.APIVersions, releaseutil.InstallOrder)
	require.NoError(t, err)
	require.Empty(t, manifests, "authorization must be available before regular release manifests are installed")
	require.Len(t, hooks, 4)
	slices.SortFunc(hooks, func(a, b *release.Hook) int { return cmp.Compare(a.Weight, b.Weight) })

	want := []struct {
		kind string
		name string
	}{
		{"ServiceAccount", "traffic-manager-migrate-hook"},
		{"Role", "traffic-manager-migrate-hook"},
		{"RoleBinding", "traffic-manager-migrate-hook"},
		{"Job", "traffic-manager-migrate-statefulset"},
	}
	for i, h := range hooks {
		require.Equal(t, want[i].kind, h.Kind)
		require.Equal(t, want[i].name, h.Name)
		require.Equal(t, []release.HookEvent{release.HookPreUpgrade}, h.Events)
		if i > 0 {
			require.Less(t, hooks[i-1].Weight, h.Weight, "each prerequisite must be installed before its dependent hook")
		}
		if i < len(hooks)-1 {
			require.Equal(t, []release.HookDeletePolicy{release.HookBeforeHookCreation}, h.DeletePolicies,
				"Helm must retain successful authorization hooks for the migration job and replace them on the next upgrade")
		}
	}

	jobHook := hooks[len(hooks)-1]
	require.Equal(t, []release.HookDeletePolicy{release.HookBeforeHookCreation, release.HookSucceeded}, jobHook.DeletePolicies)
	var job batchv1.Job
	require.NoError(t, yaml.Unmarshal([]byte(jobHook.Manifest), &job))
	require.Equal(t, hooks[0].Name, job.Spec.Template.Spec.ServiceAccountName)
}

func TestRbacOnlyOmitsUpgradeMigration(t *testing.T) {
	out := renderChart(t, map[string]any{"rbac": map[string]any{"only": true}})
	require.False(t, rendered(out, preUpgradeHookTpl))
}
