package helm

import (
	"strings"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/yaml"
)

// renderCoreChart renders the embedded telepresence-oss chart with vals coalesced
// over the chart's own defaults and returns the render error (nil when every template
// renders). withSchema selects whether the chart's values schema validates vals
// first, as it does for every packaged install; rendering the bare chart directory
// (helm template charts/telepresence-oss) has no values.schema.json and skips it,
// which is the path that reaches the templates' own guards.
func renderCoreChart(t *testing.T, vals map[string]any, withSchema bool) error {
	t.Helper()
	chrt, err := loadCoreChart(semver.MustParse("2.32.0"))
	require.NoError(t, err)
	if !withSchema {
		chrt.Schema = nil
	}
	rv, err := chartutil.ToRenderValues(chrt, vals,
		chartutil.ReleaseOptions{Name: "traffic-manager", Namespace: "ambassador", IsInstall: true},
		chartutil.DefaultCapabilities)
	if err != nil {
		return err
	}
	_, err = engine.Engine{}.Render(chrt, rv)
	return err
}

// TestQuicTunnelRequiresSingleManagerReplica pins two defenses against a
// multi-replica traffic-manager: the values schema pins replicaCount to 1,
// and the StatefulSet template also refuses replicaCount > 1 without the schema.
func TestQuicTunnelRequiresSingleManagerReplica(t *testing.T) {
	require.NoError(t, renderCoreChart(t, map[string]any{
		"quicTunnel": map[string]any{"enabled": true},
	}, true), "quicTunnel.enabled with the default single replica must render")

	err := renderCoreChart(t, map[string]any{"replicaCount": 2}, true)
	require.Error(t, err, "the values schema must reject scaling the traffic-manager")
	require.ErrorContains(t, err, "replicaCount")

	err = renderCoreChart(t, map[string]any{"replicaCount": 2}, false)
	require.Error(t, err, "scaling must refuse to render even without the schema")
	require.ErrorContains(t, err, "replicaCount must be 1")
}

func TestQuicForwarderPodLabelsSchema(t *testing.T) {
	t.Run("accepts string-valued pod labels", func(t *testing.T) {
		err := renderCoreChart(t, map[string]any{
			"quicTunnel": map[string]any{
				"enabled": true,
				"forwarder": map[string]any{
					"podLabels": map[string]any{"example.com/mesh-injection": "enabled"},
				},
			},
		}, true)
		require.NoError(t, err)
	})

	t.Run("rejects non-object pod labels", func(t *testing.T) {
		err := renderCoreChart(t, map[string]any{
			"quicTunnel": map[string]any{
				"enabled":   true,
				"forwarder": map[string]any{"podLabels": "enabled"},
			},
		}, true)
		require.Error(t, err)
		require.ErrorContains(t, err, "podLabels")
	})

	t.Run("rejects non-string pod label values", func(t *testing.T) {
		err := renderCoreChart(t, map[string]any{
			"quicTunnel": map[string]any{
				"enabled": true,
				"forwarder": map[string]any{
					"podLabels": map[string]any{"example.com/mesh-injection": true},
				},
			},
		}, true)
		require.Error(t, err)
		require.ErrorContains(t, err, "podLabels")
	})

	t.Run("rejects unknown forwarder properties", func(t *testing.T) {
		err := renderCoreChart(t, map[string]any{
			"quicTunnel": map[string]any{
				"enabled":   true,
				"forwarder": map[string]any{"unknownProperty": "value"},
			},
		}, true)
		require.Error(t, err)
		require.ErrorContains(t, err, "unknownProperty")
	})
}

func TestAgentPreStopDrainTimeout(t *testing.T) {
	for _, timeout := range []string{"0s", "25s", "2m", "1m30s"} {
		t.Run(timeout, func(t *testing.T) {
			require.NoError(t, renderCoreChart(t, map[string]any{
				"agent": map[string]any{"preStopDrainTimeout": timeout},
			}, true))
		})
	}

	for _, timeout := range []string{"-1s", "+1s", "2", "2minutes"} {
		t.Run("invalid "+timeout, func(t *testing.T) {
			err := renderCoreChart(t, map[string]any{
				"agent": map[string]any{"preStopDrainTimeout": timeout},
			}, true)
			require.Error(t, err)
			require.ErrorContains(t, err, "preStopDrainTimeout")
		})
	}
}

func TestAgentPreStopDrainTimeoutManagerEnvironment(t *testing.T) {
	tests := []struct {
		name  string
		vals  map[string]any
		value string
	}{
		{
			name:  "default",
			value: "2m",
		},
		{
			name:  "configured",
			vals:  map[string]any{"agent": map[string]any{"preStopDrainTimeout": "45s"}},
			value: "45s",
		},
		{
			name:  "disabled",
			vals:  map[string]any{"agent": map[string]any{"preStopDrainTimeout": "0s"}},
			value: "0s",
		},
		{
			name: "injector disabled",
			vals: map[string]any{"agentInjector": map[string]any{"enabled": false}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chrt, err := loadCoreChart(semver.MustParse("2.32.0"))
			require.NoError(t, err)
			values, err := chartutil.ToRenderValues(chrt, tt.vals,
				chartutil.ReleaseOptions{Name: "traffic-manager", Namespace: "ambassador", IsInstall: true},
				chartutil.DefaultCapabilities)
			require.NoError(t, err)
			rendered, err := engine.Engine{}.Render(chrt, values)
			require.NoError(t, err)
			const managerTemplate = "telepresence-oss/templates/statefulset.yaml"
			require.Contains(t, rendered, managerTemplate)
			manifest := rendered[managerTemplate]
			lf := strings.ReplaceAll(manifest, "\r\n", "\n")
			for _, format := range []struct{ name, manifest string }{
				{"rendered", manifest}, {"LF", lf}, {"CRLF", strings.ReplaceAll(lf, "\n", "\r\n")},
			} {
				t.Run(format.name, func(t *testing.T) {
					var statefulSet appsv1.StatefulSet
					require.NoError(t, yaml.Unmarshal([]byte(format.manifest), &statefulSet))
					require.Equal(t, "StatefulSet", statefulSet.Kind)
					var foundManager, foundVariable bool
					var actual string
					for _, container := range statefulSet.Spec.Template.Spec.Containers {
						if container.Name != "traffic-manager" {
							continue
						}
						foundManager = true
						for _, env := range container.Env {
							if env.Name == "AGENT_PRE_STOP_DRAIN_TIMEOUT" {
								require.False(t, foundVariable, "drain timeout must be rendered exactly once")
								foundVariable, actual = true, env.Value
							}
						}
					}
					require.True(t, foundManager, "manager container must exist")
					if tt.value == "" {
						require.False(t, foundVariable)
						return
					}
					require.True(t, foundVariable)
					require.Equal(t, tt.value, actual)
				})
			}
		})
	}
}
