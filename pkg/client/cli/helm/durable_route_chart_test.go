package helm

import (
	"maps"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
)

func TestPackagedDurableRouteOptionsRenderAndRejectUnknownKeys(t *testing.T) {
	options := map[string]any{
		"enabled": true, "namespaces": []any{"application"}, "requireAuthoritativeAgents": true,
		"agentImage": "registry.example.test/telepresence/tel2:patched", "skipGatewayAck": false,
		"controllerServiceAccounts": []any{"system:serviceaccount:gateway-system:gateway-reconciler"},
	}
	wrap := func(route map[string]any) map[string]any { return map[string]any{"routeIntent": route} }
	require.NoError(t, renderCoreChart(t, wrap(options), true))
	deployment := renderPackagedRouteChart(t, wrap(options))["telepresence-oss/templates/deployment.yaml"]
	for _, name := range []string{
		"ROUTE_INTENT_ENABLED", "ROUTE_INTENT_NAMESPACES", "AGENT_ROUTE_INTENT_IMAGE",
		"AGENT_REQUIRE_AUTHORITATIVE_ROUTES", "ROUTE_INTENT_CONTROLLER_SERVICE_ACCOUNTS",
	} {
		require.Contains(t, deployment, "name: "+name)
	}
	require.NotContains(t, deployment, "name: ROUTE_INTENT_SKIP_GATEWAY_ACK")
	for key, value := range map[string]any{
		"unknown": true, "namespaces": []any{"Invalid"}, "controllerServiceAccounts": []any{"system:serviceaccount:invalid"},
	} {
		invalid := maps.Clone(options)
		invalid[key] = value
		require.ErrorContains(t, renderCoreChart(t, wrap(invalid), true), key)
	}
	invalid := maps.Clone(options)
	invalid["enabled"] = false
	require.ErrorContains(t, renderCoreChart(t, wrap(invalid), true), "requireAuthoritativeAgents requires routeIntent.enabled")
}

func renderPackagedRouteChart(t *testing.T, vals map[string]any) map[string]string {
	t.Helper()
	chart, err := loadCoreChart(semver.MustParse("2.31.0"))
	require.NoError(t, err)
	values, err := chartutil.ToRenderValues(chart, vals,
		chartutil.ReleaseOptions{Name: "traffic-manager", Namespace: "ambassador", IsInstall: true}, chartutil.DefaultCapabilities)
	require.NoError(t, err)
	rendered, err := engine.Engine{}.Render(chart, values)
	require.NoError(t, err)
	return rendered
}
