package helm

import (
	"maps"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
)

func TestPackagedDelegatedDevboxRequiresCompleteOperatorPolicy(t *testing.T) {
	const audience = "11111111-1111-4111-8111-111111111111"
	const tenant = "33333333-3333-4333-8333-333333333333"
	policy := map[string]any{
		"delegatedSelfSubjectReviewURL":   "https://staging.proxy.example.test/clusters/staging/apis/authentication.k8s.io/v1/selfsubjectreviews",
		"delegatedDevboxProxyAudience":    audience,
		"delegatedDevboxProxyTenantID":    tenant,
		"delegatedDevboxProxyAuthorities": []any{"staging.proxy.example.test", "secondary.proxy.example.test"},
	}
	wrap := func(policy map[string]any) map[string]any {
		return map[string]any{"security": map[string]any{"authentication": policy}}
	}
	for _, withSchema := range []bool{false, true} {
		require.NoError(t, renderCoreChart(t, nil, withSchema))
		require.NoError(t, renderCoreChart(t, wrap(policy), withSchema))
		for key := range policy {
			partial := maps.Clone(policy)
			delete(partial, key)
			err := renderCoreChart(t, wrap(partial), withSchema)
			require.ErrorContains(t, err, "requires delegatedSelfSubjectReviewURL")
		}
		disabled := maps.Clone(policy)
		disabled["mode"] = "disabled"
		require.ErrorContains(t, renderCoreChart(t, wrap(disabled), withSchema), "requires authentication mode")
	}
	for key, values := range map[string][]any{
		"delegatedDevboxProxyAudience":    {"api://" + audience, "invalid"},
		"delegatedDevboxProxyTenantID":    {"INVALID", "missing"},
		"delegatedDevboxProxyAuthorities": {[]any{"*.example.test"}, []any{"example.test:443"}, []any{"localhost"}, []any{"example.test", "example.test"}},
	} {
		for _, value := range values {
			invalid := maps.Clone(policy)
			invalid[key] = value
			require.ErrorContains(t, renderCoreChart(t, wrap(invalid), true), key)
		}
	}
	deployment := renderPackagedDelegatedChart(t, wrap(policy))["telepresence-oss/templates/deployment.yaml"]
	for _, want := range []string{
		"name: AUTH_DELEGATED_SELF_SUBJECT_REVIEW_URL", "name: AUTH_DELEGATED_DEVBOX_PROXY_AUDIENCE\n            value: \"" + audience + "\"",
		"name: AUTH_DELEGATED_DEVBOX_PROXY_TENANT_ID\n            value: \"" + tenant + "\"",
		"name: AUTH_DELEGATED_DEVBOX_PROXY_AUTHORITIES\n            value: \"staging.proxy.example.test secondary.proxy.example.test\"",
	} {
		require.Contains(t, deployment, want)
	}
}

func TestPackagedDurableRouteOptionsRenderAndRejectUnknownKeys(t *testing.T) {
	options := map[string]any{
		"enabled": true, "namespaces": []any{"application"}, "requireAuthoritativeAgents": true,
		"agentImage": "registry.example.test/telepresence/tel2:patched", "skipGatewayAck": false,
		"controllerServiceAccounts": []any{"system:serviceaccount:gateway-system:gateway-reconciler"},
	}
	wrap := func(route map[string]any) map[string]any { return map[string]any{"routeIntent": route} }
	require.NoError(t, renderCoreChart(t, wrap(options), true))
	deployment := renderPackagedDelegatedChart(t, wrap(options))["telepresence-oss/templates/deployment.yaml"]
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

func renderPackagedDelegatedChart(t *testing.T, vals map[string]any) map[string]string {
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
