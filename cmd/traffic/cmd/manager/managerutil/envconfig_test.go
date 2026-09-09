package managerutil_test

import (
	"context"
	"log/slog"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/auth"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/maps"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func TestEnvconfig(t *testing.T) {
	// Default environment, providing what's necessary for the traffic-manager
	env := map[string]string{
		"REGISTRY":                        "ghcr.io/telepresenceio",
		"AGENT_ENVOY_ADMIN_PORT":          "19000",
		"AGENT_ENVOY_SERVER_PORT":         "18000",
		"AGENT_ENVOY_HTTP_IDLE_TIMEOUT":   "70s",
		"AGENT_INJECT_POLICY":             agentconfig.WhenEnabled.String(),
		"AGENT_INJECTOR_NAME":             "agent-injector",
		"AGENT_INJECTOR_SECRET":           "mutator-webhook-tls",
		"AGENT_PORT":                      "9900",
		"AGENT_ARRIVAL_TIMEOUT":           "45s",
		"CLIENT_CONNECTION_TTL":           (24 * time.Hour).String(),
		"CLIENT_DNS_EXCLUDE_SUFFIXES":     ".com .io .net .org .ru",
		"ENABLED_WORKLOAD_KINDS":          "Deployment StatefulSet ReplicaSet Rollout",
		"GRPC_MAX_RECEIVE_SIZE":           "4Mi",
		"LOG_LEVEL":                       "trace",
		"POD_IP":                          "203.0.113.18",
		"POD_CIDR_STRATEGY":               "auto",
		"SERVER_PORT":                     "8081",
		"MAX_NAMESPACE_SPECIFIC_WATCHERS": "10",
	}

	defaults := managerutil.Env{
		Registry:                     "ghcr.io/telepresenceio",
		AgentLogLevel:                slog.LevelDebug - 4,
		AgentPort:                    9900,
		AgentInjectorName:            "agent-injector",
		AgentInjectorSecret:          "mutator-webhook-tls",
		AgentInjectPolicy:            agentconfig.WhenEnabled,
		AgentArrivalTimeout:          45 * time.Second,
		ClientConnectionTTL:          24 * time.Hour,
		ClientDnsExcludeSuffixes:     []string{".com", ".io", ".net", ".org", ".ru"},
		LogLevel:                     clog.LevelTrace,
		GrpcMaxReceiveSize:           resource.MustParse("4Mi"),
		PodCidrStrategy:              "auto",
		PodIp:                        netip.AddrFrom4([4]byte{203, 0, 113, 18}),
		ServerPort:                   8081,
		EnabledWorkloadKinds:         []k8sapi.Kind{k8sapi.DeploymentKind, k8sapi.StatefulSetKind, k8sapi.ReplicaSetKind, k8sapi.RolloutKind},
		MaxNamespaceSpecificWatchers: 10,
		AgentInitContainerEnabled:    true,
		InterceptAllowGlobal:         true,
		AgentWatchRetryInterval:      10 * time.Second,
		AgentPreStopDrainTimeout:     2 * time.Minute,
		AgentConsumptionMetrics:      true,
		UsageReportingEnabled:        true,
		MutatorWebhookPort:           8443,
		TunnelQuicAgentPort:          7787,
		AuthenticationMode:           auth.ModePermissive,
	}

	testcases := map[string]struct {
		Input  map[string]string
		Output func(*managerutil.Env)
	}{
		"empty": {
			Input:  nil,
			Output: func(*managerutil.Env) {},
		},
		"simple": {
			Input: map[string]string{
				"AGENT_REGISTRY": "ghcr.io/telepresenceio",
			},
			Output: func(e *managerutil.Env) {
				e.AgentRegistry = "ghcr.io/telepresenceio"
			},
		},
		"routeIntentNamespaces": {
			Input: map[string]string{"ROUTE_INTENT_ENABLED": "true", "ROUTE_INTENT_NAMESPACES": "ambassador testing", "AGENT_ROUTE_INTENT_IMAGE": "registry.example/tel2@sha256:abc123"},
			Output: func(e *managerutil.Env) {
				e.RouteIntentEnabled = true
				e.RouteIntentNamespaces = []string{"ambassador", "testing"}
				e.AgentRouteIntentImage = "registry.example/tel2@sha256:abc123"
			},
		},
		"complex": {
			Input: map[string]string{
				"CLIENT_ROUTING_NEVER_PROXY_SUBNETS": "10.20.30.0/24 10.20.40.0/24",
			},
			Output: func(e *managerutil.Env) {
				a := netip.MustParsePrefix("10.20.30.0/24")
				b := netip.MustParsePrefix("10.20.40.0/24")
				e.ClientRoutingNeverProxySubnets = []netip.Prefix{a, b}
			},
		},
		"version": {
			Input: map[string]string{
				"COMPATIBILITY_VERSION": `2.15.3-rc.0`,
			},
			Output: func(e *managerutil.Env) {
				v := semver.MustParse("2.15.3-rc.0")
				e.CompatibilityVersion = &v
			},
		},
		"mountPolicies": {
			Input: map[string]string{
				"AGENT_MOUNT_POLICIES": `{"/home/bob":"remote","/home/alice":"local"}`,
			},
			Output: func(e *managerutil.Env) {
				e.AgentMountPolicies = types.MountPolicies{
					"/home/bob":   types.MountPolicyRemote,
					"/home/alice": types.MountPolicyLocal,
				}
			},
		},
		"resourceRequirements": {
			Input: map[string]string{
				"AGENT_RESOURCES": `{"requests":{"cpu":"100m","memory":"128Mi"},"limits":{"cpu":"200m","memory":"256Mi"}}`,
			},
			Output: func(e *managerutil.Env) {
				e.AgentResources = &corev1.ResourceRequirements{
					Limits: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:    resource.MustParse("200m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
					Requests: map[corev1.ResourceName]resource.Quantity{
						corev1.ResourceCPU:    resource.MustParse("100m"),
						corev1.ResourceMemory: resource.MustParse("128Mi"),
					},
				}
			},
		},
		"agent-image-pull-secrets": {
			Input: map[string]string{
				"AGENT_IMAGE_PULL_SECRETS": `[{"name":"my-secret"},{"name":"my-other-secret"}]`,
			},
			Output: func(e *managerutil.Env) {
				e.AgentImagePullSecrets = []corev1.LocalObjectReference{
					{
						Name: "my-secret",
					},
					{
						Name: "my-other-secret",
					},
				}
			},
		},
		"securityContext": {
			Input: map[string]string{
				"AGENT_SECURITY_CONTEXT": `{"runAsUser":1000,"runAsGroup":2000}`,
			},
			Output: func(e *managerutil.Env) {
				e.AgentSecurityContext = &corev1.SecurityContext{
					RunAsUser:  func(i int64) *int64 { return &i }(1000),
					RunAsGroup: func(i int64) *int64 { return &i }(2000),
				}
			},
		},
		"agent-pre-stop-drain-timeout": {
			Input: map[string]string{
				"AGENT_PRE_STOP_DRAIN_TIMEOUT": "45s",
			},
			Output: func(e *managerutil.Env) {
				e.AgentPreStopDrainTimeout = 45 * time.Second
			},
		},
		"agent-pre-stop-drain-disabled": {
			Input: map[string]string{
				"AGENT_PRE_STOP_DRAIN_TIMEOUT": "0s",
			},
			Output: func(e *managerutil.Env) {
				e.AgentPreStopDrainTimeout = 0
			},
		},
		"allow-global-intercepts-true": {
			Input: map[string]string{
				"INTERCEPT_ALLOW_GLOBAL": "true",
			},
			Output: func(e *managerutil.Env) {
				e.InterceptAllowGlobal = true
			},
		},
		"allow-global-intercepts-false": {
			Input: map[string]string{
				"INTERCEPT_ALLOW_GLOBAL": "false",
			},
			Output: func(e *managerutil.Env) {
				e.InterceptAllowGlobal = false
			},
		},
		"pod-host-ip": {
			Input: map[string]string{
				"POD_HOST_IP": "192.168.56.2",
			},
			Output: func(e *managerutil.Env) {
				e.PodHostIp = netip.AddrFrom4([4]byte{192, 168, 56, 2})
			},
		},
		"authentication-mode-enforcing": {
			Input: map[string]string{
				"AUTHENTICATION_MODE": "enforcing",
			},
			Output: func(e *managerutil.Env) {
				e.AuthenticationMode = auth.ModeEnforcing
			},
		},
		"authentication-mode-disabled": {
			Input: map[string]string{
				"AUTHENTICATION_MODE": "disabled",
			},
			Output: func(e *managerutil.Env) {
				e.AuthenticationMode = auth.ModeDisabled
			},
		},
	}

	for tcName, tc := range testcases {
		t.Run(tcName, func(t *testing.T) {
			t.Parallel()
			testEnv := maps.Copy(env)
			maps.Merge(testEnv, tc.Input)
			expected := defaults
			tc.Output(&expected)

			ctx, err := managerutil.LoadEnv(context.Background(), testEnv)
			require.NoError(t, err)
			actual := managerutil.GetEnv(ctx)
			assert.Equal(t, &expected, actual)
			assert.Equal(t, "", actual.QualifiedAgentImage())
		})
	}
}

func TestRouteIntentAgentImageOnlyOverridesEnabledNamespaces(t *testing.T) {
	const standard, candidate = "registry.example/tel2:stable", "registry.example/tel2@sha256:abc123"
	for _, test := range []struct {
		name, namespace, candidate, want string
		enabled                          bool
		scoped                           []string
	}{
		{name: "enabled included", namespace: "ambassador", candidate: candidate, enabled: true, scoped: []string{"ambassador"}, want: candidate},
		{name: "enabled excluded", namespace: "application", candidate: candidate, enabled: true, scoped: []string{"ambassador"}, want: standard},
		{name: "disabled ignored", namespace: "ambassador", candidate: candidate, scoped: []string{"ambassador"}, want: standard},
		{name: "unset ignored", namespace: "ambassador", enabled: true, scoped: []string{"ambassador"}, want: standard},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := &managerutil.Env{RouteIntentEnabled: test.enabled, AgentRouteIntentImage: test.candidate, RouteIntentNamespaces: test.scoped}
			ctx := managerutil.WithResolvedAgentImageRetriever(managerutil.WithEnv(t.Context(), e), managerutil.ImageFromEnv(standard))
			require.Equal(t, standard, managerutil.GetAgentImage(ctx), "the global image must not change")
			require.Equal(t, test.want, managerutil.GetAgentImageForNamespace(ctx, test.namespace))
			gc, err := e.GeneratorConfig(standard)
			require.NoError(t, err)
			require.Equal(t, test.want, gc.AgentImageForNamespace(test.namespace), "direct comparisons and generated image must match")
		})
	}
}

func TestEnvconfigPreStopDrainTimeout(t *testing.T) {
	env := map[string]string{
		"REGISTRY":    "ghcr.io/telepresenceio",
		"LOG_LEVEL":   "info",
		"SERVER_PORT": "8081",
	}

	ctx, err := managerutil.LoadEnv(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, 2*time.Minute, managerutil.GetEnv(ctx).AgentPreStopDrainTimeout)

	env["AGENT_PRE_STOP_DRAIN_TIMEOUT"] = "-1s"
	_, err = managerutil.LoadEnv(context.Background(), env)
	require.ErrorContains(t, err, "AGENT_PRE_STOP_DRAIN_TIMEOUT must not be negative")
}

func TestEnvconfigDelegatedDevboxProxyAudience(t *testing.T) {
	const staging = "11111111-1111-4111-8111-111111111111"
	const production = "22222222-2222-4222-8222-222222222222"
	const tenant = "33333333-3333-4333-8333-333333333333"
	const stageHost = "staging.proxy.example.test"
	const prodHost = "production.proxy.example.test"
	const stageURL = "https://" + stageHost + "/clusters/staging/apis/authentication.k8s.io/v1/selfsubjectreviews"
	const prodURL = "https://" + prodHost + "/clusters/staging/apis/authentication.k8s.io/v1/selfsubjectreviews"
	for _, tc := range []struct{ name, mode, endpoint, audience, tenant, authorities, err string }{
		{name: "default leaves advertisement off", mode: "permissive"},
		{name: "URL alone fails closed", mode: "permissive", endpoint: prodURL, err: "together"},
		{name: "staging permissive", mode: "permissive", endpoint: stageURL, audience: staging, tenant: tenant, authorities: stageHost + " secondary.proxy.example.test"},
		{name: "production enforcing", mode: "enforcing", endpoint: prodURL, audience: production, tenant: tenant, authorities: prodHost},
		{name: "missing endpoint", mode: "permissive", audience: staging, tenant: tenant, authorities: stageHost, err: "together"},
		{name: "missing tenant", mode: "permissive", endpoint: stageURL, audience: staging, authorities: stageHost, err: "together"},
		{name: "missing authorities", mode: "permissive", endpoint: stageURL, audience: staging, tenant: tenant, err: "together"},
		{name: "mismatch", mode: "permissive", endpoint: prodURL, audience: staging, tenant: tenant, authorities: stageHost, err: "does not match"},
		{name: "invalid UUID", mode: "enforcing", endpoint: stageURL, audience: "unexpected", tenant: tenant, authorities: stageHost, err: "canonical nonzero"},
		{name: "disabled", mode: "disabled", endpoint: stageURL, audience: staging, tenant: tenant, authorities: stageHost, err: "authentication mode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{
				"REGISTRY": "ghcr.io/telepresenceio", "LOG_LEVEL": "info", "SERVER_PORT": "8081", "AUTHENTICATION_MODE": tc.mode,
				"AUTH_DELEGATED_SELF_SUBJECT_REVIEW_URL": tc.endpoint, "AUTH_DELEGATED_DEVBOX_PROXY_AUDIENCE": tc.audience,
				"AUTH_DELEGATED_DEVBOX_PROXY_TENANT_ID": tc.tenant, "AUTH_DELEGATED_DEVBOX_PROXY_AUTHORITIES": tc.authorities,
			}
			ctx, err := managerutil.LoadEnv(t.Context(), env)
			if tc.err != "" {
				require.ErrorContains(t, err, "AUTH_DELEGATED_DEVBOX_PROXY_AUDIENCE")
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			parsed := managerutil.GetEnv(ctx)
			require.Equal(t, tc.endpoint, parsed.AuthDelegatedSelfSubjectReviewURL)
			require.Equal(t, tc.audience, parsed.AuthDelegatedDevboxProxyAudience)
			require.Equal(t, tc.tenant, parsed.AuthDelegatedDevboxProxyTenantID)
			if tc.authorities != "" {
				require.Equal(t, strings.Split(tc.authorities, " "), parsed.AuthDelegatedDevboxProxyAuthorities)
			} else {
				require.Empty(t, parsed.AuthDelegatedDevboxProxyAuthorities)
			}
		})
	}
}

func TestAuthoritativeRoutesRequireAgentInitContainer(t *testing.T) {
	for _, tc := range []struct {
		name, routeEnabled, agentStrict, initEnabled, err string
	}{
		{name: "strict defaults to enabled init", routeEnabled: "true", agentStrict: "true"},
		{name: "strict explicitly enabled init", routeEnabled: "true", agentStrict: "true", initEnabled: "true"},
		{
			name: "strict rejects disabled init", routeEnabled: "true", agentStrict: "true", initEnabled: "false",
			err: "AGENT_REQUIRE_AUTHORITATIVE_ROUTES requires AGENT_INIT_CONTAINER_ENABLED",
		},
		{name: "feature off may disable init", routeEnabled: "false", agentStrict: "false", initEnabled: "false"},
		{name: "non-strict may disable init", routeEnabled: "true", agentStrict: "false", initEnabled: "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{
				"REGISTRY": "ghcr.io/telepresenceio", "LOG_LEVEL": "info", "SERVER_PORT": "8081",
				"ROUTE_INTENT_ENABLED": tc.routeEnabled, "AGENT_REQUIRE_AUTHORITATIVE_ROUTES": tc.agentStrict,
			}
			if tc.initEnabled != "" {
				env["AGENT_INIT_CONTAINER_ENABLED"] = tc.initEnabled
			}
			ctx, err := managerutil.LoadEnv(t.Context(), env)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.initEnabled != "false", managerutil.GetEnv(ctx).AgentInitContainerEnabled)
		})
	}
}

func TestAuthoritativeRoutesValidateInitSecurityAtStartup(t *testing.T) {
	for _, tc := range []struct {
		security, wantError string
	}{
		{`{}`, ""},
		{`{"runAsUser":0,"runAsNonRoot":false,"capabilities":{"add":["NET_ADMIN"],"drop":["ALL"]}}`, ""},
		{`{"runAsUser":1000}`, "UID 0"},
		{`{"runAsNonRoot":true}`, "UID 0"},
		{`{"capabilities":{"drop":["ALL"]}}`, "NET_ADMIN"},
	} {
		t.Run(tc.security, func(t *testing.T) {
			env := map[string]string{
				"REGISTRY": "ghcr.io/telepresenceio", "LOG_LEVEL": "info", "SERVER_PORT": "8081",
				"ROUTE_INTENT_ENABLED": "true", "AGENT_REQUIRE_AUTHORITATIVE_ROUTES": "true", "AGENT_INIT_SECURITY_CONTEXT": tc.security,
			}
			_, err := managerutil.LoadEnv(t.Context(), env)
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError)
			}
			env["AGENT_REQUIRE_AUTHORITATIVE_ROUTES"] = "false"
			_, err = managerutil.LoadEnv(t.Context(), env)
			require.NoError(t, err)
		})
	}
}

func TestEnvconfigAgentArrivalTimeoutDefault(t *testing.T) {
	// AGENT_ARRIVAL_TIMEOUT must default to a positive value so that a
	// traffic-manager installed without the chart supplying it (e.g. an
	// agentInjector.enabled=false, nodeAgent.enabled=true install using an
	// older chart) never ends up with a zero-duration wait for an agent to
	// arrive.
	ctx, err := managerutil.LoadEnv(context.Background(), map[string]string{
		"REGISTRY":    "ghcr.io/telepresenceio",
		"LOG_LEVEL":   "info",
		"SERVER_PORT": "8081",
	})
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, managerutil.GetEnv(ctx).AgentArrivalTimeout)
}

func TestEnvconfigMutatorWebhookPortDefault(t *testing.T) {
	// MUTATOR_WEBHOOK_PORT must default to the same port the chart's
	// agentInjector.webhook.port default binds (8443): the chart only sets
	// this env var when agentInjector.enabled, but the plain-HTTP
	// /uninstall-only server started for a node-agent-only,
	// agentInjector.enabled=false install still needs to listen on the port
	// the agent-injector Service's targetPort names.
	ctx, err := managerutil.LoadEnv(context.Background(), map[string]string{
		"REGISTRY":    "ghcr.io/telepresenceio",
		"LOG_LEVEL":   "info",
		"SERVER_PORT": "8081",
	})
	require.NoError(t, err)
	assert.Equal(t, uint16(8443), managerutil.GetEnv(ctx).MutatorWebhookPort)
}

func TestEnvHostNetwork(t *testing.T) {
	podIP := netip.MustParseAddr("203.0.113.18")
	tests := []struct {
		name string
		env  managerutil.Env
		want bool
	}{
		{
			name: "pod IP equals host IP",
			env:  managerutil.Env{PodIp: podIP, PodHostIp: podIP},
			want: true,
		},
		{
			name: "pod IP differs from host IP",
			env:  managerutil.Env{PodIp: podIP, PodHostIp: netip.MustParseAddr("192.168.56.2")},
			want: false,
		},
		{
			name: "host IP unknown",
			env:  managerutil.Env{PodIp: podIP},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.env.HostNetwork())
		})
	}
}
