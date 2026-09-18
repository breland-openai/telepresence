package golden

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExternalEndpointDisabledByDefault asserts that a default render omits
// the external Service/Certificate, containerPort/env, and volume/mount.
func TestExternalEndpointDisabledByDefault(t *testing.T) {
	out := renderChart(t, map[string]any{})
	if rendered(out, externalEndpointTpl) {
		t.Fatalf("%s rendered under default values:\n%s", externalEndpointTpl, out[externalEndpointTpl])
	}
	doc := out[statefulsetTpl]
	for _, absent := range []string{"EXTERNAL_PORT", "EXTERNAL_TLS_CERT_DIR", "name: external", "external-tls"} {
		if strings.Contains(doc, absent) {
			t.Errorf("%s: %q rendered under default values", statefulsetTpl, absent)
		}
	}
}

// TestExternalEndpointSecretName asserts that an explicit tls.secretName
// renders the external Service, env/containerPort, and a volume sourced
// from that Secret.
func TestExternalEndpointSecretName(t *testing.T) {
	out := renderChart(t, map[string]any{
		"security": map[string]any{
			"authentication": map[string]any{"mode": "enforcing"},
		},
		"externalEndpoint": map[string]any{
			"enabled": true,
			"port":    8444,
			"tls": map[string]any{
				"secretName": "rtest-external-tls",
			},
		},
	})
	if !rendered(out, externalEndpointTpl) {
		t.Fatalf("%s did not render", externalEndpointTpl)
	}
	svcDoc := out[externalEndpointTpl]
	for _, want := range []string{
		"kind: Service",
		"name: traffic-manager-external\n",
		"type: LoadBalancer",
		"port: 443",
		"targetPort: external",
	} {
		if !strings.Contains(svcDoc, want) {
			t.Errorf("%s: %q not rendered:\n%s", externalEndpointTpl, want, svcDoc)
		}
	}
	if strings.Contains(svcDoc, "kind: Certificate") {
		t.Errorf("%s: Certificate rendered without tls.certManager.enabled:\n%s", externalEndpointTpl, svcDoc)
	}

	ssDoc := out[statefulsetTpl]
	env := parseEnv(ssDoc)
	if v := env["EXTERNAL_PORT"]; v != "8444" {
		t.Errorf("EXTERNAL_PORT = %q, want 8444", v)
	}
	if v := env["EXTERNAL_TLS_CERT_DIR"]; v != "/var/run/secrets/telepresence.io/external-tls" {
		t.Errorf("EXTERNAL_TLS_CERT_DIR = %q, want /var/run/secrets/telepresence.io/external-tls", v)
	}
	for _, want := range []string{
		"- name: external\n            containerPort: 8444",
		"- name: external-tls\n              mountPath: /var/run/secrets/telepresence.io/external-tls\n              readOnly: true",
		"- name: external-tls\n          secret:\n            secretName: rtest-external-tls",
	} {
		if !strings.Contains(ssDoc, want) {
			t.Errorf("%s: %q not rendered:\n%s", statefulsetTpl, want, ssDoc)
		}
	}
}

// TestExternalEndpointCertManager asserts that tls.certManager.enabled
// renders a Certificate with the configured dnsNames/issuerRef, and that
// the StatefulSet volume falls back to its Secret name.
func TestExternalEndpointCertManager(t *testing.T) {
	out := renderChart(t, map[string]any{
		"security": map[string]any{
			"authentication": map[string]any{"mode": "enforcing"},
		},
		"externalEndpoint": map[string]any{
			"enabled": true,
			"tls": map[string]any{
				"certManager": map[string]any{
					"enabled":   true,
					"dnsNames":  []string{"external.rtest.example.com"},
					"issuerRef": map[string]any{"name": "rtest-issuer", "kind": "ClusterIssuer"},
				},
			},
		},
	})
	doc := out[externalEndpointTpl]
	for _, want := range []string{
		"kind: Certificate",
		"name: traffic-manager-external-tls",
		"secretName: traffic-manager-external-tls",
		"external.rtest.example.com",
		"kind: ClusterIssuer",
		"name: rtest-issuer",
	} {
		if !strings.Contains(doc, want) {
			t.Errorf("%s: %q not rendered:\n%s", externalEndpointTpl, want, doc)
		}
	}

	ssDoc := out[statefulsetTpl]
	if !strings.Contains(ssDoc, "secretName: traffic-manager-external-tls") {
		t.Errorf("%s: volume did not fall back to traffic-manager-external-tls:\n%s", statefulsetTpl, ssDoc)
	}
}

// TestExternalEndpointOmitsConnectPortForward asserts that with an external
// endpoint published, the connect Role drops the pods/portforward rule under
// required grant "telepresence" but keeps it when the grant is "portforward".
func TestExternalEndpointOmitsConnectPortForward(t *testing.T) {
	subjects := []map[string]any{{
		"kind":      "ServiceAccount",
		"name":      "rtest-golden",
		"namespace": releaseNamespace,
	}}
	vals := func(grant string) map[string]any {
		return map[string]any{
			"security": map[string]any{
				"authentication": map[string]any{"mode": "enforcing"},
				"authorization":  map[string]any{"requiredGrant": grant},
			},
			"externalEndpoint": map[string]any{
				"enabled": true, "disablePortForwardRbac": true,
				"tls": map[string]any{"secretName": "rtest-external-tls"},
			},
			"clientRbac": map[string]any{"create": true, "subjects": subjects},
		}
	}

	t.Run("requiredGrant=telepresence", func(t *testing.T) {
		out := renderChart(t, vals("telepresence"))
		if !rendered(out, clientConnectTpl) {
			t.Fatalf("%s did not render", clientConnectTpl)
		}
		doc := out[clientConnectTpl]
		if strings.Contains(doc, "pods/portforward") {
			t.Errorf("%s: pods/portforward rendered with an external endpoint published:\n%s", clientConnectTpl, doc)
		}
		if !strings.Contains(doc, `resources: ["connections"]`) {
			t.Errorf("%s: connections rule missing:\n%s", clientConnectTpl, doc)
		}
	})

	t.Run("requiredGrant=portforward", func(t *testing.T) {
		out := renderChart(t, vals("portforward"))
		if !rendered(out, clientConnectTpl) {
			t.Fatalf("%s did not render", clientConnectTpl)
		}
		doc := out[clientConnectTpl]
		if !strings.Contains(doc, "resourceNames:\n      - traffic-manager-0") {
			t.Errorf("%s: named pods/portforward grant missing; the portforward grant reviews possession of it:\n%s", clientConnectTpl, doc)
		}
		if strings.Contains(doc, `resources: ["connections"]`) {
			t.Errorf("%s: connections rule rendered with requiredGrant=portforward:\n%s", clientConnectTpl, doc)
		}
		for _, absent := range []string{`resources: ["pods"]`, `resources: ["services"]`} {
			if strings.Contains(doc, absent) {
				t.Errorf("%s: discovery rule %q rendered with an external endpoint published:\n%s", clientConnectTpl, absent, doc)
			}
		}
	})
}

// TestExternalEndpointOmitsInterceptPortForward asserts that with an external
// endpoint published, the per-namespace intercept Roles withhold the
// mechanical pods/portforward grant -- clients never port-forward in external
// mode -- except under required grant "portforward", where possession of it
// is the attachment policy. The attachments grant is unaffected.
func TestExternalEndpointOmitsInterceptPortForward(t *testing.T) {
	subjects := []map[string]any{{
		"kind":      "ServiceAccount",
		"name":      "rtest-golden",
		"namespace": releaseNamespace,
	}}
	for _, grant := range []string{"any", "telepresence", "portforward"} {
		t.Run("requiredGrant="+grant, func(t *testing.T) {
			out := renderChart(t, map[string]any{
				"security": map[string]any{
					"authentication": map[string]any{"mode": "enforcing"},
					"authorization":  map[string]any{"requiredGrant": grant},
				},
				"externalEndpoint": map[string]any{
					"enabled": true, "disablePortForwardRbac": true,
					"tls": map[string]any{"secretName": "rtest-external-tls"},
				},
				"clientRbac": map[string]any{"create": true, "subjects": subjects},
			})
			if !rendered(out, clientClusterScopeTpl) {
				t.Fatalf("%s did not render", clientClusterScopeTpl)
			}
			assertInterceptRules(t, clientClusterScopeTpl, out[clientClusterScopeTpl], grant, true)
		})
	}
}

func TestExternalEndpointCoexistsWithNonEnforcingInternalListener(t *testing.T) {
	for _, mode := range []string{"permissive", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			out := renderChart(t, map[string]any{
				"security": map[string]any{
					"authentication": map[string]any{"mode": mode},
				},
				"clientRbac": map[string]any{"create": true, "subjects": []map[string]any{{"kind": "ServiceAccount", "name": "rtest-golden", "namespace": releaseNamespace}}},
				"externalEndpoint": map[string]any{
					"enabled": true,
					"tls":     map[string]any{"secretName": "rtest-external-tls"},
				},
			})
			require.True(t, rendered(out, externalEndpointTpl))
			require.True(t, rendered(out, x509Tpl), "external listener needs the cluster client CA independent of internal mode")
			require.Equal(t, mode, parseEnv(out[statefulsetTpl])["AUTHENTICATION_MODE"])
			require.Contains(t, out[clientConnectTpl], "pods/portforward", "retain old clients' bootstrap RBAC by default")
			assertInterceptRules(t, clientClusterScopeTpl, out[clientClusterScopeTpl], "any", false)
		})
	}
}

func TestExternalAuthenticationWebhook(t *testing.T) {
	values := func(webhook map[string]any) map[string]any {
		return map[string]any{"externalEndpoint": map[string]any{
			"enabled": true, "tls": map[string]any{"secretName": "external-tls"}, "authenticationWebhook": webhook,
		}}
	}
	for _, tc := range []struct {
		name        string
		credentials map[string]any
		ca          map[string]any
		expected    []string
		absent      string
	}{
		{
			"projected service account",
			map[string]any{"serviceAccountTokenAudience": "review-caller"},
			nil,
			[]string{"serviceAccountToken:", `audience: "review-caller"`, "expirationSeconds: 3600", "path: token"},
			"name: auth-webhook-ca\n",
		},
		{
			"secret and custom ca",
			map[string]any{"secretName": "review-caller-secret", "secretKey": "credential"},
			map[string]any{"secretName": "review-ca", "secretKey": "roots"},
			[]string{`secretName: "review-caller-secret"`, `key: "credential"`, `secretName: "review-ca"`, `key: "roots"`, "path: ca.crt"},
			"serviceAccountToken:",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			webhook := map[string]any{"url": "https://identity.example.com/review", "audiences": []string{"client-prod", "client-staging"}, "credentials": tc.credentials}
			if tc.ca != nil {
				webhook["ca"] = tc.ca
			}
			out := renderChart(t, values(webhook))
			ss := out[statefulsetTpl]
			env := parseEnv(ss)
			require.Equal(t, "https://identity.example.com/review", env["EXTERNAL_AUTH_WEBHOOK_URL"])
			require.Equal(t, "client-prod client-staging", env["EXTERNAL_AUTH_WEBHOOK_AUDIENCES"])
			require.Equal(t, "/var/run/secrets/telepresence.io/auth-webhook-caller/token", env["EXTERNAL_AUTH_WEBHOOK_CALLER_TOKEN_FILE"])
			if tc.ca != nil {
				require.Equal(t, "/var/run/secrets/telepresence.io/auth-webhook-ca/ca.crt", env["EXTERNAL_AUTH_WEBHOOK_CA_FILE"])
			}
			for _, expected := range tc.expected {
				require.Contains(t, ss, expected)
			}
			require.NotContains(t, ss, tc.absent)
		})
	}
	for _, tc := range []struct {
		name    string
		webhook map[string]any
		want    string
	}{
		{"missing audiences", map[string]any{"url": "https://identity.example.com/review", "credentials": map[string]any{"secretName": "secret"}}, "audiences"},
		{"missing caller", map[string]any{"url": "https://identity.example.com/review", "audiences": []string{"client"}}, "exactly one"},
		{"both callers", map[string]any{"url": "https://identity.example.com/review", "audiences": []string{"client"}, "credentials": map[string]any{"secretName": "secret", "serviceAccountTokenAudience": "projected"}}, "exactly one"},
		{"missing URL", map[string]any{"audiences": []string{"client"}}, "url is required"},
	} {
		t.Run(tc.name, func(t *testing.T) { require.ErrorContains(t, renderErr(t, values(tc.webhook)), tc.want) })
	}
}

// TestExternalEndpointRequiresExactlyOneTLSSource asserts that the render
// fails unless exactly one of tls.secretName / tls.certManager.enabled is
// set.
func TestExternalEndpointRequiresExactlyOneTLSSource(t *testing.T) {
	base := map[string]any{
		"security": map[string]any{
			"authentication": map[string]any{"mode": "enforcing"},
		},
	}

	t.Run("neither", func(t *testing.T) {
		vals := map[string]any{
			"security":         base["security"],
			"externalEndpoint": map[string]any{"enabled": true},
		}
		err := renderErr(t, vals)
		if err == nil {
			t.Fatalf("expected render to fail with neither tls.secretName nor tls.certManager.enabled set")
		}
		if !strings.Contains(err.Error(), "exactly one") {
			t.Errorf("unexpected render error: %v", err)
		}
	})

	t.Run("both", func(t *testing.T) {
		vals := map[string]any{
			"security": base["security"],
			"externalEndpoint": map[string]any{
				"enabled": true,
				"tls": map[string]any{
					"secretName":  "rtest-external-tls",
					"certManager": map[string]any{"enabled": true},
				},
			},
		}
		err := renderErr(t, vals)
		if err == nil {
			t.Fatalf("expected render to fail with both tls.secretName and tls.certManager.enabled set")
		}
		if !strings.Contains(err.Error(), "exactly one") {
			t.Errorf("unexpected render error: %v", err)
		}
	})
}
