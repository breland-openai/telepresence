package k8s

import (
	"encoding/json/v2"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func encodedDevboxManagerPolicy(t *testing.T, policy *devboxManagerPolicy) []byte {
	t.Helper()
	data, err := json.Marshal(policy)
	require.NoError(t, err)
	return data
}

func TestDevboxManagerPolicyRejectsUntrustedMappings(t *testing.T) {
	valid := string(encodedDevboxManagerPolicy(t, devboxTestPolicy()))
	policy := parseDevboxManagerPolicy([]byte(valid))
	require.NotNil(t, policy)
	require.True(t, policy.allowsAudience(devboxProxyProduction))
	require.True(t, policy.allowsAudience(devboxProxyStaging))
	require.False(t, policy.allowsAudience(devboxTestClient))
	require.False(t, (*devboxManagerPolicy)(nil).allowsAudience(devboxProxyProduction))
	for name, data := range map[string]string{
		"unknown field":    strings.TrimSuffix(valid, "}") + `,"extra":true}`,
		"duplicate field":  strings.TrimSuffix(valid, "}") + `,"version":1}`,
		"duplicate host":   strings.Replace(valid, `"devbox-proxy.production-0.example.test":`, `"devbox-proxy.production-1.example.test":`, 1),
		"other version":    strings.Replace(valid, `"version":1`, `"version":2`, 1),
		"invalid tenant":   strings.Replace(valid, devboxIdentityTenant, "not-a-tenant", 1),
		"nil tenant":       strings.Replace(valid, devboxIdentityTenant, "00000000-0000-0000-0000-000000000000", 1),
		"uppercase host":   strings.Replace(valid, "devbox-proxy.production-0", "Devbox-proxy.production-0", 1),
		"wildcard host":    strings.Replace(valid, "devbox-proxy.production-0", "*.production-0", 1),
		"host port":        strings.Replace(valid, "production-0.example.test", "production-0.example.test:443", 1),
		"host suffix":      strings.Replace(valid, "production-0.example.test", "production-0.example.test.", 1),
		"URL as host":      strings.Replace(valid, "devbox-proxy.production-0", "https://devbox-proxy.production-0", 1),
		"host IP":          strings.Replace(valid, "devbox-proxy.production-0.example.test", "127.0.0.1", 1),
		"one-label host":   strings.Replace(valid, "devbox-proxy.production-0.example.test", "localhost", 1),
		"invalid audience": strings.Replace(valid, devboxProxyStaging, "api://"+devboxProxyStaging, 1),
		"empty map":        `{"version":1,"tenantID":"` + devboxIdentityTenant + `","proxyAudiences":{}}`,
		"oversize":         valid + strings.Repeat(" ", maxDevboxManagerPolicySize),
	} {
		t.Run(name, func(t *testing.T) { require.Nil(t, parseDevboxManagerPolicy([]byte(data))) })
	}
	for _, server := range []string{
		"http://devbox-proxy.production-0.example.test/clusters/staging-1",
		"https://devbox-proxy.production-0.example.test:/clusters/staging-1",
		"https://devbox-proxy.production-0.example.test.:443/clusters/staging-1",
		"https://devbox-proxy.production-0.example.test.attacker.test/clusters/staging-1",
		"https://DEVBOX-proxy.production-0.example.test/clusters/staging-1",
		"https://devbox-proxy.production-0.example.test/clusters/staging-1#",
		"https://devbox-proxy.production-0.example.test/clusters/%73taging-1",
	} {
		require.Empty(t, policy.proxyAudience(server))
	}
	require.Equal(t, devboxProxyProduction, policy.proxyAudience("https://devbox-proxy.production-0.example.test:443/clusters/staging-1"))
	require.Equal(t, devboxProxyStaging, policy.managerAudience("https://devbox-proxy.production-0.example.test:443/clusters/staging-1"))
	require.Equal(t, devboxProxyProduction, policy.managerAudience("https://devbox-proxy.production-0.example.test:443/clusters/production-1"))
	require.Empty(t, policy.managerAudience("https://devbox-proxy.production-0.example.test:443/clusters/unapproved"))
	for _, change := range []func(*devboxManagerPolicy){
		func(p *devboxManagerPolicy) { p.ManagerNamespace = "wrong.namespace" },
		func(p *devboxManagerPolicy) { p.ManagerServiceAccount = "" },
		func(p *devboxManagerPolicy) { p.ManagerServiceAccount = "Uppercase" },
		func(p *devboxManagerPolicy) { p.ClusterAudiences = nil },
		func(p *devboxManagerPolicy) { p.ClusterAudiences["staging-1"] = devboxTestClient },
		func(p *devboxManagerPolicy) { p.ClusterAudiences["staging-1."] = devboxProxyStaging },
	} {
		candidate := devboxTestPolicy()
		change(candidate)
		require.Nil(t, parseDevboxManagerPolicy(encodedDevboxManagerPolicy(t, candidate)))
	}
}

func TestDevboxManagerPolicyMissingNeverNegotiatesAndRootReadsIndependently(t *testing.T) {
	var reads atomic.Int32
	kc := devboxTestKubeconfig(devboxTestRest("production-0", devboxProxyProduction))
	kc.devboxManagerPolicyLoader = func() *devboxManagerPolicy { reads.Add(1); return nil }
	require.Nil(t, kc.NegotiatedDevboxManagerTokenProvider("ambassador"))
	_, err := connectionManagerAuthenticationForVersion(t.Context(), kc, devboxTestTarget(devboxTestPodA), devboxProxyStaging)
	require.ErrorContains(t, err, "not trusted by system policy")
	_, err = connectionManagerAuthenticationForVersion(t.Context(), kc, devboxTestTarget(devboxTestPodA), "")
	require.NoError(t, err)
	require.Equal(t, int32(1), reads.Load())

	root := devboxTestKubeconfig(&rest.Config{BearerToken: "existing-native"})
	root.managerTokenCallbackNegotiated = true
	root.managerTokenCallback = &managerCallbackTokenSource{}
	root.devboxManagerPolicyLoader = func() *devboxManagerPolicy { return nil }
	_, err = connectionManagerAuthenticationForVersion(t.Context(), root, devboxTestTarget(devboxTestPodA), devboxProxyStaging)
	require.ErrorContains(t, err, "not trusted by system policy")

	native := &Kubeconfig{RestConfig: &rest.Config{BearerToken: "native"}, devboxManagerPolicyLoader: func() *devboxManagerPolicy { return nil }}
	auth, err := connectionManagerAuthenticationForVersion(t.Context(), native, devboxTestTarget(devboxTestPodA), devboxProxyStaging)
	require.NoError(t, err)
	metadata, err := auth.credentials.GetRequestMetadata(t.Context())
	require.NoError(t, err)
	require.Equal(t, "Bearer native", metadata["authorization"])
}
