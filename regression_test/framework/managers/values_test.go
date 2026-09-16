package managers_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/telepresenceio/telepresence/v2/regression_test/framework/managers"
)

func TestClientDNSPreservedNamesReachHelmValues(t *testing.T) {
	const (
		inherited = "cache.platform.svc.cluster.local"
		first     = "artifact-gateway.platform.svc.cluster.local"
		second    = "image-cache.platform.svc.cluster.local"
		suffix    = "rtest-clientconfig.internal"
	)
	base := managers.Baseline("local", "2.32.0-test.0", "Never", "true")
	base.Client.DNS = managers.ClientDNS{
		IncludeSuffixes:              []string{suffix},
		PreserveLocalClusterDNSNames: []string{inherited},
	}

	for _, tc := range []struct {
		name string
		over []string
		want []string
	}{
		{name: "unset inherits", want: []string{inherited}},
		{name: "configured replaces", over: []string{first, second}, want: []string{first, second}},
		{name: "empty clears", over: []string{}, want: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := managers.ClientConfig("dns-routing", managers.Values{
				Client: managers.Client{DNS: managers.ClientDNS{PreserveLocalClusterDNSNames: tc.over}},
			})
			merged := managers.Merge(base, spec.Values)
			require.Equal(t, tc.want, merged.Client.DNS.PreserveLocalClusterDNSNames)
			require.Equal(t, []string{suffix}, merged.Client.DNS.IncludeSuffixes)

			data, err := yaml.Marshal(merged)
			require.NoError(t, err)
			var rendered struct {
				Client struct {
					DNS map[string][]string `json:"dns"`
				} `json:"client"`
			}
			require.NoError(t, yaml.Unmarshal(data, &rendered))
			require.Equal(t, []string{suffix}, rendered.Client.DNS["includeSuffixes"])
			if len(tc.want) == 0 {
				require.NotContains(t, rendered.Client.DNS, "preserveLocalClusterDNSNames")
			} else {
				require.Equal(t, tc.want, rendered.Client.DNS["preserveLocalClusterDNSNames"])
			}
		})
	}
}
