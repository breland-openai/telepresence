package golden

import (
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// TestExtraValuesReachManagerAndForwarder pins the contract that the
// top-level extraEnv/extraVolumes/extraVolumeMounts values render into both
// the traffic-manager and, when quicTunnel is enabled, the quic-forwarder
// workload (values.yaml documents them as applying to both).
func TestExtraValuesReachManagerAndForwarder(t *testing.T) {
	out := renderChart(t, map[string]any{
		"quicTunnel": map[string]any{"enabled": true},
		"extraEnv":   []map[string]any{{"name": "EXTRA_ENV_RTEST", "value": "x"}},
		"extraVolumes": []map[string]any{{
			"name":     "extra-vol-rtest",
			"hostPath": map[string]any{"path": "/rtest-extra", "type": "DirectoryOrCreate"},
		}},
		"extraVolumeMounts": []map[string]any{{
			"name":      "extra-vol-rtest",
			"mountPath": "/rtest-extra",
		}},
	})
	for _, tpl := range []string{deploymentTpl, quicFwdTpl} {
		doc := out[tpl]
		for _, want := range []string{"EXTRA_ENV_RTEST", "extra-vol-rtest", "/rtest-extra"} {
			if !strings.Contains(doc, want) {
				t.Errorf("%s: %q not rendered", tpl, want)
			}
		}
	}
}

func TestLocalClusterDNSPreservationReachesManager(t *testing.T) {
	wantNames := []string{
		"artifact-gateway.platform.svc.cluster.local",
		"image-cache.platform.svc.cluster.local",
	}
	out := renderChart(t, map[string]any{
		"client": map[string]any{
			"dns": map[string]any{
				"preserveLocalClusterDNS":      true,
				"preserveLocalClusterDNSNames": wantNames,
			},
		},
	})

	const configMapTemplate = "telepresence-oss/templates/trafficManager-configmap.yaml"
	var configMap struct {
		Data map[string]string `json:"data"`
	}
	if err := yaml.Unmarshal([]byte(out[configMapTemplate]), &configMap); err != nil {
		t.Fatalf("unmarshal manager ConfigMap: %v", err)
	}
	clientYAML, ok := configMap.Data["client.yaml"]
	if !ok {
		t.Fatal("manager ConfigMap does not contain client.yaml")
	}

	var clientConfig struct {
		DNS struct {
			PreserveLocalClusterDNS      bool     `json:"preserveLocalClusterDNS"`
			PreserveLocalClusterDNSNames []string `json:"preserveLocalClusterDNSNames"`
		} `json:"dns"`
	}
	if err := yaml.Unmarshal([]byte(clientYAML), &clientConfig); err != nil {
		t.Fatalf("unmarshal manager client configuration: %v", err)
	}
	if !clientConfig.DNS.PreserveLocalClusterDNS {
		t.Error("manager client configuration does not enable local cluster DNS preservation")
	}
	if !slices.Equal(clientConfig.DNS.PreserveLocalClusterDNSNames, wantNames) {
		t.Errorf("manager client preserved names = %v, want %v", clientConfig.DNS.PreserveLocalClusterDNSNames, wantNames)
	}
}
