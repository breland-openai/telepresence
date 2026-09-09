package agentconfig

import (
	stdjson "encoding/json"
	"testing"
	"time"
)

func TestSidecarDecodesInjectedDurationWireFormat(t *testing.T) {
	const annotation = `{
		"agentName":"canary-web",
		"namespace":"ambassador",
		"workloadName":"canary-web",
		"workloadKind":"Deployment",
		"managerHost":"traffic-manager.ambassador.svc.cluster.local",
		"managerPort":8081,
		"quicPort":7787,
		"mountPolicies":{"/tmp":"Local","/var/run/secrets/provider":"Ignore"},
		"meshDialSubnets":["240.240.0.0/16"],
		"containers":[{
			"name":"app",
			"intercepts":[{
				"containerPortName":"http",
				"serviceName":"canary-web",
				"serviceUID":"12345678-1234-1234-1234-123456789abc",
				"servicePortName":"http",
				"protocol":"TCP",
				"appProtocol":"http",
				"containerPort":8080,
				"servicePort":80,
				"agentPort":9900
			}],
			"envPrefix":"A_",
			"mountPoint":"/tel_app_mounts/app"
		}],
		"clientConnectionTTL":"24h0m0s",
		"enableMetrics":true,
		"enableH2cProbing":true,
		"watchRetryInterval":"10s"
	}`
	sidecar, err := UnmarshalJSON(annotation)
	if err != nil {
		t.Fatalf("decode injected pod annotation: %v", err)
	}
	if sidecar.ClientConnectionTTL != 24*time.Hour || sidecar.WatchRetryInterval != 10*time.Second {
		t.Fatalf("decoded durations: client TTL = %s, watch retry = %s", sidecar.ClientConnectionTTL, sidecar.WatchRetryInterval)
	}
	if len(sidecar.Containers) != 1 || len(sidecar.Containers[0].Intercepts) != 1 || sidecar.Containers[0].Intercepts[0].AgentPort != 9900 {
		t.Fatalf("decoded route: %+v", sidecar.Containers)
	}
	wire, err := MarshalTight(sidecar)
	if err != nil {
		t.Fatalf("encode injected pod annotation: %v", err)
	}
	var fields map[string]any
	if err := stdjson.Unmarshal([]byte(wire), &fields); err != nil {
		t.Fatalf("decode emitted JSON independently: %v", err)
	}
	if fields["clientConnectionTTL"] != "24h0m0s" || fields["watchRetryInterval"] != "10s" {
		t.Fatalf("emitted wire durations: client TTL = %#v, watch retry = %#v", fields["clientConnectionTTL"], fields["watchRetryInterval"])
	}

	for _, invalid := range []string{
		`{"clientConnectionTTL":86400000000000,"watchRetryInterval":"10s"}`,
		`{"clientConnectionTTL":"24h0m0s","watchRetryInterval":"soon"}`,
	} {
		if _, err := UnmarshalJSON(invalid); err == nil {
			t.Errorf("accepted invalid duration: %s", invalid)
		}
	}
}
