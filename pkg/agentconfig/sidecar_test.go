package agentconfig

import (
	"fmt"
	"net/netip"
	"sync"
	"testing"

	core "k8s.io/api/core/v1"

	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func TestInterceptorInactivePort(t *testing.T) {
	for _, tc := range []struct {
		name          string
		numericTarget bool
		inactivePort  uint16
		want          uint16
	}{
		{name: "named target", want: 8000},
		{name: "numeric target", numericTarget: true, want: 9912},
		{name: "explicit inactive port", numericTarget: true, inactivePort: 8399, want: 8399},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc := &Sidecar{Containers: []*Container{{
				Name: "app",
				Intercepts: []*Intercept{{
					ContainerPort:     8000,
					InactivePort:      tc.inactivePort,
					AgentPort:         9900,
					Protocol:          types.ProtoTCP,
					TargetPortNumeric: tc.numericTarget,
				}},
			}}}

			if got := sc.InterceptorInactivePort(8000, types.ProtoTCP); got != tc.want {
				t.Fatalf("InterceptorInactivePort() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestAgentGIDFromEnv(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		gid, ok, err := AgentGIDFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatalf("ok = true, want false")
		}
		if gid != 0 {
			t.Fatalf("gid = %d, want 0", gid)
		}
	})

	t.Run("empty", func(t *testing.T) {
		t.Setenv(EnvAgentGID, "")
		gid, ok, err := AgentGIDFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatalf("ok = true, want false")
		}
		if gid != 0 {
			t.Fatalf("gid = %d, want 0", gid)
		}
	})

	t.Run("valid", func(t *testing.T) {
		t.Setenv(EnvAgentGID, "7439")
		gid, ok, err := AgentGIDFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Fatalf("ok = false, want true")
		}
		if gid != 7439 {
			t.Fatalf("gid = %d, want 7439", gid)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		t.Setenv(EnvAgentGID, "not-a-number")
		_, _, err := AgentGIDFromEnv()
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}

// sidecarWithTarget builds a single-container, single-intercept Sidecar config with the
// intercept's TargetPortNumeric set as requested, mirroring the fake-Config pattern used
// by cmd/traffic/cmd/agent's newPortHandler tests.
func sidecarWithTarget(numericTarget bool) *Sidecar {
	ic := &Intercept{
		ContainerPortName: "http",
		ServicePortName:   "http",
		ServicePort:       80,
		Protocol:          types.ProtoTCP,
		AgentPort:         9900,
		ContainerPort:     8080,
		TargetPortNumeric: numericTarget,
	}
	return &Sidecar{
		AgentName: "test",
		Containers: []*Container{{
			Name:       "app",
			Intercepts: []*Intercept{ic},
		}},
	}
}

// Test_PassThroughTarget guards the address a forwarder or protocol prober picks for
// the agent's pass-through dial to the application when no intercept is active: the pod
// IP's proxy port for a numeric target port, loopback for a named one when the pod
// carries the nftables ruleset (dialing the pod IP there would hit the unconditional
// pod-IP redirect gate and loop back into the agent), and the pod IP's container port
// for a named target port without the ruleset — the application may be bound to the
// pod IP exclusively, and no gate exists to loop through (see the doc comment on
// PassThroughTarget).
func Test_PassThroughTarget(t *testing.T) {
	ipv4 := netip.MustParseAddr("192.168.50.34")
	ipv6 := netip.MustParseAddr("fd00::34")

	t.Run("named target port dials IPv4 loopback under nft redirects", func(t *testing.T) {
		sc := sidecarWithTarget(false)
		got := sc.PassThroughTarget(ipv4, 8080, types.ProtoTCP, true)
		want := netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 8080)
		if got != want {
			t.Fatalf("PassThroughTarget() = %v, want %v", got, want)
		}
	})

	t.Run("named target port dials IPv6 loopback under nft redirects", func(t *testing.T) {
		sc := sidecarWithTarget(false)
		got := sc.PassThroughTarget(ipv6, 8080, types.ProtoTCP, true)
		want := netip.AddrPortFrom(netip.IPv6Loopback(), 8080)
		if got != want {
			t.Fatalf("PassThroughTarget() = %v, want %v", got, want)
		}
	})

	t.Run("named target port dials the pod IP without nft redirects", func(t *testing.T) {
		sc := sidecarWithTarget(false)
		got := sc.PassThroughTarget(ipv4, 8080, types.ProtoTCP, false)
		want := netip.AddrPortFrom(ipv4, 8080)
		if got != want {
			t.Fatalf("PassThroughTarget() = %v, want %v", got, want)
		}
	})

	t.Run("numeric target port dials the pod IP's proxy port", func(t *testing.T) {
		sc := sidecarWithTarget(true)
		got := sc.PassThroughTarget(ipv4, 8080, types.ProtoTCP, true)
		want := netip.AddrPortFrom(ipv4, sc.ProxyPort(sc.Containers[0].Intercepts[0].AgentPort))
		if got != want {
			t.Fatalf("PassThroughTarget() = %v, want %v", got, want)
		}
	})
}

// Test_NftRedirectsActive guards the predicate that decides whether a sidecar's pod
// carries the agent's nftables ruleset: only a headless or numeric-target intercept in
// a replace-on-intercept container requires it. The injector's needInitContainer and
// the agent's pass-through target selection must agree on this, so both use it.
func Test_NftRedirectsActive(t *testing.T) {
	named := sidecarWithTarget(false)
	named.Containers[0].Replace = ReplacePolicyIntercept
	if named.NftRedirectsActive() {
		t.Fatal("a named-target-only config must not require nft redirects")
	}

	numeric := sidecarWithTarget(true)
	numeric.Containers[0].Replace = ReplacePolicyIntercept
	if !numeric.NftRedirectsActive() {
		t.Fatal("a numeric-target config must require nft redirects")
	}

	headless := sidecarWithTarget(false)
	headless.Containers[0].Replace = ReplacePolicyIntercept
	headless.Containers[0].Intercepts[0].Headless = true
	if !headless.NftRedirectsActive() {
		t.Fatal("a headless config must require nft redirects")
	}

	noReplace := sidecarWithTarget(true)
	noReplace.Containers[0].Replace = ReplacePolicyInactive
	if noReplace.NftRedirectsActive() {
		t.Fatal("a config without replace-on-intercept containers must not require nft redirects")
	}
}

func TestStrictNamedHTTPRedirectPreservesApplicationAndRawRoutes(t *testing.T) {
	pod := netip.MustParseAddr("192.0.2.12")
	sc := &Sidecar{
		RequireAuthoritativeRoutes: true,
		Containers: []*Container{{Name: "app", Replace: ReplacePolicyIntercept, Intercepts: []*Intercept{
			{ContainerPort: 8080, AgentPort: 9900, Protocol: types.ProtoTCP, AppProtocol: "kubernetes.io/http"},
			{ContainerPort: 5432, AgentPort: 9901, Protocol: types.ProtoTCP, AppProtocol: "tcp"},
			{ContainerPort: 8443, AgentPort: 9902, Protocol: types.ProtoTCP, AppProtocol: "https"},
			{ContainerPort: 8053, AgentPort: 9903, Protocol: types.ProtoUDP, AppProtocol: "http"},
		}}},
	}
	if !sc.NftRedirectsActive() || !sc.NftRedirectsPort(8080, types.ProtoTCP) {
		t.Fatal("strict named clear HTTP must program a pod and mesh redirect")
	}
	if got, want := sc.PassThroughTarget(pod, 8080, types.ProtoTCP, true), netip.AddrPortFrom(pod, sc.ProxyPort(9900)); got != want {
		t.Fatalf("application pass-through got %v, want pod-IP proxy %v", got, want)
	}
	for _, port := range []uint16{5432, 8443} {
		if sc.NftRedirectsPort(port, types.ProtoTCP) {
			t.Fatalf("unsupported named port %d unexpectedly redirected", port)
		}
		if got, want := sc.PassThroughTarget(pod, port, types.ProtoTCP, false), netip.AddrPortFrom(pod, port); got != want {
			t.Fatalf("raw pass-through got %v, want unchanged pod target %v", got, want)
		}
	}
	if sc.NftRedirectsPort(8053, types.ProtoUDP) {
		t.Fatal("a UDP port must not enter the HTTP redirect")
	}
	sc.RequireAuthoritativeRoutes = false
	if sc.NftRedirectsActive() || sc.InterceptorInactivePort(8080, types.ProtoTCP) != 8080 {
		t.Fatal("feature-off named ports must keep their original routing")
	}
	sc.RequireAuthoritativeRoutes = true
	sc.Containers[0].Intercepts[1].TargetPortNumeric = true
	if !sc.NftRedirectsPort(8443, types.ProtoTCP) {
		t.Fatal("a numeric target must preserve the original all-ports redirect behavior")
	}
}

func Test_LoopbackFor(t *testing.T) {
	if got := LoopbackFor(netip.MustParseAddr("10.1.2.3")); got != netip.AddrFrom4([4]byte{127, 0, 0, 1}) {
		t.Fatalf("LoopbackFor() = %v, want 127.0.0.1", got)
	}
	if got := LoopbackFor(netip.MustParseAddr("fd00::1")); got != netip.IPv6Loopback() {
		t.Fatalf("LoopbackFor() = %v, want ::1", got)
	}
}

func TestMarshalTightDoesNotMutateSidecar(t *testing.T) {
	sc := &Sidecar{
		AgentImage:          "registry.example/traffic-agent:latest",
		PullPolicy:          string(core.PullAlways),
		PullSecrets:         []core.LocalObjectReference{{Name: "agent-pull-secret"}},
		InitResources:       &core.ResourceRequirements{},
		SecurityContext:     &core.SecurityContext{},
		InitSecurityContext: &core.SecurityContext{},
	}

	tight, err := MarshalTight(sc)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalJSON(tight)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.AgentImage != "" ||
		decoded.PullPolicy != "" ||
		decoded.PullSecrets != nil ||
		decoded.InitResources != nil ||
		decoded.SecurityContext != nil ||
		decoded.InitSecurityContext != nil {
		t.Fatalf("tight config retained container creation fields: %#v", decoded)
	}

	if sc.AgentImage == "" ||
		sc.PullPolicy == "" ||
		sc.PullSecrets == nil ||
		sc.InitResources == nil ||
		sc.SecurityContext == nil ||
		sc.InitSecurityContext == nil {
		t.Fatalf("MarshalTight mutated source config: %#v", sc)
	}
}

func TestMarshalTightConcurrentContainerBuilds(t *testing.T) {
	const (
		agentImage = "registry.example/traffic-agent:latest"
		workers    = 8
		iterations = 1000
	)

	sc := &Sidecar{AgentImage: agentImage}
	start := make(chan struct{})
	errCh := make(chan error, workers*2)
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				if _, err := MarshalTight(sc); err != nil {
					errCh <- err
					return
				}
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				if image := InitContainer(sc, nil, "").Image; image != agentImage {
					errCh <- fmt.Errorf("injected container image = %q, want %q", image, agentImage)
					return
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
