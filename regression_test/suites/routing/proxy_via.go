package routing

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/subnet"
	"github.com/telepresenceio/telepresence/v2/pkg/vif"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/check"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/cli"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/managers"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/rt"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/workloads"
)

// ProxyVia proves --proxy-via all=<workload> routes every cluster subnet
// through that workload's traffic-agent instead of the manager-bound
// tunnel: the workload's own service still round-trips, and every routed
// subnet in `status` lands inside the virtual subnet. The
// --proxy-via-and-mounts variant is left to the mounts area.
type ProxyVia struct {
	rt.Suite
}

func init() {
	rt.Register(&ProxyVia{}, rt.InArea("routing"), rt.NeedsManager(managers.Default))
}

// Test_AllSubnetsRouteThroughWorkload connects with --proxy-via all=<wl>. A
// non-default connection: ConnExtraArgs folds the flag into the fixture
// hash, so it is Mutate'd and explicitly quit at the end rather than left
// for a later suite to adopt (a plain default connect must not inherit a
// proxy-via session). It then checks that every routed subnet in `status`
// lands inside client.DefaultVirtualSubnet(): the same range
// activateProxyViaWorkloads draws proxy-via virtual IPs from
// (pkg/client/rootd/session.go) -- and that the workload's service still
// answers.
func (s *ProxyVia) Test_AllSubnetsRouteThroughWorkload() {
	t := s.T()
	ctx := s.Ctx()
	ns := s.AppNamespace()
	s.Manager()

	wl := s.Workload(workloads.Echo("proxy-via-wl"))

	conn := rt.Mutate(t, rt.ConnectionFixture(ns, rt.ConnExtraArgs("--proxy-via", "all="+wl.Name)))
	awaitRouted(t, ctx, s.CLI(), ns)
	defer conn.Disconnect(t)

	vs := client.DefaultVirtualSubnet()
	var lastSubnets []string
	var lastVirtualSubnet string
	s.Eventually(func() bool {
		var st daemonStatus
		if err := s.CLI().JSON(ctx, &st, "status", "--format", "json"); err != nil {
			return false
		}
		lastSubnets = st.RootDaemon.Subnets
		lastVirtualSubnet = st.RootDaemon.VirtualSubnet
		if len(st.RootDaemon.Subnets) == 0 {
			return false
		}
		// Dual-stack: IPv4 subnets translate into the IPv4 virtual range,
		// IPv6 ones into the fixed Telepresence ULA (vif.TelepresenceULA6),
		// mirroring rootd's per-family vip generators.
		for _, raw := range st.RootDaemon.Subnets {
			sn, err := netip.ParsePrefix(raw)
			if err != nil {
				return false
			}
			virt := vs
			if sn.Addr().Is6() {
				virt = vif.TelepresenceULA6
			}
			if !subnet.Covers(virt, sn) {
				return false
			}
		}
		return true
	}, subnetPollTimeout, subnetPollInterval,
		"proxy-via all should translate every routed subnet inside %s (last subnets %v, last virtual_subnet %q)",
		vs, lastSubnets, lastVirtualSubnet)

	rt.RoutedToCluster(t, wl.ServiceURL())
}

// Test_ComplexLookupUsesProxyViaWorkload gives two agents contradictory
// pod-local answers for a name the manager cannot resolve. The selected
// proxy-via workload must provide the answer and carry the translated traffic.
func (s *ProxyVia) Test_ComplexLookupUsesProxyViaWorkload() {
	const (
		hostname = "proxy-via-agent.rtest.invalid"
		suffix   = ".rtest.invalid"
	)

	t := s.T()
	ctx := s.Ctx()
	ns := s.AppNamespace()
	s.Manager()
	target := rt.Mutate(t, rt.WorkloadFixture(ns, workloads.Echo("proxy-via-dns-target")))
	decoy := rt.Mutate(t, rt.WorkloadFixture(ns, workloads.Echo("proxy-via-dns-decoy")))

	serviceAddress := func(workload *rt.Workload) netip.Addr {
		address, err := s.R().Kubectl(ctx, ns, "get", "service", workload.SvcName, "-o", "jsonpath={.spec.clusterIP}")
		s.Require().NoError(err)
		ip, err := netip.ParseAddr(strings.TrimSpace(address))
		s.Require().NoError(err)

		patch := fmt.Sprintf(`{"spec":{"template":{"spec":{"hostAliases":[{"ip":%q,"hostnames":[%q]}]}}}}`, ip, hostname)
		_, err = s.R().Kubectl(ctx, ns, "patch", "deployment", workload.Name, "--type=merge", "-p", patch)
		s.Require().NoError(err)
		_, err = s.R().Kubectl(ctx, ns, "rollout", "status", "deployment/"+workload.Name, "--timeout=120s")
		s.Require().NoError(err)
		return ip
	}
	targetIP := serviceAddress(target)
	decoyIP := serviceAddress(decoy)
	s.Require().NotEqual(targetIP, decoyIP)
	targetSubnet := netip.PrefixFrom(targetIP, targetIP.BitLen())

	conn := rt.Mutate(t, rt.ConnectionFixture(ns,
		rt.ConnExtraArgs("--proxy-via", targetSubnet.String()+"="+target.Name),
		rt.ConnWithConfig(func(config client.Config) {
			config.DNS().UseComplexLookup = true
			config.DNS().IncludeSuffixes = append(config.DNS().IncludeSuffixes, suffix)
		}),
	))
	t.Cleanup(func() { conn.Disconnect(t) })
	awaitRouted(t, ctx, s.CLI(), ns)
	var status struct {
		RootDaemon struct {
			DNS struct {
				UseComplexLookup bool     `json:"use_complex_lookup"`
				IncludeSuffixes  []string `json:"include_suffixes"`
			} `json:"dns"`
		} `json:"root_daemon"`
	}
	s.Require().NoError(s.CLI().JSON(ctx, &status, "status", "--format", "json"))
	s.Require().True(status.RootDaemon.DNS.UseComplexLookup)
	s.Require().Contains(status.RootDaemon.DNS.IncludeSuffixes, suffix)

	attachment := conn.Intercept(t, decoy, rt.ToLocal(s.LocalEcho(), "http"), cli.MountFalse())
	t.Cleanup(func() { attachment.Detach(t) })

	virtualSubnet := client.DefaultVirtualSubnet()
	if targetIP.Is6() {
		virtualSubnet = vif.TelepresenceULA6
	}
	var resolved []string
	s.Eventually(func() bool {
		lookupContext, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		var err error
		resolved, err = net.DefaultResolver.LookupHost(lookupContext, hostname)
		if err != nil || len(resolved) != 1 {
			return false
		}
		address, err := netip.ParseAddr(resolved[0])
		return err == nil && virtualSubnet.Contains(address)
	}, subnetPollTimeout, time.Second,
		"%s should resolve to a virtual IP in %s, not target %s or decoy %s; got %v",
		hostname, virtualSubnet, targetIP, decoyIP, resolved)

	url := "http://" + net.JoinHostPort(hostname, strconv.Itoa(target.Port))
	check.EventuallyHTTP(t, url, check.BodyContains(target.Name), subnetPollTimeout)
	rt.RoutedToCluster(t, target.ServiceURL())
}
