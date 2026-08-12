package rootd

import (
	"context"
	"net/netip"
	"slices"
	"strings"
	"time"

	dns2 "github.com/miekg/dns"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/rootd/dns"
	"github.com/telepresenceio/telepresence/v2/pkg/vif"
)

const (
	localClusterAPIName           = "kubernetes.default.svc"
	localClusterAPIFQDN           = localClusterAPIName + ".cluster.local."
	localClusterDNSDiscoveryLimit = 750 * time.Millisecond
	localClusterDNSQueryLimit     = 250 * time.Millisecond
)

type localClusterDNSLookup func(context.Context, netip.AddrPort, uint16) (netip.Addr, bool)

func discoverLocalClusterDNSMappings(ctx context.Context, config *client.DNS, virtualSubnet netip.Prefix) client.DNSMappings {
	if !config.PreserveLocalClusterDNS {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, localClusterDNSDiscoveryLimit)
	defer cancel()

	return localClusterDNSMappings(ctx, config, virtualSubnet, dns.SystemResolvers(ctx), queryLocalClusterDNS)
}

func localClusterDNSMappings(
	ctx context.Context,
	config *client.DNS,
	virtualSubnet netip.Prefix,
	resolvers []netip.AddrPort,
	lookup localClusterDNSLookup,
) client.DNSMappings {
	if !config.PreserveLocalClusterDNS {
		return nil
	}

	for _, resolver := range physicalLocalClusterDNSResolvers(config, virtualSubnet, resolvers) {
		if addr, ok := lookupLocalClusterDNS(ctx, resolver, lookup); ok {
			clog.Infof(ctx, "Preserving local Kubernetes API DNS at %s using resolver %s", addr, resolver)
			return client.DNSMappings{
				{Name: localClusterAPIName, AliasFor: addr.String()},
				{Name: strings.TrimSuffix(localClusterAPIFQDN, "."), AliasFor: addr.String()},
			}
		}
	}
	return nil
}

func physicalLocalClusterDNSResolvers(
	config *client.DNS,
	virtualSubnet netip.Prefix,
	resolvers []netip.AddrPort,
) []netip.AddrPort {
	physical := make([]netip.AddrPort, 0, len(resolvers))
	defaultVirtualSubnet := client.DefaultVirtualSubnet()
	for _, resolver := range resolvers {
		if !resolver.IsValid() || resolver.Port() == 0 {
			continue
		}
		addr := resolver.Addr().Unmap()
		if addr.IsLoopback() || addr.IsUnspecified() {
			continue
		}
		if config.VIFAddress.IsValid() && addr == config.VIFAddress.Addr().Unmap() {
			continue
		}
		if (virtualSubnet.IsValid() && virtualSubnet.Contains(addr)) ||
			(defaultVirtualSubnet.IsValid() && defaultVirtualSubnet.Contains(addr)) ||
			vif.TelepresenceULA6.Contains(addr) {
			continue
		}
		resolver = netip.AddrPortFrom(addr, resolver.Port())
		if !slices.Contains(physical, resolver) {
			physical = append(physical, resolver)
		}
	}
	return physical
}

func lookupLocalClusterDNS(ctx context.Context, resolver netip.AddrPort, lookup localClusterDNSLookup) (netip.Addr, bool) {
	ctx, cancel := context.WithTimeout(ctx, localClusterDNSQueryLimit)
	defer cancel()

	type result struct {
		addr  netip.Addr
		qType uint16
		ok    bool
	}
	results := make(chan result, 2)
	for _, qType := range []uint16{dns2.TypeA, dns2.TypeAAAA} {
		go func() {
			addr, ok := lookup(ctx, resolver, qType)
			results <- result{addr: addr, qType: qType, ok: ok}
		}()
	}

	var ipv6 netip.Addr
	for range cap(results) {
		select {
		case <-ctx.Done():
			return ipv6, ipv6.IsValid()
		case response := <-results:
			if !response.ok {
				if response.qType == dns2.TypeA && ipv6.IsValid() {
					return ipv6, true
				}
				continue
			}
			if response.qType == dns2.TypeA {
				return response.addr, true
			}
			ipv6 = response.addr
		}
	}
	return ipv6, ipv6.IsValid()
}

func queryLocalClusterDNS(ctx context.Context, resolver netip.AddrPort, qType uint16) (netip.Addr, bool) {
	request := new(dns2.Msg)
	request.SetQuestion(localClusterAPIFQDN, qType)
	request.RecursionDesired = true

	response, _, err := (&dns2.Client{Net: "udp", Timeout: localClusterDNSQueryLimit}).ExchangeContext(ctx, request, resolver.String())
	if err != nil {
		clog.Debugf(ctx, "Unable to resolve local Kubernetes API through %s: %v", resolver, err)
		return netip.Addr{}, false
	}
	if response == nil || response.Rcode != dns2.RcodeSuccess {
		return netip.Addr{}, false
	}

	for _, answer := range response.Answer {
		if !strings.EqualFold(answer.Header().Name, localClusterAPIFQDN) {
			continue
		}
		var addr netip.Addr
		switch rr := answer.(type) {
		case *dns2.A:
			if qType == dns2.TypeA {
				addr, _ = netip.AddrFromSlice(rr.A)
			}
		case *dns2.AAAA:
			if qType == dns2.TypeAAAA {
				addr, _ = netip.AddrFromSlice(rr.AAAA)
			}
		}
		if addr.IsValid() && !addr.IsUnspecified() && !addr.IsLoopback() {
			return addr.Unmap(), true
		}
	}
	return netip.Addr{}, false
}

func mergeLocalClusterDNSMappings(explicit, preserved client.DNSMappings) client.DNSMappings {
	mappings := slices.Clone(explicit)
	for _, local := range preserved {
		name := canonicalLocalClusterDNSName(local.Name)
		if !slices.ContainsFunc(explicit, func(mapping *client.DNSMapping) bool {
			return canonicalLocalClusterDNSName(mapping.Name) == name
		}) {
			mappings = append(mappings, local)
		}
	}
	return mappings
}

func appendPreservedLocalClusterDNSAddresses(addresses []netip.Addr, mappings client.DNSMappings) []netip.Addr {
	for _, mapping := range mappings {
		if addr, err := netip.ParseAddr(mapping.AliasFor); err == nil {
			addresses = append(addresses, addr.Unmap())
		}
	}
	return addresses
}

func canonicalLocalClusterDNSName(name string) string {
	return strings.ToLower(strings.TrimSuffix(name, "."))
}
