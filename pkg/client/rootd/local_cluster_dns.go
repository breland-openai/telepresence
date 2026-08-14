package rootd

import (
	"context"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dns2 "github.com/miekg/dns"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/rootd/dns"
	"github.com/telepresenceio/telepresence/v2/pkg/vif"
)

const (
	localClusterAPIName                 = "kubernetes.default.svc"
	localClusterAPIFQDN                 = localClusterAPIName + ".cluster.local."
	localClusterDNSDiscoveryLimit       = 750 * time.Millisecond
	localClusterDNSMaximumDiscoveryTime = 3 * time.Second
	localClusterDNSQueryLimit           = 250 * time.Millisecond
	localClusterDNSMaximumNames         = 32
	localClusterDNSParallelNames        = 4
)

type localClusterDNSLookup func(context.Context, netip.AddrPort, string, uint16) (netip.Addr, bool)

type localClusterDNSName struct {
	query    string
	mappings []string
}

func discoverLocalClusterDNSMappings(ctx context.Context, config *client.DNS, virtualSubnet netip.Prefix) client.DNSMappings {
	if !config.PreserveLocalClusterDNS {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, localClusterDNSDiscoveryTimeout(config))
	defer cancel()

	return localClusterDNSMappings(ctx, config, virtualSubnet, dns.SystemResolvers(ctx), queryLocalClusterDNS)
}

func localClusterDNSDiscoveryTimeout(config *client.DNS) time.Duration {
	nameCount := min(len(config.PreserveLocalClusterDNSNames), localClusterDNSMaximumNames) + 1
	batches := (nameCount + localClusterDNSParallelNames - 1) / localClusterDNSParallelNames
	timeout := localClusterDNSDiscoveryLimit + time.Duration(batches-1)*localClusterDNSQueryLimit
	return min(timeout, localClusterDNSMaximumDiscoveryTime)
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

	physicalResolvers := physicalLocalClusterDNSResolvers(config, virtualSubnet, resolvers)
	if len(physicalResolvers) == 0 {
		return nil
	}

	names := preservedLocalClusterDNSNames(ctx, config.PreserveLocalClusterDNSNames)
	results := make([]client.DNSMappings, len(names))
	var next atomic.Int32
	var workers sync.WaitGroup
	for range min(len(names), localClusterDNSParallelNames) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				index := int(next.Add(1) - 1)
				if index >= len(names) || ctx.Err() != nil {
					return
				}
				name := names[index]
				for _, resolver := range physicalResolvers {
					if addr, ok := lookupLocalClusterDNS(ctx, resolver, name.query, lookup); ok {
						clog.Infof(ctx, "Preserving local cluster DNS for %s at %s using resolver %s", name.query, addr, resolver)
						for _, mapping := range name.mappings {
							results[index] = append(results[index], &client.DNSMapping{Name: mapping, AliasFor: addr.String()})
						}
						break
					}
				}
			}
		}()
	}
	workers.Wait()

	var mappings client.DNSMappings
	for _, result := range results {
		mappings = append(mappings, result...)
	}
	return mappings
}

func preservedLocalClusterDNSNames(ctx context.Context, configured []string) []localClusterDNSName {
	names := []localClusterDNSName{{
		query:    localClusterAPIFQDN,
		mappings: []string{localClusterAPIName, strings.TrimSuffix(localClusterAPIFQDN, ".")},
	}}
	seen := map[string]struct{}{
		localClusterAPIName:                          {},
		strings.TrimSuffix(localClusterAPIFQDN, "."): {},
	}
	for _, configuredName := range configured {
		name := canonicalLocalClusterDNSName(configuredName)
		_, addressError := netip.ParseAddr(name)
		if addressError == nil || !strings.Contains(name, ".") || len(validation.IsDNS1123Subdomain(name)) != 0 {
			clog.Warnf(ctx, "Ignoring invalid local cluster DNS preservation name %q", configuredName)
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		if len(names) > localClusterDNSMaximumNames {
			clog.Warnf(ctx, "Ignoring local cluster DNS preservation names beyond the limit of %d", localClusterDNSMaximumNames)
			break
		}
		seen[name] = struct{}{}
		names = append(names, localClusterDNSName{query: dns2.Fqdn(name), mappings: []string{name}})
	}
	return names
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

func lookupLocalClusterDNS(ctx context.Context, resolver netip.AddrPort, name string, lookup localClusterDNSLookup) (netip.Addr, bool) {
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
			addr, ok := lookup(ctx, resolver, name, qType)
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

func queryLocalClusterDNS(ctx context.Context, resolver netip.AddrPort, name string, qType uint16) (netip.Addr, bool) {
	request := new(dns2.Msg)
	request.SetQuestion(name, qType)
	request.RecursionDesired = true

	response, _, err := (&dns2.Client{Net: "udp", Timeout: localClusterDNSQueryLimit}).ExchangeContext(ctx, request, resolver.String())
	if err != nil {
		clog.Debugf(ctx, "Unable to resolve local cluster DNS name %s through %s: %v", name, resolver, err)
		return netip.Addr{}, false
	}
	if response == nil || response.Rcode != dns2.RcodeSuccess {
		return netip.Addr{}, false
	}

	for _, answer := range response.Answer {
		if !strings.EqualFold(answer.Header().Name, name) {
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
