package rootd

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dns2 "github.com/miekg/dns"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
	"github.com/telepresenceio/telepresence/v2/pkg/client/rootd/dns"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func fakeLocalClusterDNS(t *testing.T, address netip.Addr) (netip.AddrPort, *atomic.Int64) {
	t.Helper()
	return fakeLocalClusterDNSRecords(t, map[string]netip.Addr{localClusterAPIFQDN: address})
}

func fakeLocalClusterDNSRecords(t *testing.T, addresses map[string]netip.Addr) (netip.AddrPort, *atomic.Int64) {
	t.Helper()

	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	queries := new(atomic.Int64)
	started := make(chan struct{})
	server := &dns2.Server{
		PacketConn:        listener,
		NotifyStartedFunc: func() { close(started) },
		Handler: dns2.HandlerFunc(func(w dns2.ResponseWriter, request *dns2.Msg) {
			queries.Add(1)
			response := new(dns2.Msg)
			response.SetReply(request)
			question := request.Question[0]
			address, found := addresses[question.Name]
			if !found || !address.IsValid() {
				response.Rcode = dns2.RcodeNameError
			} else {
				header := dns2.RR_Header{Name: question.Name, Rrtype: question.Qtype, Class: dns2.ClassINET}
				switch {
				case question.Qtype == dns2.TypeA && address.Is4():
					response.Answer = append(response.Answer, &dns2.A{Hdr: header, A: address.AsSlice()})
				case question.Qtype == dns2.TypeAAAA && address.Is6():
					response.Answer = append(response.Answer, &dns2.AAAA{Hdr: header, AAAA: address.AsSlice()})
				}
			}
			_ = w.WriteMsg(response)
		}),
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ActivateAndServe()
	}()
	<-started
	t.Cleanup(func() {
		require.NoError(t, server.Shutdown())
		require.NoError(t, <-serveErr)
	})
	return netip.MustParseAddrPort(listener.LocalAddr().String()), queries
}

func TestLocalClusterDNSMappings(t *testing.T) {
	tests := []struct {
		name      string
		address   netip.Addr
		disabled  bool
		wantAlias string
	}{
		{
			name:      "preserves physical IPv4 API for short and fully qualified names",
			address:   netip.MustParseAddr("10.0.0.1"),
			wantAlias: "10.0.0.1",
		},
		{
			name:      "preserves physical IPv6 API when A is unavailable",
			address:   netip.MustParseAddr("fd00::1"),
			wantAlias: "fd00::1",
		},
		{
			name: "does not preserve an unresolvable local API",
		},
		{
			name:     "does not query physical DNS when preservation is disabled",
			address:  netip.MustParseAddr("10.0.0.1"),
			disabled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeResolver, queries := fakeLocalClusterDNS(t, tt.address)
			physicalResolver := netip.MustParseAddrPort("10.0.0.10:53")
			config := *client.GetDefaultConfig().DNS()
			config.PreserveLocalClusterDNS = !tt.disabled

			mappings := localClusterDNSMappings(
				context.Background(),
				&config,
				client.DefaultVirtualSubnet(),
				[]netip.AddrPort{physicalResolver},
				func(ctx context.Context, resolver netip.AddrPort, name string, qType uint16) (netip.Addr, bool) {
					if resolver != physicalResolver {
						return netip.Addr{}, false
					}
					return queryLocalClusterDNS(ctx, fakeResolver, name, qType)
				},
			)

			if tt.wantAlias == "" {
				require.Empty(t, mappings)
			} else {
				require.Equal(t, client.DNSMappings{
					{Name: "kubernetes.default.svc", AliasFor: tt.wantAlias},
					{Name: "kubernetes.default.svc.cluster.local", AliasFor: tt.wantAlias},
				}, mappings)
			}
			if tt.disabled {
				require.Zero(t, queries.Load())
			} else {
				require.Positive(t, queries.Load())
			}
		})
	}
}

func TestLocalClusterDNSMappingsConfiguredNames(t *testing.T) {
	const (
		artifactName = "artifact-gateway.platform.svc.cluster.local"
		auditName    = "audit-gateway.platform.svc.cluster.local"
	)
	apiAddress := netip.MustParseAddr("192.0.2.1")
	artifactAddress := netip.MustParseAddr("198.51.100.21")
	auditAddress := netip.MustParseAddr("203.0.113.32")
	ipv6Address := netip.MustParseAddr("2001:db8::21")
	apiMappings := client.DNSMappings{
		{Name: localClusterAPIName, AliasFor: apiAddress.String()},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: apiAddress.String()},
	}

	tests := []struct {
		name       string
		records    map[string]netip.Addr
		configured []string
		disabled   bool
		want       client.DNSMappings
	}{
		{
			name: "preserves each configured hostname at its independent physical address",
			records: map[string]netip.Addr{
				localClusterAPIFQDN: apiAddress,
				artifactName + ".":  artifactAddress,
				auditName + ".":     auditAddress,
			},
			configured: []string{artifactName, auditName},
			want: append(append(client.DNSMappings{}, apiMappings...),
				&client.DNSMapping{Name: artifactName, AliasFor: artifactAddress.String()},
				&client.DNSMapping{Name: auditName, AliasFor: auditAddress.String()}),
		},
		{
			name: "preserves an IPv6-only configured hostname",
			records: map[string]netip.Addr{
				localClusterAPIFQDN: apiAddress,
				artifactName + ".":  ipv6Address,
			},
			configured: []string{artifactName},
			want: append(append(client.DNSMappings{}, apiMappings...),
				&client.DNSMapping{Name: artifactName, AliasFor: ipv6Address.String()}),
		},
		{
			name:       "preserves configured hostname when the local API is absent",
			records:    map[string]netip.Addr{artifactName + ".": artifactAddress},
			configured: []string{artifactName},
			want:       client.DNSMappings{{Name: artifactName, AliasFor: artifactAddress.String()}},
		},
		{
			name:       "does not map configured names absent from physical DNS",
			records:    map[string]netip.Addr{localClusterAPIFQDN: apiAddress},
			configured: []string{artifactName},
			want:       apiMappings,
		},
		{
			name: "canonicalizes and deduplicates configured names and API aliases",
			records: map[string]netip.Addr{
				localClusterAPIFQDN: apiAddress,
				artifactName + ".":  artifactAddress,
			},
			configured: []string{
				"KUBERNETES.DEFAULT.SVC.",
				"KUBERNETES.DEFAULT.SVC.CLUSTER.LOCAL.",
				"ARTIFACT-GATEWAY.PLATFORM.SVC.CLUSTER.LOCAL.",
				artifactName,
			},
			want: append(append(client.DNSMappings{}, apiMappings...),
				&client.DNSMapping{Name: artifactName, AliasFor: artifactAddress.String()}),
		},
		{
			name: "rejects wildcards suffix rules single labels IP literals and invalid DNS labels",
			records: map[string]netip.Addr{
				localClusterAPIFQDN: apiAddress,
				artifactName + ".":  artifactAddress,
				"192.0.2.21.":       auditAddress,
			},
			configured: []string{
				"*.platform.svc.cluster.local",
				".svc.cluster.local",
				"artifact-gateway",
				"192.0.2.21",
				"artifact_gateway.platform.svc.cluster.local",
				"artifact-gateway..platform.svc.cluster.local",
				artifactName,
			},
			want: append(append(client.DNSMappings{}, apiMappings...),
				&client.DNSMapping{Name: artifactName, AliasFor: artifactAddress.String()}),
		},
		{
			name: "does not query configured hostnames when preservation is disabled",
			records: map[string]netip.Addr{
				localClusterAPIFQDN: apiAddress,
				artifactName + ".":  artifactAddress,
			},
			configured: []string{artifactName},
			disabled:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeResolver, queries := fakeLocalClusterDNSRecords(t, tt.records)
			physicalResolver := netip.MustParseAddrPort("192.0.2.53:53")
			config := *client.GetDefaultConfig().DNS()
			config.PreserveLocalClusterDNS = !tt.disabled
			config.PreserveLocalClusterDNSNames = tt.configured

			mappings := localClusterDNSMappings(context.Background(), &config, client.DefaultVirtualSubnet(),
				[]netip.AddrPort{physicalResolver},
				func(ctx context.Context, resolver netip.AddrPort, name string, qType uint16) (netip.Addr, bool) {
					if resolver != physicalResolver {
						return netip.Addr{}, false
					}
					return queryLocalClusterDNS(ctx, fakeResolver, name, qType)
				})

			require.Equal(t, tt.want, mappings)
			if tt.disabled {
				require.Zero(t, queries.Load())
			} else {
				require.Positive(t, queries.Load())
			}
		})
	}
}

func TestLocalClusterDNSMappingsConfiguredNamesTryEachPhysicalResolver(t *testing.T) {
	const artifactName = "artifact-gateway.platform.svc.cluster.local"
	first := netip.MustParseAddrPort("192.0.2.53:53")
	second := netip.MustParseAddrPort("198.51.100.53:53")
	config := *client.GetDefaultConfig().DNS()
	config.PreserveLocalClusterDNSNames = []string{artifactName}

	mappings := localClusterDNSMappings(context.Background(), &config, client.DefaultVirtualSubnet(),
		[]netip.AddrPort{first, second},
		func(_ context.Context, resolver netip.AddrPort, name string, qType uint16) (netip.Addr, bool) {
			if qType != dns2.TypeA {
				return netip.Addr{}, false
			}
			switch {
			case name == localClusterAPIFQDN && resolver == first:
				return netip.MustParseAddr("192.0.2.1"), true
			case name == artifactName+"." && resolver == second:
				return netip.MustParseAddr("198.51.100.21"), true
			default:
				return netip.Addr{}, false
			}
		})

	require.Equal(t, client.DNSMappings{
		{Name: localClusterAPIName, AliasFor: "192.0.2.1"},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: "192.0.2.1"},
		{Name: artifactName, AliasFor: "198.51.100.21"},
	}, mappings)
}

func TestLocalClusterDNSMappingsConfiguredNamesAreIndependent(t *testing.T) {
	const artifactName = "artifact-gateway.platform.svc.cluster.local"
	resolver := netip.MustParseAddrPort("192.0.2.53:53")
	config := *client.GetDefaultConfig().DNS()
	config.PreserveLocalClusterDNSNames = []string{artifactName}
	artifactQueried := make(chan struct{})
	var artifactOnce sync.Once

	mappings := localClusterDNSMappings(context.Background(), &config, client.DefaultVirtualSubnet(),
		[]netip.AddrPort{resolver},
		func(ctx context.Context, _ netip.AddrPort, name string, qType uint16) (netip.Addr, bool) {
			if qType != dns2.TypeA {
				return netip.Addr{}, false
			}
			if name == artifactName+"." {
				artifactOnce.Do(func() { close(artifactQueried) })
				return netip.MustParseAddr("198.51.100.21"), true
			}
			select {
			case <-artifactQueried:
				return netip.MustParseAddr("192.0.2.1"), true
			case <-ctx.Done():
				return netip.Addr{}, false
			}
		})

	require.Equal(t, client.DNSMappings{
		{Name: localClusterAPIName, AliasFor: "192.0.2.1"},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: "192.0.2.1"},
		{Name: artifactName, AliasFor: "198.51.100.21"},
	}, mappings)
}

func TestPreservedLocalClusterDNSNamesAreBounded(t *testing.T) {
	configured := make([]string, localClusterDNSMaximumNames+3)
	for index := range configured {
		configured[index] = fmt.Sprintf("gateway-%d.platform.svc.cluster.local", index)
	}

	names := preservedLocalClusterDNSNames(context.Background(), configured)
	require.Len(t, names, localClusterDNSMaximumNames+1)
	require.Equal(t, localClusterAPIFQDN, names[0].query)
	require.Equal(t, "gateway-0.platform.svc.cluster.local.", names[1].query)
	require.Equal(t,
		fmt.Sprintf("gateway-%d.platform.svc.cluster.local.", localClusterDNSMaximumNames-1),
		names[len(names)-1].query)
}

func TestLocalClusterDNSDiscoveryTimeout(t *testing.T) {
	tests := []struct {
		name       string
		configured int
		want       time.Duration
	}{
		{name: "existing API-only timeout is unchanged", configured: 0, want: 750 * time.Millisecond},
		{name: "one configured name uses the existing timeout", configured: 1, want: 750 * time.Millisecond},
		{name: "one worker batch uses the existing timeout", configured: 3, want: 750 * time.Millisecond},
		{name: "another worker batch adds one query timeout", configured: 4, want: time.Second},
		{name: "maximum configured names receive a bounded extended timeout", configured: 32, want: 2750 * time.Millisecond},
		{name: "excess configured names cannot increase the timeout", configured: 320, want: 2750 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := *client.GetDefaultConfig().DNS()
			config.PreserveLocalClusterDNSNames = make([]string, tt.configured)
			require.Equal(t, tt.want, localClusterDNSDiscoveryTimeout(&config))
			require.LessOrEqual(t, localClusterDNSDiscoveryTimeout(&config), localClusterDNSMaximumDiscoveryTime)
		})
	}
}

func TestLocalClusterDNSMappingsPreservesMaximumConfiguredNames(t *testing.T) {
	resolver := netip.MustParseAddrPort("192.0.2.53:53")
	config := *client.GetDefaultConfig().DNS()
	records := map[string]netip.Addr{localClusterAPIFQDN: netip.MustParseAddr("192.0.2.1")}
	for index := range localClusterDNSMaximumNames {
		name := fmt.Sprintf("gateway-%d.platform.svc.cluster.local", index)
		config.PreserveLocalClusterDNSNames = append(config.PreserveLocalClusterDNSNames, name)
		records[name+"."] = netip.MustParseAddr(fmt.Sprintf("198.51.100.%d", index+1))
	}

	mappings := localClusterDNSMappings(context.Background(), &config, client.DefaultVirtualSubnet(),
		[]netip.AddrPort{resolver},
		func(_ context.Context, _ netip.AddrPort, name string, qType uint16) (netip.Addr, bool) {
			address, exists := records[name]
			return address, exists && qType == dns2.TypeA
		})

	require.Len(t, mappings, localClusterDNSMaximumNames+2)
	require.Equal(t, localClusterAPIName, mappings[0].Name)
	require.Equal(t, "kubernetes.default.svc.cluster.local", mappings[1].Name)
	for index := range localClusterDNSMaximumNames {
		require.Equal(t, fmt.Sprintf("gateway-%d.platform.svc.cluster.local", index), mappings[index+2].Name)
		require.Equal(t, fmt.Sprintf("198.51.100.%d", index+1), mappings[index+2].AliasFor)
	}
}

func TestPhysicalLocalClusterDNSResolvers(t *testing.T) {
	config := *client.GetDefaultConfig().DNS()
	config.VIFAddress = netip.MustParseAddrPort("192.0.2.53:53")
	defaultVirtual := netip.AddrPortFrom(client.DefaultVirtualSubnet().Addr().Next(), 53)
	customVirtual := netip.MustParsePrefix("198.18.0.0/15")
	physicalIPv4 := netip.MustParseAddrPort("10.0.0.10:53")
	physicalIPv6 := netip.MustParseAddrPort("[2001:db8::53]:53")

	resolvers := physicalLocalClusterDNSResolvers(&config, customVirtual, []netip.AddrPort{
		{},
		netip.MustParseAddrPort("127.0.0.53:53"),
		netip.MustParseAddrPort("0.0.0.0:53"),
		netip.MustParseAddrPort("[::1]:53"),
		netip.MustParseAddrPort("[::]:53"),
		netip.MustParseAddrPort("10.0.0.11:0"),
		config.VIFAddress,
		defaultVirtual,
		netip.MustParseAddrPort("198.18.0.53:53"),
		netip.MustParseAddrPort("[fd00:0:0:246::53]:53"),
		physicalIPv4,
		netip.MustParseAddrPort("[::ffff:10.0.0.10]:53"),
		physicalIPv6,
		physicalIPv4,
	})

	require.Equal(t, []netip.AddrPort{physicalIPv4, physicalIPv6}, resolvers)
}

func TestLocalClusterDNSMappingsSkipsVirtualResolvers(t *testing.T) {
	config := *client.GetDefaultConfig().DNS()
	config.VIFAddress = netip.MustParseAddrPort("192.0.2.53:53")
	customVirtual := netip.MustParsePrefix("198.18.0.0/15")
	physical := netip.MustParseAddrPort("10.0.0.10:53")
	var queried sync.Map

	mappings := localClusterDNSMappings(context.Background(), &config, customVirtual, []netip.AddrPort{
		netip.AddrPortFrom(client.DefaultVirtualSubnet().Addr().Next(), 53),
		netip.MustParseAddrPort("198.18.0.53:53"),
		netip.MustParseAddrPort("[fd00:0:0:246::53]:53"),
		config.VIFAddress,
		physical,
	}, func(_ context.Context, resolver netip.AddrPort, _ string, qType uint16) (netip.Addr, bool) {
		queried.Store(resolver, struct{}{})
		return netip.MustParseAddr("10.0.0.1"), qType == dns2.TypeA
	})

	require.Len(t, mappings, 2)
	queried.Range(func(key, _ any) bool {
		require.Equal(t, physical, key)
		return true
	})
}

func TestLocalClusterDNSMappingsTryNextPhysicalResolver(t *testing.T) {
	config := *client.GetDefaultConfig().DNS()
	first := netip.MustParseAddrPort("10.0.0.10:53")
	second := netip.MustParseAddrPort("10.0.0.11:53")

	mappings := localClusterDNSMappings(context.Background(), &config, client.DefaultVirtualSubnet(),
		[]netip.AddrPort{first, second},
		func(_ context.Context, resolver netip.AddrPort, _ string, qType uint16) (netip.Addr, bool) {
			if resolver == second && qType == dns2.TypeA {
				return netip.MustParseAddr("10.0.0.1"), true
			}
			return netip.Addr{}, false
		})

	require.Len(t, mappings, 2)
	require.Equal(t, "10.0.0.1", mappings[0].AliasFor)
}

func TestLookupLocalClusterDNSPrefersIPv4(t *testing.T) {
	resolver := netip.MustParseAddrPort("10.0.0.10:53")
	addr, ok := lookupLocalClusterDNS(context.Background(), resolver, localClusterAPIFQDN,
		func(_ context.Context, _ netip.AddrPort, _ string, qType uint16) (netip.Addr, bool) {
			if qType == dns2.TypeAAAA {
				return netip.MustParseAddr("fd00::1"), true
			}
			time.Sleep(10 * time.Millisecond)
			return netip.MustParseAddr("10.0.0.1"), true
		})

	require.True(t, ok)
	require.Equal(t, netip.MustParseAddr("10.0.0.1"), addr)
}

func TestMergeLocalClusterDNSMappings(t *testing.T) {
	preserved := client.DNSMappings{
		{Name: "kubernetes.default.svc", AliasFor: "10.0.0.1"},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: "10.0.0.1"},
	}
	explicit := client.DNSMappings{
		{Name: "KUBERNETES.DEFAULT.SVC.", AliasFor: "192.0.2.1"},
		{Name: "other.service", AliasFor: "192.0.2.2"},
	}

	mappings := mergeLocalClusterDNSMappings(explicit, preserved)
	require.Equal(t, client.DNSMappings{
		{Name: "KUBERNETES.DEFAULT.SVC.", AliasFor: "192.0.2.1"},
		{Name: "other.service", AliasFor: "192.0.2.2"},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: "10.0.0.1"},
	}, mappings)
	require.Len(t, explicit, 2)
}

func TestMergeLocalClusterDNSMappingsExplicitConfiguredNameWins(t *testing.T) {
	preserved := client.DNSMappings{
		{Name: localClusterAPIName, AliasFor: "192.0.2.1"},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: "192.0.2.1"},
		{Name: "artifact-gateway.platform.svc.cluster.local", AliasFor: "198.51.100.21"},
	}
	explicit := client.DNSMappings{
		{Name: "ARTIFACT-GATEWAY.PLATFORM.SVC.CLUSTER.LOCAL.", AliasFor: "203.0.113.21"},
	}

	require.Equal(t, client.DNSMappings{
		{Name: "ARTIFACT-GATEWAY.PLATFORM.SVC.CLUSTER.LOCAL.", AliasFor: "203.0.113.21"},
		{Name: localClusterAPIName, AliasFor: "192.0.2.1"},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: "192.0.2.1"},
	}, mergeLocalClusterDNSMappings(explicit, preserved))
}

func TestPreservedLocalClusterDNSNeverProxy(t *testing.T) {
	tests := []struct {
		name      string
		alias     string
		subnet    netip.Prefix
		wantRoute netip.Prefix
	}{
		{
			name:      "preserves local IPv4 API inside remote service subnet",
			alias:     "10.0.0.1",
			subnet:    netip.MustParsePrefix("10.0.0.0/16"),
			wantRoute: netip.MustParsePrefix("10.0.0.1/32"),
		},
		{
			name:      "preserves local IPv6 API inside remote service subnet",
			alias:     "fd00::1",
			subnet:    netip.MustParsePrefix("fd00::/64"),
			wantRoute: netip.MustParsePrefix("fd00::1/128"),
		},
		{
			name:   "does not add host route for nonoverlapping local API",
			alias:  "10.1.0.1",
			subnet: netip.MustParsePrefix("10.0.0.0/16"),
		},
		{
			name:   "ignores a nonliteral API mapping",
			alias:  "api.example.test",
			subnet: netip.MustParsePrefix("10.0.0.0/16"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preserved := client.DNSMappings{
				{Name: localClusterAPIName, AliasFor: tt.alias},
				{Name: localClusterAPIFQDN, AliasFor: tt.alias},
			}
			addresses := appendPreservedLocalClusterDNSAddresses(nil, preserved)
			neverProxy, routes := appendLocalDNSNeverProxy(context.Background(), nil,
				[]netip.Prefix{tt.subnet}, addresses)
			if !tt.wantRoute.IsValid() {
				require.Empty(t, neverProxy)
				require.Empty(t, routes)
				return
			}
			require.Equal(t, []netip.Prefix{tt.wantRoute}, neverProxy)
			require.Equal(t, []netip.Prefix{tt.wantRoute}, routes)
		})
	}
}

func TestPreservedLocalClusterDNSConfiguredNamesNeverProxy(t *testing.T) {
	preserved := client.DNSMappings{
		{Name: localClusterAPIName, AliasFor: "192.0.2.1"},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: "192.0.2.1"},
		{Name: "artifact-gateway.platform.svc.cluster.local", AliasFor: "198.51.100.21"},
		{Name: "audit-gateway.platform.svc.cluster.local", AliasFor: "2001:db8::32"},
	}
	addresses := appendPreservedLocalClusterDNSAddresses(nil, preserved)
	neverProxy, routes := appendLocalDNSNeverProxy(context.Background(), nil, []netip.Prefix{
		netip.MustParsePrefix("192.0.2.0/24"),
		netip.MustParsePrefix("198.51.100.0/24"),
		netip.MustParsePrefix("2001:db8::/64"),
	}, addresses)
	want := []netip.Prefix{
		netip.MustParsePrefix("192.0.2.1/32"),
		netip.MustParsePrefix("198.51.100.21/32"),
		netip.MustParsePrefix("2001:db8::32/128"),
	}

	require.ElementsMatch(t, want, neverProxy)
	require.ElementsMatch(t, want, routes)
}

func TestSessionSetMappingsPreservesLocalClusterDNS(t *testing.T) {
	preserved := client.DNSMappings{
		{Name: "kubernetes.default.svc", AliasFor: "10.0.0.1"},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: "10.0.0.1"},
	}
	config := *client.GetDefaultConfig().DNS()
	config.Mappings = preserved
	s := &session{
		dnsServer:               dns.NewServer(&config, "default", nil),
		localClusterDNSMappings: preserved,
	}

	s.SetMappings([]*rpc.DNSMapping{
		{Name: "kubernetes.default.svc", AliasFor: "192.0.2.1"},
		{Name: "other.service", AliasFor: "192.0.2.2"},
	})
	require.Equal(t, client.DNSMappings{
		{Name: "kubernetes.default.svc", AliasFor: "192.0.2.1"},
		{Name: "other.service", AliasFor: "192.0.2.2"},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: "10.0.0.1"},
	}, s.dnsServer.GetConfig().Mappings)

	s.SetMappings(nil)
	require.Equal(t, preserved, s.dnsServer.GetConfig().Mappings)
}

func TestSessionSetMappingsPreservesConfiguredLocalClusterDNSName(t *testing.T) {
	const artifactName = "artifact-gateway.platform.svc.cluster.local"
	preserved := client.DNSMappings{
		{Name: localClusterAPIName, AliasFor: "192.0.2.1"},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: "192.0.2.1"},
		{Name: artifactName, AliasFor: "198.51.100.21"},
	}
	config := *client.GetDefaultConfig().DNS()
	config.PreserveLocalClusterDNSNames = []string{artifactName}
	config.Mappings = preserved
	s := &session{
		dnsServer:               dns.NewServer(&config, "default", nil),
		localClusterDNSMappings: preserved,
	}

	s.SetMappings([]*rpc.DNSMapping{
		{Name: "ARTIFACT-GATEWAY.PLATFORM.SVC.CLUSTER.LOCAL.", AliasFor: "203.0.113.21"},
	})
	require.Equal(t, client.DNSMappings{
		{Name: "ARTIFACT-GATEWAY.PLATFORM.SVC.CLUSTER.LOCAL.", AliasFor: "203.0.113.21"},
		{Name: localClusterAPIName, AliasFor: "192.0.2.1"},
		{Name: "kubernetes.default.svc.cluster.local", AliasFor: "192.0.2.1"},
	}, s.dnsServer.GetConfig().Mappings)

	s.SetMappings(nil)
	require.Equal(t, preserved, s.dnsServer.GetConfig().Mappings)
}

func TestSessionNetworkConfigIncludesPreservedMappings(t *testing.T) {
	config := client.GetDefaultConfig()
	explicit := &client.DNSMapping{Name: "other.service", AliasFor: "192.0.2.2"}
	preserved := &client.DNSMapping{Name: "kubernetes.default.svc", AliasFor: "10.0.0.1"}
	config.DNS().Mappings = client.DNSMappings{explicit}
	config.Routing().Subnets = []netip.Prefix{netip.MustParsePrefix("10.99.0.0/16")}
	ctx := client.WithConfig(context.Background(), config)
	effectiveDNS := *config.DNS()
	effectiveDNS.Mappings = client.DNSMappings{explicit, preserved}
	s := &session{
		Cluster: &k8s.Cluster{
			Kubeconfig: &k8s.Kubeconfig{Context: ctx},
		},
		session:   &manager.SessionInfo{SessionId: "test-session"},
		dnsServer: dns.NewServer(&effectiveDNS, "default", nil),
		l4PortMap: xsync.NewMap[types.AddrPortProto, uint16](),
	}

	network := s.getNetworkConfig()
	statusConfig, err := client.UnmarshalJSONConfig(network.ClientConfig, false)
	require.NoError(t, err)
	require.Equal(t, effectiveDNS.Mappings, statusConfig.DNS().Mappings)
	require.Equal(t, client.DNSMappings{explicit}, client.GetConfig(s).DNS().Mappings)
	require.Equal(t, []netip.Prefix{netip.MustParsePrefix("10.99.0.0/16")}, client.GetConfig(s).Routing().Subnets)
}
