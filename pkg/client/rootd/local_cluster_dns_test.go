package rootd

import (
	"context"
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
			if question.Name != localClusterAPIFQDN || !address.IsValid() {
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
				func(ctx context.Context, resolver netip.AddrPort, qType uint16) (netip.Addr, bool) {
					if resolver != physicalResolver {
						return netip.Addr{}, false
					}
					return queryLocalClusterDNS(ctx, fakeResolver, qType)
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
	}, func(_ context.Context, resolver netip.AddrPort, qType uint16) (netip.Addr, bool) {
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
		func(_ context.Context, resolver netip.AddrPort, qType uint16) (netip.Addr, bool) {
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
	addr, ok := lookupLocalClusterDNS(context.Background(), resolver,
		func(_ context.Context, _ netip.AddrPort, qType uint16) (netip.Addr, bool) {
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
