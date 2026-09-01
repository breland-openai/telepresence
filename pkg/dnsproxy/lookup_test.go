package dnsproxy

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"runtime"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/telepresenceio/clog/testutil"
)

func TestLookup(t *testing.T) {
	type tType struct {
		qType uint16
		qName string
	}
	tests := []tType{
		{
			dns.TypeA,
			"google.com.",
		},
		{
			dns.TypeCNAME,
			"_smpp_._tcp.golang.org.",
		},
		{
			dns.TypePTR,
			"78.217.250.142.in-addr.arpa.",
		},
		{
			dns.TypeMX,
			"gmail.com.",
		},
		{
			dns.TypeTXT,
			"dns.google.",
		},
		{
			dns.TypeSRV,
			"_myservice._tcp.tada.se.",
		},
	}
	// AAAA returns an error on Windows:
	// "getaddrinfow: The requested name is valid, but no data of the requested type was found"
	if runtime.GOOS != "windows" {
		tests = append(tests, tType{
			dns.TypeAAAA,
			"google.com.",
		})
	}
	for _, tt := range tests {
		t.Run(dns.TypeToString[tt.qType], func(t *testing.T) {
			if tt.qType == dns.TypeSRV && runtime.GOOS == "darwin" {
				t.Skip("SRV sporadically fails to parse reply on darwin")
			}
			ctx := testutil.NewContext(t, false)
			got, _, err := Lookup(ctx, tt.qType, tt.qName, "")
			require.NoError(t, err)
			require.Greater(t, len(got), 0)
		})
	}
}

func TestLookupIPAddressFamilies(t *testing.T) {
	resolver := startLookupIPTestResolver(t)
	ctx := testutil.NewContext(t, false)

	tests := []struct {
		name       string
		network    string
		hostname   string
		wantIPs    []string
		wantAbsent bool
	}{
		{
			name:     "AAAA lookup for IPv4-only hostname returns no data",
			network:  "ip6",
			hostname: "ipv4.example.",
		},
		{
			name:     "A lookup for IPv6-only hostname returns no data",
			network:  "ip4",
			hostname: "ipv6.example.",
		},
		{
			name:     "IPv4-only hostname returns its A record",
			network:  "ip4",
			hostname: "ipv4.example.",
			wantIPs:  []string{"192.0.2.10"},
		},
		{
			name:     "IPv6-only hostname returns its AAAA record",
			network:  "ip6",
			hostname: "ipv6.example.",
			wantIPs:  []string{"2001:db8::10"},
		},
		{
			name:     "dual-stack hostname returns its A record",
			network:  "ip4",
			hostname: "dual.example.",
			wantIPs:  []string{"192.0.2.20"},
		},
		{
			name:     "dual-stack hostname returns its AAAA record",
			network:  "ip6",
			hostname: "dual.example.",
			wantIPs:  []string{"2001:db8::20"},
		},
		{
			name:       "missing hostname preserves A NXDOMAIN",
			network:    "ip4",
			hostname:   "missing.example.",
			wantAbsent: true,
		},
		{
			name:       "missing hostname preserves AAAA NXDOMAIN",
			network:    "ip6",
			hostname:   "missing.example.",
			wantAbsent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ips, err := lookupIP(ctx, tt.network, tt.hostname, ".example.", resolver)
			if tt.wantAbsent {
				require.Error(t, err)
				var dnsErr *net.DNSError
				require.ErrorAs(t, err, &dnsErr)
				require.True(t, dnsErr.IsNotFound)
				rCode, rpcErr := MakeDNSError(err)
				require.Equal(t, dns.RcodeNameError, rCode)
				require.NoError(t, rpcErr)
				return
			}

			require.NoError(t, err)
			var got []string
			for _, ip := range ips {
				got = append(got, ip.String())
			}
			require.Equal(t, tt.wantIPs, got)
		})
	}

	t.Run("SERVFAIL preserves a temporary resolver error", func(t *testing.T) {
		ips, err := lookupIP(ctx, "ip4", "failure.example.", ".example.", resolver)
		require.Empty(t, ips)
		require.Error(t, err)
		var dnsErr *net.DNSError
		require.ErrorAs(t, err, &dnsErr)
		require.True(t, dnsErr.IsTemporary)
		rCode, rpcErr := MakeDNSError(err)
		require.Equal(t, dns.RcodeServerFailure, rCode)
		require.Equal(t, codes.Unavailable, status.Code(rpcErr))
	})

	t.Run("canceled context preserves its error", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		ips, err := lookupIP(canceled, "ip4", "ipv4.example.", ".example.", resolver)
		require.Empty(t, ips)
		require.ErrorIs(t, err, context.Canceled)
		rCode, rpcErr := MakeDNSError(err)
		require.Equal(t, dns.RcodeServerFailure, rCode)
		require.Equal(t, codes.Canceled, status.Code(rpcErr))
	})

	t.Run("expired deadline preserves its error", func(t *testing.T) {
		expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
		t.Cleanup(cancel)
		ips, err := lookupIP(expired, "ip4", "ipv4.example.", ".example.", resolver)
		require.Empty(t, ips)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		rCode, rpcErr := MakeDNSError(err)
		require.Equal(t, dns.RcodeServerFailure, rCode)
		require.Equal(t, codes.DeadlineExceeded, status.Code(rpcErr))
	})
}

type ipResolverFunc func(context.Context, string, string) ([]net.IP, error)

func (f ipResolverFunc) LookupIP(ctx context.Context, network, host string) ([]net.IP, error) {
	return f(ctx, network, host)
}

func TestLookupIPStopsCanceledRetries(t *testing.T) {
	for _, tt := range []struct {
		name        string
		cancelAfter int
	}{
		{name: "before first query"},
		{name: "before absolute name retry", cancelAfter: 1},
		{name: "before opposite family", cancelAfter: 2},
		{name: "before opposite family absolute retry", cancelAfter: 3},
		{name: "during final query", cancelAfter: 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(testutil.NewContext(t, false))
			defer cancel()
			if tt.cancelAfter == 0 {
				cancel()
			}
			calls := 0
			resolver := ipResolverFunc(func(_ context.Context, _, host string) ([]net.IP, error) {
				calls++
				if calls == tt.cancelAfter {
					cancel()
				}
				return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
			})

			ips, err := lookupIP(ctx, "ip4", "missing.example.", "", resolver)

			require.Empty(t, ips)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, tt.cancelAfter, calls)
		})
	}
}

func TestLookupIPPreservesOppositeFamilyFailure(t *testing.T) {
	for _, failure := range []error{
		&net.DNSError{Err: "server misbehaving", IsTemporary: true},
		context.Canceled,
		context.DeadlineExceeded,
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			calls := 0
			resolver := ipResolverFunc(func(_ context.Context, network, host string) ([]net.IP, error) {
				calls++
				if network == "ip4" {
					return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
				}
				return nil, failure
			})

			ips, err := lookupIP(testutil.NewContext(t, false), "ip4", "missing.example.", ".example.", resolver)

			require.Empty(t, ips)
			require.ErrorIs(t, err, failure)
			require.Equal(t, 2, calls)
		})
	}
}

func TestMakeDNSErrorPreservesFailureClassification(t *testing.T) {
	for _, tt := range []struct {
		name  string
		err   error
		rCode int
		code  codes.Code
	}{
		{name: "absent", err: &net.DNSError{Err: "no such host", IsNotFound: true}, rCode: dns.RcodeNameError, code: codes.OK},
		{name: "canceled", err: context.Canceled, rCode: dns.RcodeServerFailure, code: codes.Canceled},
		{name: "deadline", err: context.DeadlineExceeded, rCode: dns.RcodeServerFailure, code: codes.DeadlineExceeded},
		{name: "temporary", err: &net.DNSError{Err: "server misbehaving", IsTemporary: true}, rCode: dns.RcodeServerFailure, code: codes.Unavailable},
		{name: "timeout", err: &net.DNSError{Err: "timeout", IsTimeout: true, IsTemporary: true}, rCode: dns.RcodeServerFailure, code: codes.DeadlineExceeded},
		{name: "other", err: errors.New("resolver failed"), rCode: dns.RcodeServerFailure, code: codes.Internal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rCode, err := MakeDNSError(tt.err)
			require.Equal(t, tt.rCode, rCode)
			require.Equal(t, tt.code, status.Code(err))

			response, err := ToRPC(nil, rCode)
			require.NoError(t, err)
			records, decodedCode, err := FromRPC(response)
			require.NoError(t, err)
			require.Empty(t, records)
			require.Equal(t, tt.rCode, decodedCode)
		})
	}
}

func startLookupIPTestResolver(t *testing.T) *net.Resolver {
	t.Helper()

	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	started := make(chan struct{})
	server := &dns.Server{
		PacketConn: listener,
		NotifyStartedFunc: func() {
			close(started)
		},
		Handler: dns.HandlerFunc(func(writer dns.ResponseWriter, request *dns.Msg) {
			response := new(dns.Msg)
			response.SetReply(request)
			response.Authoritative = true

			question := request.Question[0]
			switch question.Name {
			case "ipv4.example.":
				if question.Qtype == dns.TypeA {
					response.Answer = []dns.RR{&dns.A{
						Hdr: dns.RR_Header{Name: question.Name, Rrtype: dns.TypeA, Class: dns.ClassINET},
						A:   net.ParseIP("192.0.2.10").To4(),
					}}
				}
			case "ipv6.example.":
				if question.Qtype == dns.TypeAAAA {
					response.Answer = []dns.RR{&dns.AAAA{
						Hdr:  dns.RR_Header{Name: question.Name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET},
						AAAA: net.ParseIP("2001:db8::10"),
					}}
				}
			case "dual.example.":
				switch question.Qtype {
				case dns.TypeA:
					response.Answer = []dns.RR{&dns.A{
						Hdr: dns.RR_Header{Name: question.Name, Rrtype: dns.TypeA, Class: dns.ClassINET},
						A:   net.ParseIP("192.0.2.20").To4(),
					}}
				case dns.TypeAAAA:
					response.Answer = []dns.RR{&dns.AAAA{
						Hdr:  dns.RR_Header{Name: question.Name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET},
						AAAA: net.ParseIP("2001:db8::20"),
					}}
				}
			case "failure.example.":
				response.SetRcode(request, dns.RcodeServerFailure)
			default:
				response.SetRcode(request, dns.RcodeNameError)
			}

			if err := writer.WriteMsg(response); err != nil {
				t.Errorf("write DNS response: %v", err)
			}
		}),
	}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ActivateAndServe()
	}()

	select {
	case <-started:
	case err := <-serverErr:
		require.NoError(t, err)
		t.Fatal("DNS test server stopped before becoming ready")
	case <-time.After(5 * time.Second):
		t.Fatal("DNS test server did not become ready")
	}

	t.Cleanup(func() {
		require.NoError(t, server.Shutdown())
		require.NoError(t, <-serverErr)
	})

	return &net.Resolver{
		PreferGo:     true,
		StrictErrors: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "udp", listener.LocalAddr().String())
		},
	}
}

func TestPtrAddress_v4(t *testing.T) {
	ip, err := PtrAddress("32.127.168.192.in-addr.arpa.")
	require.NoError(t, err)
	require.Equal(t, netip.AddrFrom4([4]byte{192, 168, 127, 32}), ip)
}

func TestPtrAddress_v6(t *testing.T) {
	ip, err := PtrAddress("b.a.9.8.7.6.5.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.8.b.d.0.1.0.0.2.ip6.arpa.")
	require.NoError(t, err)
	require.Equal(t, netip.MustParseAddr("2001:db8::567:89ab"), ip)
}
