//go:build linux

package dns

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/telepresenceio/clog/testutil"
)

func TestStaleDNSRule(t *testing.T) {
	const canonical = "-A TELEPRESENCE_DNS -d 10.0.0.10/32 -p udp -m udp --dport 53 -j DNAT --to-destination 127.0.0.1:33695"
	tests := []struct {
		name, line, chain, target string
		valid                     bool
	}{
		{"canonical", canonical, tpDNSChain, "DNAT", true},
		{"emitted", "-A TELEPRESENCE_DNS -p udp --dest 10.0.0.10/32 --dport 53 -j DNAT --to-destination 127.0.0.1:33695", tpDNSChain, "DNAT", true},
		{"raw canonical", "-A TELEPRESENCE_DNS_RAW -s 127.0.0.1/32 -p udp -m udp --sport 33695 -j CT --zone 2", tpDNSRawChain, "CT", true},
		{"raw emitted", "-A TELEPRESENCE_DNS_RAW -p udp --source 127.0.0.1/32 --sport 33695 -j CT --zone 2", tpDNSRawChain, "CT", true},
		{"other chain", strings.Replace(canonical, tpDNSChain, "OTHER_DNS", 1), tpDNSChain, "DNAT", false},
		{"remote target", strings.Replace(canonical, "127.0.0.1", "10.0.0.22", 1), tpDNSChain, "DNAT", false},
		{"port zero", strings.Replace(canonical, ":33695", ":0", 1), tpDNSChain, "DNAT", false},
		{"target range", canonical + "-33696", tpDNSChain, "DNAT", false},
		{"extra option", canonical + " --random", tpDNSChain, "DNAT", false},
		{"unknown match", canonical + " -m owner --uid-owner 1000", tpDNSChain, "DNAT", false},
		{"duplicate option", canonical + " --dport 54", tpDNSChain, "DNAT", false},
		{"tcp", strings.ReplaceAll(canonical, "udp", "tcp"), tpDNSChain, "DNAT", false},
		{"network destination", strings.Replace(canonical, "/32", "/24", 1), tpDNSChain, "DNAT", false},
		{"quoted garbage", canonical + " '", tpDNSChain, "DNAT", false},
		{"RETURN", "-A TELEPRESENCE_DNS -p udp --source 10.0.0.25 --sport 33445 -j RETURN", tpDNSChain, "DNAT", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args, endpoint := staleDNSRule(tt.line, tt.chain, tt.target)
			assert.Equal(t, tt.valid, endpoint.IsValid())
			if tt.valid {
				assert.NotEmpty(t, args)
				assert.Equal(t, netip.MustParseAddrPort("127.0.0.1:33695"), endpoint)
			} else {
				assert.Empty(t, args)
			}
		})
	}
}

func dnsRuleFixture(endpoint netip.AddrPort) (string, string) {
	return fmt.Sprintf("-A TELEPRESENCE_DNS -d 10.0.0.10/32 -p udp -m udp --dport 53 -j DNAT --to-destination %s", endpoint),
		fmt.Sprintf("-A TELEPRESENCE_DNS_RAW -s 127.0.0.1/32 -p udp -m udp --sport %d -j CT --zone 2", endpoint.Port())
}

func reserveTestDNSEndpoint(t *testing.T) (*net.UDPConn, netip.AddrPort) {
	t.Helper()
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return listener, listener.LocalAddr().(*net.UDPAddr).AddrPort()
}

func TestCleanupStaleRoutingReservesEndpointThroughExactDeletions(t *testing.T) {
	listener, endpoint := reserveTestDNSEndpoint(t)
	require.NoError(t, listener.Close())
	nat, raw := dnsRuleFixture(endpoint)
	nat += "\n-A OUTPUT -j TELEPRESENCE_DNS\n-A TELEPRESENCE_DNS -s 10.0.0.25/32 -p udp -m udp --sport 33445 -j RETURN"
	raw += "\n-A OUTPUT -j TELEPRESENCE_DNS_RAW"
	var removed []string
	err := cleanupStaleRouting(testutil.NewContext(t, true), nat, raw, func(_ context.Context, table string, rule []string) error {
		// A competing listener must stay excluded during both table operations.
		competitor, bindErr := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(endpoint))
		if competitor != nil {
			_ = competitor.Close()
		}
		require.Error(t, bindErr)
		removed = append(removed, table+" "+strings.Join(rule, " "))
		return nil
	})
	require.NoError(t, err)
	wantNAT, wantRaw := dnsRuleFixture(endpoint)
	assert.Equal(t, []string{"nat " + wantNAT, "raw " + wantRaw}, removed)
	available, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(endpoint))
	require.NoError(t, err)
	require.NoError(t, available.Close())
}

func TestCleanupStaleRoutingPreservesLiveListeners(t *testing.T) {
	for _, wildcard := range []bool{false, true} {
		t.Run(fmt.Sprintf("wildcard=%t", wildcard), func(t *testing.T) {
			address := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
			if wildcard {
				address.IP = net.IPv4zero
			}
			listener, err := net.ListenUDP("udp4", address)
			require.NoError(t, err)
			defer listener.Close()
			endpoint := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), listener.LocalAddr().(*net.UDPAddr).AddrPort().Port())
			nat, raw := dnsRuleFixture(endpoint)
			calls := 0
			err = cleanupStaleRouting(testutil.NewContext(t, true), nat, raw, func(context.Context, string, []string) error {
				calls++
				return nil
			})
			require.NoError(t, err)
			assert.Zero(t, calls)
		})
	}
}

func TestCleanupStaleRoutingDoesNotDeleteReplacementRules(t *testing.T) {
	listener, endpoint := reserveTestDNSEndpoint(t)
	require.NoError(t, listener.Close())
	nat, raw := dnsRuleFixture(endpoint)
	_, replacementEndpoint := reserveTestDNSEndpoint(t)
	replacementNAT, replacementRaw := dnsRuleFixture(replacementEndpoint)
	current := map[string][]string{"nat": {replacementNAT}, "raw": {replacementRaw}}
	errChanged := errors.New("observed rule was replaced")
	err := cleanupStaleRouting(testutil.NewContext(t, true), nat, raw, func(_ context.Context, table string, rule []string) error {
		if !slices.Contains(current[table], strings.Join(rule, " ")) {
			return errChanged
		}
		t.Fatal("cleanup attempted to delete a replacement rule")
		return nil
	})
	assert.ErrorIs(t, err, errChanged)
	assert.Equal(t, []string{replacementNAT}, current["nat"])
	assert.Equal(t, []string{replacementRaw}, current["raw"])
}

func TestCleanupStaleRoutingStopsOnDeleteFailure(t *testing.T) {
	listener, endpoint := reserveTestDNSEndpoint(t)
	require.NoError(t, listener.Close())
	nat, raw := dnsRuleFixture(endpoint)
	wantErr := errors.New("iptables unavailable")
	var tables []string
	err := cleanupStaleRouting(testutil.NewContext(t, true), nat, raw, func(_ context.Context, table string, _ []string) error {
		tables = append(tables, table)
		return wantErr
	})
	assert.ErrorIs(t, err, wantErr)
	assert.Equal(t, []string{"nat"}, tables)
}
