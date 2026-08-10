//go:build linux

package dns

import (
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/telepresenceio/clog/testutil"
)

func fakeIPTables(t *testing.T, sidecarExit, dnsCaptureExit int, failTable, failOperation string) string {
	t.Helper()
	dir := t.TempDir()
	logFile := filepath.Join(dir, "iptables.log")
	require.NoError(t, os.WriteFile(logFile, nil, 0o600))

	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> %q
if [ "$3" = "-C" ]; then
  if [ "$2" = "nat" ]; then
    exit %d
  fi
  exit %d
fi
if [ "$2" = %q ] && [ "$3" = %q ]; then
  exit 4
fi
exit 0
`, logFile, sidecarExit, dnsCaptureExit, failTable, failOperation)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "iptables"), []byte(script), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return logFile
}

func iptablesCommands(t *testing.T, logFile string) []string {
	t.Helper()
	data, err := os.ReadFile(logFile)
	require.NoError(t, err)
	if len(data) == 0 {
		return nil
	}
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

func TestRouteDNSIstioCompatibility(t *testing.T) {
	dnsAddress := netip.MustParseAddrPort("10.0.0.10:53")
	listener := netip.MustParseAddrPort("127.0.0.1:33695")
	fallback := netip.MustParseAddrPort("10.0.0.25:33445")
	ownerRule := "-t nat -A TELEPRESENCE_DNS -p udp --dest 10.0.0.10/32 --dport 53 -m owner --uid-owner 1337 -j RETURN"
	fallbackRule := "-t nat -A TELEPRESENCE_DNS -p udp --source 10.0.0.25 --sport 33445 -j RETURN"
	conntrackRule := "-t raw -A TELEPRESENCE_DNS_RAW -p udp --source 127.0.0.1/32 --sport 33695 -j CT --zone 2"
	dnatRule := "-t nat -A TELEPRESENCE_DNS -p udp --dest 10.0.0.10/32 --dport 53 -j DNAT --to-destination 127.0.0.1:33695"
	sidecarCheck := "-t nat -C OUTPUT -j ISTIO_OUTPUT"
	dnsCaptureCheck := "-t raw -C OUTPUT -j ISTIO_OUTPUT_DNS"

	tests := []struct {
		name           string
		sidecarExit    int
		dnsCaptureExit int
		wantOwner      bool
		wantConntrack  bool
		wantDNSCheck   bool
	}{
		{
			name:        "unmeshed clients preserve existing DNS routing",
			sidecarExit: 1,
		},
		{
			name:           "sidecar without DNS capture excludes only its proxy",
			dnsCaptureExit: 1,
			wantOwner:      true,
			wantDNSCheck:   true,
		},
		{
			name:          "sidecar DNS capture aligns only dynamic listener replies",
			wantOwner:     true,
			wantConntrack: true,
			wantDNSCheck:  true,
		},
		{
			name:        "sidecar inspection failure leaves DNS routing operational",
			sidecarExit: 4,
		},
		{
			name:           "DNS capture inspection failure retains proxy exemption",
			dnsCaptureExit: 4,
			wantOwner:      true,
			wantDNSCheck:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logFile := fakeIPTables(t, test.sidecarExit, test.dnsCaptureExit, "", "")
			require.NoError(t, routeDNS(testutil.NewContext(t, true), dnsAddress, listener, []netip.AddrPort{fallback}))
			commands := iptablesCommands(t, logFile)

			assert.Contains(t, commands, sidecarCheck)
			assert.Contains(t, commands, fallbackRule)
			assert.Contains(t, commands, dnatRule)
			assert.Contains(t, commands, "-t nat -I OUTPUT 1 -j TELEPRESENCE_DNS")

			if test.wantDNSCheck {
				assert.Contains(t, commands, dnsCaptureCheck)
			} else {
				assert.NotContains(t, commands, dnsCaptureCheck)
			}

			if test.wantOwner {
				assert.Contains(t, commands, ownerRule)
				assert.Less(t, slices.Index(commands, ownerRule), slices.Index(commands, dnatRule))
			} else {
				assert.NotContains(t, commands, ownerRule)
			}

			if test.wantConntrack {
				assert.Contains(t, commands, "-t raw -N TELEPRESENCE_DNS_RAW")
				assert.Contains(t, commands, conntrackRule)
				assert.Contains(t, commands, "-t raw -I OUTPUT 1 -j TELEPRESENCE_DNS_RAW")
			} else {
				assert.NotContains(t, commands, "-t raw -N TELEPRESENCE_DNS_RAW")
				assert.NotContains(t, commands, conntrackRule)
			}
		})
	}
}

func TestRouteDNSIstioFailureRollsBackBothTables(t *testing.T) {
	dnsAddress := netip.MustParseAddrPort("10.0.0.10:53")
	listener := netip.MustParseAddrPort("127.0.0.1:33695")
	logFile := fakeIPTables(t, 0, 0, "raw", "-A")

	require.Error(t, routeDNS(testutil.NewContext(t, true), dnsAddress, listener, nil))
	commands := iptablesCommands(t, logFile)
	require.GreaterOrEqual(t, len(commands), 6)
	assert.Equal(t, []string{
		"-t nat -D OUTPUT -j TELEPRESENCE_DNS",
		"-t nat -F TELEPRESENCE_DNS",
		"-t nat -X TELEPRESENCE_DNS",
		"-t raw -D OUTPUT -j TELEPRESENCE_DNS_RAW",
		"-t raw -F TELEPRESENCE_DNS_RAW",
		"-t raw -X TELEPRESENCE_DNS_RAW",
	}, commands[len(commands)-6:])
}

func TestRouteDNSIstioReconnectAndCleanupAreIdempotent(t *testing.T) {
	dnsAddress := netip.MustParseAddrPort("10.0.0.10:53")
	listener := netip.MustParseAddrPort("127.0.0.1:33695")
	logFile := fakeIPTables(t, 0, 0, "", "")
	c := testutil.NewContext(t, true)

	require.NoError(t, routeDNS(c, dnsAddress, listener, nil))
	require.NoError(t, routeDNS(c, dnsAddress, listener, nil))
	CleanupRouting(c)

	commands := iptablesCommands(t, logFile)
	countCommand := func(expected string) int {
		count := 0
		for _, actual := range commands {
			if actual == expected {
				count++
			}
		}
		return count
	}
	assert.Equal(t, 2, countCommand("-t nat -N TELEPRESENCE_DNS"))
	assert.Equal(t, 2, countCommand("-t raw -N TELEPRESENCE_DNS_RAW"))
	assert.Equal(t, 3, countCommand("-t nat -D OUTPUT -j TELEPRESENCE_DNS"))
	assert.Equal(t, 3, countCommand("-t raw -D OUTPUT -j TELEPRESENCE_DNS_RAW"))
	assert.Equal(t, []string{
		"-t nat -D OUTPUT -j TELEPRESENCE_DNS",
		"-t nat -F TELEPRESENCE_DNS",
		"-t nat -X TELEPRESENCE_DNS",
		"-t raw -D OUTPUT -j TELEPRESENCE_DNS_RAW",
		"-t raw -F TELEPRESENCE_DNS_RAW",
		"-t raw -X TELEPRESENCE_DNS_RAW",
	}, commands[len(commands)-6:])
}
