//go:build linux

package dns

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/pkg/shellquote"
)

// CleanupStaleRouting removes recognized DNS redirects whose local UDP endpoint
// can be exclusively reserved. Live listeners and shared chains are preserved.
// Inspection or deletion failures stop cleanup and are returned to the caller.
func CleanupStaleRouting(c context.Context) error {
	c, cancel := context.WithTimeout(c, 2*time.Second)
	defer cancel()
	nat, err := readStaleRoutingRules(c, "nat")
	if err != nil {
		return err
	}
	if !strings.Contains(nat, "-A "+tpDNSChain+" ") {
		return nil
	}
	raw, err := readStaleRoutingRules(c, "raw")
	if err != nil {
		return err
	}
	return cleanupStaleRouting(c, nat, raw, func(c context.Context, table string, rule []string) error {
		args := append([]string{"-w", "1", "-t", table, "-D"}, rule[1:]...)
		if err := exec.CommandContext(c, "iptables", args...).Run(); err != nil {
			return fmt.Errorf("remove stale %s DNS rule: %w", table, err)
		}
		return nil
	})
}

func readStaleRoutingRules(c context.Context, table string) (string, error) {
	out, err := exec.CommandContext(c, "iptables", "-w", "1", "-t", table, "-S").Output()
	if err != nil {
		return "", fmt.Errorf("inspect %s DNS routing: %w", table, err)
	}
	return string(out), nil
}

func cleanupStaleRouting(c context.Context, nat, raw string, remove func(context.Context, string, []string) error) error {
	for _, line := range strings.Split(nat, "\n") {
		rule, endpoint := staleDNSRule(line, tpDNSChain, "DNAT")
		if !endpoint.IsValid() {
			continue
		}
		if err := removeStaleDNSRule(c, rule, endpoint, raw, remove); err != nil {
			return err
		}
	}
	return nil
}

func removeStaleDNSRule(c context.Context, rule []string, endpoint netip.AddrPort, raw string, remove func(context.Context, string, []string) error) error {
	// Keep the port reserved until every exact-rule deletion completes. A new
	// DNS listener cannot acquire this endpoint while its old rules are removed.
	lc := net.ListenConfig{}
	guard, err := lc.ListenPacket(c, "udp4", endpoint.String())
	if err != nil {
		if errors.Is(err, unix.EADDRINUSE) {
			clog.Debugf(c, "Preserving DNS routing to occupied endpoint %s", endpoint)
			return nil
		}
		return fmt.Errorf("reserve stale DNS endpoint %s: %w", endpoint, err)
	}
	defer guard.Close()

	if err = remove(c, "nat", rule); err != nil {
		return err
	}
	for _, line := range strings.Split(raw, "\n") {
		rawRule, rawEndpoint := staleDNSRule(line, tpDNSRawChain, "CT")
		if rawEndpoint == endpoint {
			if err = remove(c, "raw", rawRule); err != nil {
				return err
			}
		}
	}
	clog.Infof(c, "Removed stale DNS routing to %s", endpoint)
	return nil
}

func staleDNSRule(line, chain, target string) ([]string, netip.AddrPort) {
	args, err := shellquote.Split(line)
	if err != nil || len(args) < 2 || args[0] != "-A" || args[1] != chain || len(args)%2 != 0 {
		return nil, netip.AddrPort{}
	}
	values := make(map[string]string)
	for i := 2; i < len(args); i += 2 {
		key := args[i]
		switch key {
		case "--dest":
			key = "-d"
		case "--source":
			key = "-s"
		}
		if _, found := values[key]; found {
			return nil, netip.AddrPort{}
		}
		values[key] = args[i+1]
	}
	if values["-p"] != "udp" || values["-j"] != target || (values["-m"] != "" && values["-m"] != "udp") {
		return nil, netip.AddrPort{}
	}
	allowed := []string{"-p", "-j", "-m", "-d", "--dport", "--to-destination"}
	if target == "CT" {
		allowed = []string{"-p", "-j", "-m", "-s", "--sport", "--zone"}
	}
	for key := range values {
		if !slices.Contains(allowed, key) {
			return nil, netip.AddrPort{}
		}
	}
	var endpoint netip.AddrPort
	if target == "DNAT" {
		prefix, prefixErr := netip.ParsePrefix(values["-d"])
		if prefixErr != nil || !prefix.Addr().Is4() || prefix.Bits() != 32 || !validDNSRulePort(values["--dport"]) {
			return nil, netip.AddrPort{}
		}
		endpoint, err = netip.ParseAddrPort(values["--to-destination"])
	} else {
		prefix, prefixErr := netip.ParsePrefix(values["-s"])
		port, portErr := strconv.ParseUint(values["--sport"], 10, 16)
		if prefixErr != nil || prefix.Bits() != 32 || portErr != nil || port == 0 || !validDNSRulePort(values["--zone"]) {
			return nil, netip.AddrPort{}
		}
		endpoint = netip.AddrPortFrom(prefix.Addr(), uint16(port))
	}
	if err != nil || endpoint.Addr() != netip.MustParseAddr("127.0.0.1") || endpoint.Port() == 0 {
		return nil, netip.AddrPort{}
	}
	return args, endpoint
}

func validDNSRulePort(value string) bool {
	port, err := strconv.ParseUint(value, 10, 16)
	return err == nil && port != 0
}
