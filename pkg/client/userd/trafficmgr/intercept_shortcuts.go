package trafficmgr

import (
	"context"
	"net/netip"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/matcher"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

// pushInterceptShortcuts declares the cluster-side destinations of the currently
// active intercepts to the root daemon, which connects them directly to their local
// intercept handlers instead of tunneling to the cluster. An empty set is pushed
// when the feature is disabled or no intercepts are active, since each push
// replaces the previous set.
func (s *session) pushInterceptShortcuts() {
	// Build the complete table only once earlier writes have finished; otherwise
	// a slow, older write could reinstall a shortcut after a durable confirmation.
	s.shortcutPushLock.Lock()
	defer s.shortcutPushLock.Unlock()
	var shortcuts []*daemon.InterceptShortcut
	if ic := client.GetConfig(s).Intercept(); ic.LocalShortcut {
		shortcuts = s.interceptShortcuts(s.getCurrentInterceptInfos(), ic.LocalShortcutIsGlobal)
	}
	err := s.WithRootClient(s, func(ctx context.Context, rd daemon.DaemonClient) error {
		_, err := rd.SetInterceptShortcuts(ctx, &daemon.SetInterceptShortcutsRequest{Shortcuts: shortcuts})
		return err
	})
	switch status.Code(err) {
	case codes.OK:
	case codes.Unimplemented:
		// The root daemon predates intercept shortcuts.
	case codes.FailedPrecondition, codes.Canceled:
		// The root daemon is reconnecting or the session is ending. The next
		// snapshot push repairs the state.
		clog.Debugf(s, "unable to set intercept shortcuts: %v", err)
	default:
		clog.Warnf(s, "failed to set intercept shortcuts: %v", err)
	}
}

// shortcutEligible returns true when the given spec describes an intercept whose
// cluster-side destinations can be served by the local intercept handler. A wiretap
// receives a copy of the traffic rather than the traffic itself, so it is never
// shortcut. Filtered intercepts only receive matching requests, so they qualify only
// when isGlobal is set, on the assumption that the filters exist to limit how the
// intercept impacts others, not the developer's own traffic.
func shortcutEligible(spec *manager.InterceptSpec, isGlobal bool) bool {
	if spec == nil || spec.Wiretap {
		return false
	}
	if isGlobal {
		return true
	}
	return len(spec.HeaderFilters) == 0 && len(spec.PathFilters) == 0 && (spec.Mechanism == "" || spec.Mechanism == "tcp")
}

type durableShortcutRoute struct{ id, routingKey string }

type pendingDurableShortcutRoute struct{ namespace, name, routingKey string }

// An exact local creation proposes a durable incarnation even to an old manager.
// Temporarily decline the shortcut while awaiting its response; an old manager's
// non-confirmation releases this guard and restores its configured legacy behavior.
func (s *session) beginDurableShortcutCandidate(spec *manager.InterceptSpec) func() {
	key := exactShortcutRoutingKey(spec)
	if spec.GetName() == "" || key == "" {
		return func() {}
	}
	pending := pendingDurableShortcutRoute{namespace: spec.Namespace, name: spec.Name, routingKey: key}
	s.durableShortcutLock.Lock()
	if s.pendingDurableShortcuts == nil {
		s.pendingDurableShortcuts = make(map[pendingDurableShortcutRoute]int)
	}
	s.pendingDurableShortcuts[pending]++
	s.durableShortcutLock.Unlock()
	return sync.OnceFunc(func() {
		s.durableShortcutLock.Lock()
		if s.pendingDurableShortcuts[pending] <= 1 {
			delete(s.pendingDurableShortcuts, pending)
		} else {
			s.pendingDurableShortcuts[pending]--
		}
		s.durableShortcutLock.Unlock()
		if s.Cluster != nil {
			s.pushInterceptShortcuts()
		}
	})
}

// requiresDurableRouteMediation remembers the manager's confirmation for this
// session. An old or replacement manager's missing field/snapshot cannot safely
// change a previously guarded exact rule back into an unconditional TCP shortcut.
func (s *session) requiresDurableRouteMediation(ii *manager.InterceptInfo) bool {
	required, _ := s.trackDurableRouteMediation(ii)
	return required
}

func (s *session) trackDurableRouteMediation(ii *manager.InterceptInfo) (required, newlyConfirmed bool) {
	confirmed := ii.GetRouteIncarnation() != ""
	key := exactShortcutRoutingKey(ii.GetSpec())
	if ii.GetId() == "" || key == "" {
		return confirmed, false
	}
	route := durableShortcutRoute{id: ii.Id, routingKey: key}
	s.durableShortcutLock.Lock()
	defer s.durableShortcutLock.Unlock()
	_, known := s.durableShortcutRoutes[route]
	if confirmed {
		if s.durableShortcutRoutes == nil {
			s.durableShortcutRoutes = make(map[durableShortcutRoute]struct{})
		}
		s.durableShortcutRoutes[route] = struct{}{}
		return true, !known
	}
	pending := pendingDurableShortcutRoute{namespace: ii.Spec.Namespace, name: ii.Spec.Name, routingKey: key}
	return known || s.pendingDurableShortcuts[pending] != 0, false
}

func exactShortcutRoutingKey(spec *manager.InterceptSpec) string {
	if spec == nil || spec.Wiretap || len(spec.HeaderFilters) != 1 || len(spec.PathFilters) != 0 {
		return ""
	}
	for name, key := range spec.HeaderFilters {
		if strings.EqualFold(name, "X-Local-Routing-Key") && key != "" && matcher.NewValue(key).Op() == matcher.ValueOpEqual {
			return key
		}
	}
	return ""
}

// interceptShortcuts derives the shortcut declarations from the active intercepts of
// the given snapshot.
func (s *session) interceptShortcuts(intercepts []*manager.InterceptInfo, isGlobal bool) []*daemon.InterceptShortcut {
	shortcuts := make([]*daemon.InterceptShortcut, 0, len(intercepts))
	for _, ii := range intercepts {
		if s.requiresDurableRouteMediation(ii) {
			// The manager confirms this intercept is an exact, durably guarded
			// local HTTP route. A connection-level shortcut cannot distinguish its
			// developer key from an unrelated or headerless request, including on
			// a reused connection, so let the traffic-agent inspect every request.
			continue
		}
		if ii.GetDisposition() != manager.InterceptDispositionType_ACTIVE {
			continue
		}
		spec := ii.Spec
		if !shortcutEligible(spec, isGlobal) {
			continue
		}
		addr, err := netip.ParseAddr(spec.TargetHost)
		if err != nil {
			continue
		}
		// The target host may be a synthetic IP representing a hostname.
		if addr, err = s.Resolve(addr); err != nil {
			clog.Debugf(s, "no intercept shortcut for %s: %v", spec.Name, err)
			continue
		}
		target, err := netip.AddrPortFrom(addr, uint16(spec.TargetPort)).MarshalBinary()
		if err != nil {
			continue
		}
		proto := spec.Protocol
		if proto == "" {
			proto = types.ProtoTCP.String()
		}
		shortcuts = append(shortcuts, &daemon.InterceptShortcut{
			Namespace:     spec.Namespace,
			Workload:      spec.Agent,
			Protocol:      proto,
			ServiceAddrs:  shortcutServiceAddrs(spec),
			ContainerPort: uint32(spec.ContainerPort),
			Target:        target,
		})
	}
	return shortcuts
}

// shortcutServiceAddrs returns the ClusterIP:servicePort addresses of the service
// that the given intercept is made on, each in binary form. The cluster IPs are
// resolved by the traffic-manager when the intercept is prepared.
func shortcutServiceAddrs(spec *manager.InterceptSpec) (addrs [][]byte) {
	if spec.ServicePort == 0 {
		return nil
	}
	for _, ipb := range spec.ServiceIps {
		ip, ok := netip.AddrFromSlice(ipb)
		if !ok {
			continue
		}
		if ab, err := netip.AddrPortFrom(ip, uint16(spec.ServicePort)).MarshalBinary(); err == nil {
			addrs = append(addrs, ab)
		}
	}
	return addrs
}
