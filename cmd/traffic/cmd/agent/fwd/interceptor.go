package fwd

import (
	"context"
	"fmt"
	"net/netip"
	"sync"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/forwarder"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

type Interceptor interface {
	forwarder.Forwarder

	// IsHTTP returns true if this interceptor is for HTTP traffic.
	IsHTTP() bool
	// SupportsHTTPRouteGuards is true when requests on this port have been
	// mediated as HTTP since the listener's first connection.
	SupportsHTTPRouteGuards() bool

	SetIntercepting([]*manager.InterceptInfo)
	SetWiretapping([]*manager.InterceptInfo)
	SetRouteGuards(desired, removed []RouteGuard)
	MarkRouteSnapshotInstalled()
	SetPendingRouteGuards([]RouteGuard)
	SetStreamProvider(tunnel.ClientStreamProvider)
	Tag() tunnel.Tag

	// InterceptInfos returns the intercepts that are currently being handled by this interceptor. The
	// wiretaps are not included.
	InterceptInfos() []*manager.InterceptInfo
}

// RouteGuard protects one exact local HTTP route even when its live intercept
// has not been restored. An incarnation prevents a stale intercept from serving
// a route that was removed and recreated with the same name.
type RouteGuard struct {
	RoutingKey  string
	InterceptID string
	Incarnation string
}

type routeGuardIdentity struct {
	interceptID string
	incarnation string
}

func (g RouteGuard) identity() routeGuardIdentity {
	return routeGuardIdentity{interceptID: g.InterceptID, incarnation: g.Incarnation}
}

type routeGuardRetirement bool

const (
	routeNoLongerScoped    routeGuardRetirement = false
	routeExplicitlyRemoved routeGuardRetirement = true
)

type interceptor struct {
	forwarder.Forwarder
	mu             sync.Mutex
	lCtx           context.Context
	streamProvider tunnel.ClientStreamProvider
	wiretaps       interceptControllerMap
	intercepts     interceptControllerMap
	routeGuards    map[string][]RouteGuard
	pendingGuards  map[routeGuardIdentity]RouteGuard
	retiredGuards  map[routeGuardIdentity]routeGuardRetirement
	guardRoutes    bool
}

func (*interceptor) SupportsHTTPRouteGuards() bool { return false }

func (*interceptor) MarkRouteSnapshotInstalled() {}

func NewInterceptor(ctx context.Context, from types.PortAndProto, tag tunnel.Tag, target netip.AddrPort, opts ...forwarder.Option) Interceptor {
	switch from.Proto {
	case types.ProtoTCP:
		return NewTCPInterceptor(ctx, from, tag, nil, target, opts...)
	case types.ProtoUDP:
		return newUDP(ctx, from, tag, target, opts...)
	default:
		panic(fmt.Errorf("unsupported protocol %s", from.Proto))
	}
}

func newInterceptor(ctx context.Context, listenPort types.PortAndProto, tag tunnel.Tag, target netip.AddrPort, opts ...forwarder.Option) *interceptor {
	fx := &interceptor{
		Forwarder:  forwarder.New(listenPort, tag, target, opts...),
		lCtx:       ctx,
		intercepts: make(interceptControllerMap),
		wiretaps:   make(interceptControllerMap),
	}
	return fx
}

func (f *interceptor) InterceptInfos() []*manager.InterceptInfo {
	f.mu.Lock()
	infos := f.intercepts.sortedInfos()
	f.mu.Unlock()
	return infos
}

func (f *interceptor) WiretapInfos() []*manager.InterceptInfo {
	f.mu.Lock()
	infos := f.wiretaps.sortedInfos()
	f.mu.Unlock()
	return infos
}

func (f *interceptor) SetStreamProvider(streamProvider tunnel.ClientStreamProvider) {
	f.mu.Lock()
	f.streamProvider = streamProvider
	f.mu.Unlock()
}

func (f *interceptor) SetIntercepting(infos []*manager.InterceptInfo) {
	f.mu.Lock()
	f.intercepts.reconcile(f.lCtx, infos)
	clog.Debugf(f.lCtx, "SetIntercepting %d intercepts", len(f.intercepts))
	f.mu.Unlock()
}

func (f *interceptor) SetWiretapping(infos []*manager.InterceptInfo) {
	f.mu.Lock()
	f.wiretaps.reconcile(f.lCtx, infos)
	clog.Debugf(f.lCtx, "SetWiretapping %d wiretaps", len(f.wiretaps))
	f.mu.Unlock()
}

func (f *interceptor) SetRouteGuards(desired, removed []RouteGuard) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.retiredGuards == nil {
		f.retiredGuards = make(map[routeGuardIdentity]routeGuardRetirement)
	}
	if f.pendingGuards == nil {
		f.pendingGuards = make(map[routeGuardIdentity]RouteGuard)
	}
	for _, guard := range removed {
		identity := guard.identity()
		f.retiredGuards[identity] = routeExplicitlyRemoved
		delete(f.pendingGuards, identity)
	}
	byKey := make(map[string][]RouteGuard, len(desired))
	identities := make(map[routeGuardIdentity]struct{}, len(desired))
	for _, guard := range desired {
		identity := guard.identity()
		if retirement, ok := f.retiredGuards[identity]; ok {
			if retirement == routeExplicitlyRemoved {
				continue // an old snapshot cannot restore a removed incarnation
			}
			delete(f.retiredGuards, identity) // the service selected this workload again
		}
		delete(f.pendingGuards, identity)
		identities[identity] = struct{}{}
		byKey[guard.RoutingKey] = append(byKey[guard.RoutingKey], guard)
	}
	for _, guards := range f.routeGuards {
		for _, guard := range guards {
			identity := guard.identity()
			if _, stillScoped := identities[identity]; !stillScoped {
				if _, alreadyRetired := f.retiredGuards[identity]; !alreadyRetired {
					f.retiredGuards[identity] = routeNoLongerScoped
				}
			}
		}
	}
	f.routeGuards = byKey
	f.guardRoutes = true
}

// SetPendingRouteGuards closes the gap between the separate runtime and durable
// watches. A newly seen runtime route remains protected until a durable desired
// or explicit removal for that incarnation arrives; a concurrently queued older
// empty authoritative snapshot cannot remove it.
func (f *interceptor) SetPendingRouteGuards(guards []RouteGuard) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, guard := range guards {
		identity := guard.identity()
		if _, retired := f.retiredGuards[identity]; retired {
			continue
		}
		known := false
		for _, desired := range f.routeGuards[guard.RoutingKey] {
			if desired.identity() == identity {
				known = true
				break
			}
		}
		if !known {
			if f.pendingGuards == nil {
				f.pendingGuards = make(map[routeGuardIdentity]RouteGuard)
			}
			f.pendingGuards[identity] = guard
		}
	}
}
