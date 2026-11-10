package fwd

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/forwarder"
	"github.com/telepresenceio/telepresence/v2/pkg/iputil"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

type udp struct {
	*interceptor
	forwarding *udpForwarding // guarded by interceptor.mu
}

type udpForwarding struct {
	conn    net.Conn
	changed chan struct{}
	once    sync.Once
}

type udpInterceptRoute struct {
	active           bool
	id               string
	clientSession    string
	targetHost       string
	targetPort       int32
	roundtripLatency int64
	dialTimeout      int64
}

func udpRouteFor(ic *interceptController) udpInterceptRoute {
	if ic == nil {
		return udpInterceptRoute{}
	}
	spec := ic.GetSpec()
	return udpInterceptRoute{
		active:           true,
		id:               ic.GetId(),
		clientSession:    ic.GetClientSession().GetSessionId(),
		targetHost:       spec.GetTargetHost(),
		targetPort:       spec.GetTargetPort(),
		roundtripLatency: spec.GetRoundtripLatency(),
		dialTimeout:      spec.GetDialTimeout(),
	}
}

func (r *udpForwarding) reset() {
	r.once.Do(func() {
		close(r.changed)
		// Closing lets the current reader drain and finish before its context
		// is canceled; ServeTo then binds the same port for the new route.
		_ = r.conn.Close()
	})
}

func newUDP(ctx context.Context, listenPort types.PortAndProto, tag tunnel.Tag, target netip.AddrPort, opts ...forwarder.Option) Interceptor {
	return &udp{interceptor: newInterceptor(ctx, listenPort, tag, target, opts...)}
}

func (f *udp) IsHTTP() bool {
	return false
}

func (f *udp) SetIntercepting(infos []*manager.InterceptInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	previous, _ := f.intercepts.global()
	previousRoute := udpRouteFor(previous)
	f.intercepts.reconcile(f.lCtx, infos)
	current, _ := f.intercepts.global()
	if previousRoute != udpRouteFor(current) && f.forwarding != nil {
		f.forwarding.reset()
	}
	clog.Debugf(f.lCtx, "SetIntercepting %d intercepts", len(f.intercepts))
}

func (f *udp) Serve(_ context.Context, initCh chan<- netip.AddrPort) error {
	return f.ServeTo(f.lCtx, initCh, f.Forward)
}

func (f *udp) Forward(ctx context.Context, conn net.Conn) error {
	ctx, cancel := context.WithCancel(ctx)
	route := &udpForwarding{conn: conn, changed: make(chan struct{})}
	f.mu.Lock()
	f.forwarding = route
	intercept, err := f.intercepts.global()
	var info *manager.InterceptInfo
	if intercept != nil {
		info = intercept.InterceptInfo
	}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		if f.forwarding == route {
			f.forwarding = nil
		}
		f.mu.Unlock()
		cancel()
		_ = conn.Close()
	}()
	if err != nil {
		return err
	}
	if info != nil {
		return f.interceptConn(ctx, conn, info)
	}
	if f.Target().Port() == 0 {
		// A replaced app has no destination. Keep the socket bound while
		// inactive, without repeatedly opening it or retaining old datagrams
		// when a client intercept later becomes active.
		select {
		case <-ctx.Done():
		case <-route.changed:
		}
		return nil
	}
	return f.Forwarder.Forward(ctx, conn)
}

func (f *udp) interceptConn(ctx context.Context, conn net.Conn, info *manager.InterceptInfo) error {
	spec := info.Spec
	ip, err := iputil.ParseAddr(spec.TargetHost)
	if err != nil {
		return err
	}
	dest := netip.AddrPortFrom(ip, uint16(spec.TargetPort))
	clog.Infof(ctx, "Forwarding udp from %s to %s %s", conn.LocalAddr(), spec.Client, dest)
	defer clog.Infof(ctx, "Done forwarding udp from %s to %s %s", conn.LocalAddr(), spec.Client, dest)
	d := tunnel.NewUDPListener(conn.(*net.UDPConn), tunnel.AgentToClient, dest, func(ctx context.Context, id tunnel.ConnID) (tunnel.Stream, error) {
		f.mu.Lock()
		sp := f.streamProvider
		f.mu.Unlock()
		return sp.CreateClientStream(
			ctx, tunnel.AgentToClient, tunnel.SessionID(info.ClientSession.SessionId), id, time.Duration(spec.RoundtripLatency), time.Duration(spec.DialTimeout))
	})
	d.Start(ctx)
	<-d.Done()
	return nil
}
