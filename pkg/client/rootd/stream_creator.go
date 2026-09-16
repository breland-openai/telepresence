package rootd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func (s *session) isForDNS(ip netip.Addr, port uint16) bool {
	return s.vifDNS.Addr() == ip && s.vifDNS.Port() == port
}

func (s *session) applyPortMapping(id tunnel.ConnID) tunnel.ConnID {
	if s.l4PortMap == nil {
		return id
	}
	destAddr := id.DestinationAddr()
	if mp, ok := s.l4PortMap.Load(types.AddrPortProto{
		AddrPort: netip.AddrPortFrom(destAddr, id.DestinationPort()),
		Proto:    id.Protocol(),
	}); ok {
		return tunnel.NewConnID(id.Protocol(), id.Source(), netip.AddrPortFrom(destAddr, mp))
	}
	return id
}

func (s *session) localClientRedirectTarget(id tunnel.ConnID) (netip.AddrPort, bool) {
	if s.localClientRedirects == nil {
		return netip.AddrPort{}, false
	}
	return s.localClientRedirects.Load(types.AddrPortProto{
		AddrPort: id.Destination(),
		Proto:    id.Protocol(),
	})
}

func (s *session) newLocalClientRedirectStream(c context.Context, id tunnel.ConnID, target netip.AddrPort) tunnel.Stream {
	pipeID := tunnel.NewConnID(id.Protocol(), id.Source(), target)
	clog.Debugf(c, "Redirecting local client traffic for %s to %s", id.Destination(), target)
	from, to := tunnel.NewPipe(pipeID, tunnel.SessionID(s.session.SessionId), tunnel.LocalToTun, tunnel.TunToLocal)
	tunnel.NewDialer(to, func() {}, nil, nil).Start(c)
	return from
}

// checkRecursion checks that the given IP is not contained in any of the subnets
// that the VIF is configured with. When that's the case, the VIF is somehow receiving
// requests that originate from the cluster and dispatching it leads to infinite recursion.
func checkRecursion(p types.Proto, ip netip.Addr, sn netip.Prefix) (err error) {
	if sn.Contains(ip) && ip != sn.Masked().Addr() {
		err = fmt.Errorf("refusing recursive %s %s dispatch from pod subnet %s", p, ip, sn)
	}
	return err
}

func (s *session) streamCreator() tunnel.StreamCreator {
	return func(c context.Context, id tunnel.ConnID) (stream tunnel.Stream, err error) {
		// Telemetry: count every attempt as either a success or an error,
		// except DNS pipes (handled below) which are not real outbound
		// tunnels to the cluster.
		var countAsOutbound bool
		defer func() {
			if !countAsOutbound {
				return
			}
			if err != nil {
				s.outboundTunnelErrors.Add(1)
			} else {
				s.outboundTunnels.Add(1)
			}
		}()

		p := id.Protocol()
		srcIp := id.SourceAddr()
		for _, podSn := range s.podSubnets {
			if err := checkRecursion(p, srcIp, podSn); err != nil {
				// Recursive dispatch is a tunnel-creation attempt that
				// failed; count it.
				countAsOutbound = true
				return nil, err
			}
		}

		destAddr := id.DestinationAddr()
		if p == types.ProtoUDP {
			if s.isForDNS(destAddr, id.DestinationPort()) {
				pipeId := tunnel.NewConnID(p, id.Source(), s.localDNS)
				clog.Tracef(c, "Intercept DNS %s to %s", id, pipeId.Destination())
				from, to := tunnel.NewPipe(pipeId, tunnel.SessionID(s.session.SessionId), tunnel.DnsToTun, tunnel.TunToDNS)
				ttl := tunnel.DNSConnTTL(client.GetConfig(c).DNS().LookupTimeout)
				tunnel.NewDialerTTL(to, func() {}, ttl, nil, nil).Start(c)
				return from, nil
			}
		}
		// Past the DNS short-circuit, every outcome counts as an outbound
		// tunnel attempt.
		countAsOutbound = true

		id = s.applyPortMapping(id)
		destAddr = id.DestinationAddr()
		if target, ok := s.localClientRedirectTarget(id); ok {
			return s.newLocalClientRedirectStream(c, id, target), nil
		}

		var tp tunnel.Provider
		var optionalServiceAgent bool
		if a, ok := s.getAgentVIP(destAddr); ok {
			// s.agentClients is never nil when agentVIPs are used.
			if a.workload != "" {
				tp = s.agentClients.GetWorkloadClient(a.workload)
				if tp == nil {
					return nil, fmt.Errorf("unable to connect to a traffic-agent for workload %q", a.workload)
				}
				// Replace the virtual IP with the original destination IP. This will ensure that the agent
				// dials the original destination when the tunnel is established.
				id = tunnel.NewConnID(id.Protocol(), id.Source(), netip.AddrPortFrom(a.destinationIP, id.DestinationPort()))
				clog.Debugf(c, "Opening proxy-via %s tunnel for id %s", a.workload, id)
			} else {
				clog.Debugf(c, "Translating proxy-via %s to %s", destAddr, a.destinationIP)
				destAddr = a.destinationIP
				id = tunnel.NewConnID(id.Protocol(), id.Source(), netip.AddrPortFrom(destAddr, id.DestinationPort()))
			}
		}

		// A destination that one of the client's own intercepts covers is served by
		// the local intercept handler, so the connection is piped directly to it
		// instead of round-tripping through the cluster.
		if lt, ok := s.shortcutTarget(p, id.Destination()); ok {
			countAsOutbound = false
			pipeId := tunnel.NewConnID(p, id.Source(), lt)
			clog.Debugf(c, "Shortcutting %s to local intercept handler %s", id, lt)
			from, to := tunnel.NewPipe(pipeId, tunnel.SessionID(s.session.SessionId), tunnel.LocalToTun, tunnel.TunToLocal)
			tunnel.NewDialer(to, func() {}, nil, nil).Start(c)
			if p == types.ProtoTCP {
				s.MarkActivity()
			}
			return from, nil
		}

		if tp == nil {
			if s.isAlsoProxyDestination(destAddr) {
				tp = s.managerTunnelProvider()
				clog.Debugf(c, "Opening traffic-manager tunnel for also-proxy id %s", id)
			} else if tp = s.getAgentClient(destAddr); tp != nil {
				optionalServiceAgent = s.isClusterServiceDestination(destAddr)
				clog.Debugf(c, "Opening traffic-agent tunnel for id %s using agent %s", id, tp)
			} else {
				tp = s.managerTunnelProvider()
				clog.Debugf(c, "Opening traffic-manager tunnel for id %s", id)
			}
		}
		var cs tunnel.Stream
		var activityMarked bool
		if optionalServiceAgent {
			// A sidecar is only a relay for a cluster Service. Retire a failed
			// setup before asking the manager to reach the same destination. A
			// confirmed stream stays with its original provider and is never retried.
			agentCtx, cancelAgent := context.WithCancel(c)
			stopAgentCancel := context.AfterFunc(c, cancelAgent)
			cs, err = s.openTunnelStream(agentCtx, tp, id, true, &activityMarked)
			if err != nil {
				stopAgentCancel()
				cancelAgent()
				if c.Err() == nil && (status.Code(err) == codes.Unavailable || errors.Is(err, io.EOF)) {
					clog.Debugf(c, "Traffic-agent unavailable for cluster Service id %s; opening traffic-manager tunnel: %v", id, err)
					cs, err = s.openTunnelStream(c, s.managerTunnelProvider(), id, false, &activityMarked)
				}
			}
		} else {
			cs, err = s.openTunnelStream(c, tp, id, false, &activityMarked)
		}
		if err != nil {
			return nil, err
		}
		if id.Protocol() == types.ProtoUDP {
			// A no-op unless ct is backed by a QUIC connection that negotiated
			// datagrams (never true for tp == s.agentClients.* today, since agentpf
			// wraps a QUIC stream as a net.Conn for a real gRPC client rather than
			// using pkg/tunnel's own framing); detach is called once this flow's
			// context ends rather than here, since the stream is only just starting.
			detach := tunnel.AttachDatagramRoute(cs)
			go func() {
				<-c.Done()
				detach()
			}()
		}
		return cs, nil
	}
}

func (s *session) openTunnelStream(c context.Context, tp tunnel.Provider, id tunnel.ConnID, readStatusOnEOF bool, activityMarked *bool) (tunnel.Stream, error) {
	ct, err := tp.Tunnel(c)
	if err != nil {
		return nil, err
	}
	if id.Protocol() == types.ProtoTCP && !*activityMarked {
		s.MarkActivity()
		*activityMarked = true
	}
	tc := client.GetConfig(c).Timeouts()
	cs, err := tunnel.NewClientStream(c, tunnel.TunToClient, ct, id, tunnel.SessionID(s.session.SessionId),
		tc.Get(client.TimeoutRoundtripLatency), tc.Get(client.TimeoutEndpointDial))
	if readStatusOnEOF && errors.Is(err, io.EOF) {
		// gRPC can return only EOF from Send when the server closed before the
		// handshake. Recv reveals the actual status, if any, so an explicit
		// authentication or routing failure does not become a manager retry.
		if _, finalErr := ct.Recv(); finalErr == nil {
			// A message may even be StreamOK. The agent received this attempt,
			// and its result is ambiguous; leave the connection failed rather
			// than starting another stream through the manager.
			return nil, status.Error(codes.FailedPrecondition, "traffic-agent responded after the initial tunnel send failed")
		} else if !errors.Is(finalErr, io.EOF) {
			return nil, finalErr
		}
	}
	return cs, err
}

func (s *session) isClusterServiceDestination(ip netip.Addr) bool {
	// Direct and headless Pod traffic retains its selected physical agent, even
	// if the reported ranges happen to overlap.
	for _, sn := range s.podSubnets {
		if sn.Contains(ip) {
			return false
		}
	}
	for _, sn := range s.serviceSubnets {
		if sn.Contains(ip) {
			return true
		}
	}
	return false
}

func (s *session) isAlsoProxyDestination(ip netip.Addr) bool {
	for _, sn := range s.alsoProxySubnets {
		if sn.Contains(ip) {
			return true
		}
	}
	return false
}

func (s *session) getAgentVIP(dest netip.Addr) (a agentVIP, ok bool) {
	if s.virtualIPs != nil {
		a, ok = s.virtualIPs.Load(dest)
	}
	return a, ok
}

func (s *session) getAgentClient(ip netip.Addr) (pvd tunnel.Provider) {
	if s.agentClients != nil {
		pvd = s.agentClients.GetClient(ip)
	}
	return pvd
}
