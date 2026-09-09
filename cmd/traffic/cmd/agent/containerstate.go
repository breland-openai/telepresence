package agent

import (
	"context"
	"fmt"
	"net/netip"
	"strings"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/agent/fwd"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/agent/tls"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/forwarder"
	"github.com/telepresenceio/telepresence/v2/pkg/log"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

type containerState struct {
	State
	container  *agentconfig.Container
	mountPoint string
	env        map[string]string
}

func (c *containerState) AddPortHandler(g log.Group, pp types.PortAndProto, it agentconfig.InterceptTarget) {
	ph := c.newPortHandler(g, pp, it)
	c.AddInterceptState(c.NewInterceptState(ph, it, c.container.Name))
	g.Go(fmt.Sprintf("forward-%s-%s:%d", c.container.Name, it.Protocol(), it.ContainerPort()), func(ctx context.Context) error {
		return ph.Serve(tunnel.WithPool(ctx, tunnel.NewPool()), nil)
	})
}

func (c *containerState) newPortHandler(ctx context.Context, pp types.PortAndProto, ics agentconfig.InterceptTarget) fwd.Interceptor {
	ic := ics[0] // They all have the same protocol container port, so the first one will do.
	var opts []forwarder.Option
	if lf := c.ListenerFactory(); lf != nil {
		opts = append(opts, forwarder.WithListener(lf))
	}
	if d := c.DialerFactory(); d != nil {
		opts = append(opts, forwarder.WithDialer(d))
	}
	if pp.Proto != types.ProtoTCP {
		return fwd.NewInterceptor(ctx, pp, tunnel.AgentToClient, netip.AddrPort{}, opts...)
	}
	var defaultTarget netip.AddrPort
	var tlsManager tls.Manager
	if c.container.Replace == agentconfig.ReplacePolicyIntercept {
		// The agent's own pass-through dial to the real app -- made here,
		// once, when no intercept is active -- must land somewhere the
		// nftables pod-IP redirect gate doesn't reach; see the doc comment on
		// agentconfig.Sidecar.PassThroughTarget for the numeric/named
		// distinction. The TLS/H2C prober in cmd/traffic/cmd/agent/tls uses
		// the same helper for the identical reason.
		cfg := c.AgentConfig()
		nftRedirects := c.DialerFactory() != nil || cfg.NftRedirectsPort(ic.ContainerPort, pp.Proto)
		defaultTarget = cfg.PassThroughTarget(c.AppPodIP(), ic.ContainerPort, pp.Proto, nftRedirects)
		tlsManager = c.TLSManager()
	}
	if c.AgentConfig().RequireAuthoritativeRoutes && permanentHTTPMediation(ics.AppProtocol(ctx), tlsManager, defaultTarget.Port()) {
		return fwd.NewHTTPMediatedTCPInterceptor(ctx, pp, tunnel.AgentToClient, tlsManager, defaultTarget, opts...)
	}
	return fwd.NewTCPInterceptor(ctx, pp, tunnel.AgentToClient, tlsManager, defaultTarget, opts...)
}

func permanentHTTPMediation(appProtocol string, tlsManager tls.Manager, targetPort uint16) bool {
	switch strings.ToLower(appProtocol) {
	case "http", "kubernetes.io/http", "http1", "http1.0", "http1.1", "http/1.0", "http/1.1",
		"ws", "kubernetes.io/ws", "h2c", "kubernetes.io/h2c":
		return true
	case "https", "http2", "wss", "kubernetes.io/wss", "grpc":
		// End-to-end encrypted connections cannot expose request headers without
		// the application's downstream certificate. Certificate watchers complete
		// their initial load before Sidecar creates the port handlers. A plaintext
		// gRPC service can explicitly declare h2c to avoid ambiguous TLS detection.
		return tlsManager != nil && tlsManager.GetDownstreamCertificate(targetPort) != nil
	default:
		// Do not turn databases, opaque TCP, or undeclared ports into HTTP. An
		// unsupported port does not acknowledge a durable exact-header route.
		return false
	}
}

func (c *containerState) GlobalState() State {
	return c.State
}

func (c *containerState) Container() *agentconfig.Container {
	return c.container
}

func (c *containerState) MountPoint() string {
	return c.mountPoint
}

func (c *containerState) Mounts() types.MountPolicies {
	return c.container.Mounts
}

func (c *containerState) Env() map[string]string {
	return c.env
}

func (c *containerState) Name() string {
	return c.container.Name
}

func (c *containerState) ReplaceContainer() bool {
	return c.container.Replace == agentconfig.ReplacePolicyContainer
}

// HandleContainer on the containerState takes care of intercepts that just replaces a container and do not declare
// any ports. Without port declarations, there will be no Intercept entries for an fwdState to handle.
func (c *containerState) HandleContainer(ctx context.Context, iis []*manager.InterceptInfo) (rs []*manager.ReviewInterceptRequest) {
	for _, ii := range iis {
		if ii.Disposition == manager.InterceptDispositionType_WAITING {
			spec := ii.Spec
			if c.ReplaceContainer() && c.Name() == spec.ContainerName && spec.ContainerPort == 0 {
				clog.Debugf(ctx, "container %s handling replace %s", c.Name(), spec.Name)
				rs = append(rs, &manager.ReviewInterceptRequest{
					Id:          ii.Id,
					Disposition: manager.InterceptDispositionType_ACTIVE,
					PodIp:       c.PodIP().String(),
					SftpPort:    int32(c.SftpPort()),
					FtpPort:     int32(c.FtpPort()),
					MountPoint:  c.MountPoint(),
					Mounts:      c.Mounts().ToRPC(),
					Environment: c.Env(),
				})
			}
		}
	}
	return rs
}

// NewContainerState creates a ContainerState that provides the environment variables and the mount point for a container.
func (s *state) NewContainerState(gs State, cn *agentconfig.Container, mountPoint string, env map[string]string) ContainerState {
	return &containerState{
		State:      gs,
		container:  cn,
		mountPoint: mountPoint,
		env:        env,
	}
}
