package cmd

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/telepresence/rpc/v2/connector"
	daemonRpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/filelocation"
)

func TestStatusReportsCoreStatusWhenOptionalAgentImageStalls(t *testing.T) {
	type statusResult struct {
		info *StatusInfo
		err  error
	}
	ctx := filelocation.WithAppUserCacheDir(t.Context(), t.TempDir())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ud := &statusAgentImageTestClient{called: make(chan context.Context, 1), stall: true}
	result := make(chan statusResult, 1)
	go func() {
		info, err := getStatusInfo(daemon.WithUserClient(ctx, ud), nil)
		result <- statusResult{info, err}
	}()
	select {
	case metadataCtx := <-ud.called:
		deadline, ok := metadataCtx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), 2*time.Second)
	case <-time.After(4 * time.Second):
		t.Fatal("optional agent image was not requested")
	}
	select {
	case response := <-result:
		require.NoError(t, response.err)
		require.Equal(t, "Connected", response.info.UserDaemon.Status)
		require.Equal(t, "manager", response.info.TrafficManager.Name)
		require.Equal(t, "v2.33.0", response.info.TrafficManager.Version)
		require.Empty(t, response.info.TrafficManager.TrafficAgent)
	case <-time.After(4 * time.Second):
		t.Fatal("optional agent image prevented the core daemon status from being reported")
	}
	require.NoError(t, ctx.Err())

	ud = &statusAgentImageTestClient{called: make(chan context.Context, 1)}
	info, err := getStatusInfo(daemon.WithUserClient(ctx, ud), nil)
	require.NoError(t, err)
	require.Equal(t, "registry/agent:tag", info.TrafficManager.TrafficAgent)
}

type statusAgentImageTestClient struct {
	daemon.UserClient
	called chan context.Context
	stall  bool
}

func (*statusAgentImageTestClient) Containerized() bool    { return false }
func (*statusAgentImageTestClient) Executable() string     { return "/test/telepresence" }
func (*statusAgentImageTestClient) Name() string           { return "userd" }
func (*statusAgentImageTestClient) Semver() semver.Version { return semver.MustParse("2.33.0") }
func (*statusAgentImageTestClient) DaemonID() *daemon.Identifier {
	return daemon.NewIdentifier("test", "test", "default", false)
}

func (*statusAgentImageTestClient) Status(context.Context, *emptypb.Empty, ...grpc.CallOption) (*connector.ConnectInfo, error) {
	return &connector.ConnectInfo{
		ClusterContext: "test",
		ManagerVersion: &manager.VersionInfo2{Name: "manager", Version: "v2.33.0"},
	}, nil
}

func (c *statusAgentImageTestClient) AgentImageFQN(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*manager.AgentImageFQN, error) {
	c.called <- ctx
	if c.stall {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &manager.AgentImageFQN{FQN: "registry/agent:tag"}, nil
}

func TestStatusInfoDurationJSON(t *testing.T) {
	s := &StatusInfo{RootDaemon: RootDaemonStatus{
		Running: true,
		DNS:     &client.DNSSnake{LookupTimeout: 4 * time.Second},
	}}
	data, err := s.MarshalJSON()
	require.NoError(t, err)
	assert.Contains(t, string(data), `"lookup_timeout":"4s"`)
}

func TestFormatTunnelTransport(t *testing.T) {
	tests := []struct {
		name string
		tt   *daemonRpc.TunnelTransport
		want string
	}{
		{"nil, old daemon", nil, ""},
		{"zero value", &daemonRpc.TunnelTransport{}, ""},
		{"grpc, no endpoint", &daemonRpc.TunnelTransport{Transport: "grpc"}, "grpc"},
		{"quic with endpoint", &daemonRpc.TunnelTransport{Transport: "quic", Endpoint: "1.2.3.4:7778"}, "quic (1.2.3.4:7778)"},
		{"fallback, no endpoint", &daemonRpc.TunnelTransport{Transport: "grpc (fallback)"}, "grpc (fallback)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatTunnelTransport(tc.tt))
		})
	}
}

// TestRootDaemonStatus_TunnelTransport verifies that the tunnel transport line
// appears in both the text and JSON status output when known, and is omitted
// entirely (degrading gracefully against an old root daemon) when empty.
func TestRootDaemonStatus_TunnelTransport(t *testing.T) {
	t.Run("present", func(t *testing.T) {
		ds := &RootDaemonStatus{Running: true, Name: "Root Daemon", TunnelTransport: "quic (1.2.3.4:7778)"}

		buf := &bytes.Buffer{}
		_, err := ds.WriteTo(buf)
		require.NoError(t, err)
		assert.Contains(t, buf.String(), "Tunnel transport")
		assert.Contains(t, buf.String(), "quic (1.2.3.4:7778)")

		js, err := json.Marshal(ds)
		require.NoError(t, err)
		assert.Contains(t, string(js), `"tunnel_transport":"quic (1.2.3.4:7778)"`)
	})

	t.Run("absent", func(t *testing.T) {
		ds := &RootDaemonStatus{Running: true, Name: "Root Daemon"}

		buf := &bytes.Buffer{}
		_, err := ds.WriteTo(buf)
		require.NoError(t, err)
		assert.NotContains(t, buf.String(), "Tunnel transport")

		js, err := json.Marshal(ds)
		require.NoError(t, err)
		assert.NotContains(t, string(js), "tunnel_transport")
	})
}
