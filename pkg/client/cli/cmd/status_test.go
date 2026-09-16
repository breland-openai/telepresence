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
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/telepresenceio/telepresence/rpc/v2/common"
	"github.com/telepresenceio/telepresence/rpc/v2/connector"
	daemonRpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/filelocation"
	"github.com/telepresenceio/telepresence/v2/pkg/ioutil"
)

func TestStatusReportsConnectingWithoutRemoteMetadata(t *testing.T) {
	ctx := filelocation.WithAppUserCacheDir(t.Context(), t.TempDir())
	ctx = filelocation.WithAppUserConfigDir(ctx, t.TempDir())
	ud := &statusAgentImageTestClient{
		called: make(chan context.Context, 1), statusErr: grpcStatus.Error(codes.FailedPrecondition, "connection in progress"),
	}
	previousExtras := GetTrafficManagerStatusExtras
	var extraCalls int
	GetTrafficManagerStatusExtras = func(context.Context, daemon.UserClient) ioutil.KeyValueProvider {
		extraCalls++
		return nil
	}
	t.Cleanup(func() { GetTrafficManagerStatusExtras = previousExtras })

	info, err := getStatusInfo(daemon.WithUserClient(ctx, ud), nil)
	require.NoError(t, err)
	require.Equal(t, "Connecting", info.UserDaemon.Status)
	require.True(t, info.UserDaemon.Running)
	require.Equal(t, "test", info.UserDaemon.Name)
	require.Equal(t, "2.33.0", info.UserDaemon.Version)
	require.Equal(t, "/test/telepresence", info.UserDaemon.Executable)
	require.NotEmpty(t, info.UserDaemon.InstallID)
	require.Empty(t, info.UserDaemon.Error)
	require.False(t, info.RootDaemon.Running)
	require.Empty(t, info.TrafficManager.Name)
	require.Equal(t, 1, ud.statusCalls)
	require.Zero(t, ud.optionalCalls)
	require.Zero(t, extraCalls)

	for _, tc := range []struct {
		name   string
		output ioutil.WriterTos
		multi  bool
	}{
		{name: "single", output: &SingleConnectStatusInfo{statusInfo: info}},
		{name: "multi", output: &MultiConnectStatusInfo{statusInfos: []ioutil.WriterTos{info}}, multi: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, marshalErr := json.Marshal(tc.output)
			require.NoError(t, marshalErr)
			var document map[string]any
			require.NoError(t, json.Unmarshal(data, &document))
			if tc.multi {
				connections := document["connections"].([]any)
				require.Len(t, connections, 1)
				document = connections[0].(map[string]any)
			}
			u := document["user_daemon"].(map[string]any)
			require.Equal(t, "Connecting", u["status"])
			require.Equal(t, true, u["running"])
			require.Equal(t, "test", u["name"])
			require.Equal(t, "/test/telepresence", u["executable"])
			require.NotContains(t, u, "error")
			require.Equal(t, false, document["root_daemon"].(map[string]any)["running"])
			require.Empty(t, document["traffic_manager"].(map[string]any)["name"])

			var human bytes.Buffer
			_, writeErr := ioutil.WriteAllTo(&human, tc.output.WriterTos()...)
			require.NoError(t, writeErr)
			require.Contains(t, human.String(), "userd: Running")
			require.Regexp(t, `(?m)^\s*Status\s*:\s*Connecting\s*$`, human.String())
			require.Contains(t, human.String(), "/test/telepresence")
		})
	}
}

func TestStatusDoesNotHideOtherGRPCErrors(t *testing.T) {
	for _, tc := range []struct {
		code    codes.Code
		message string
	}{
		{code: codes.FailedPrecondition, message: "root daemon is reconnecting"},
		{code: codes.FailedPrecondition, message: "connection in progress: other failure"},
		{code: codes.Unavailable, message: "connection in progress"},
	} {
		t.Run(tc.message+tc.code.String(), func(t *testing.T) {
			ctx := filelocation.WithAppUserCacheDir(t.Context(), t.TempDir())
			ctx = filelocation.WithAppUserConfigDir(ctx, t.TempDir())
			ud := &statusAgentImageTestClient{called: make(chan context.Context, 1), statusErr: grpcStatus.Error(tc.code, tc.message)}
			info, err := getStatusInfo(daemon.WithUserClient(ctx, ud), nil)
			require.Nil(t, info)
			require.ErrorContains(t, err, tc.message)
			require.Zero(t, ud.optionalCalls)

			var daemonStatus UserDaemonStatus
			connectInfo, err := setUserDaemonStatus(ctx, ud, nil, &daemonStatus)
			require.Nil(t, connectInfo)
			require.ErrorContains(t, err, tc.message)
			require.Equal(t, "Not connected", daemonStatus.Status)
			require.Contains(t, daemonStatus.Error, tc.message)
		})
	}
}

func TestStatusReportsCoreStatusWhenOptionalAgentImageStalls(t *testing.T) {
	type statusResult struct {
		info *StatusInfo
		err  error
	}
	ctx := filelocation.WithAppUserCacheDir(t.Context(), t.TempDir())
	ctx = filelocation.WithAppUserConfigDir(ctx, t.TempDir())
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
	case response := <-result:
		require.NoError(t, response.err, "status returned before optional agent metadata was requested")
		select {
		case metadataCtx := <-ud.called:
			deadline, ok := metadataCtx.Deadline()
			require.True(t, ok)
			require.LessOrEqual(t, time.Until(deadline), 2*time.Second)
		default:
			t.Fatal("status returned before optional agent metadata was requested")
		}
		result <- response
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
	called        chan context.Context
	stall         bool
	statusErr     error
	statusCalls   int
	optionalCalls int
}

func (*statusAgentImageTestClient) Containerized() bool    { return false }
func (*statusAgentImageTestClient) Executable() string     { return "/test/telepresence" }
func (*statusAgentImageTestClient) Name() string           { return "userd" }
func (*statusAgentImageTestClient) Semver() semver.Version { return semver.MustParse("2.33.0") }
func (*statusAgentImageTestClient) DaemonID() *daemon.Identifier {
	return daemon.NewIdentifier("test", "test", "default", false)
}

func (c *statusAgentImageTestClient) Status(context.Context, *emptypb.Empty, ...grpc.CallOption) (*connector.ConnectInfo, error) {
	c.statusCalls++
	if c.statusErr != nil {
		return nil, c.statusErr
	}
	return &connector.ConnectInfo{
		ClusterContext: "test",
		ManagerVersion: &manager.VersionInfo2{Name: "manager", Version: "v2.33.0"},
	}, nil
}

func (c *statusAgentImageTestClient) AgentImageFQN(ctx context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*manager.AgentImageFQN, error) {
	c.optionalCalls++
	c.called <- ctx
	if c.stall {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &manager.AgentImageFQN{FQN: "registry/agent:tag"}, nil
}

func (c *statusAgentImageTestClient) RootDaemonVersion(context.Context, *emptypb.Empty, ...grpc.CallOption) (*common.VersionInfo, error) {
	c.optionalCalls++
	return &common.VersionInfo{}, nil
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
