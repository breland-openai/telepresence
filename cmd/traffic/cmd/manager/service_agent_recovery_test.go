package manager

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"

	"github.com/telepresenceio/clog/testutil"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/mutator"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/state"
	testdata "github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/test"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

func TestExpiredAgentClosesWatchesAndReconnects(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx, cancel := context.WithTimeout(testutil.NewContext(t, true), 15*time.Second)
	defer cancel()
	conn, mgr, sctx := getTestClientConnAndService(ctx, t, nil)
	defer conn.Close()
	client := rpc.NewManagerClient(conn)
	agent := testdata.GetTestAgents(t)["hello"]
	id := tunnel.SessionID(state.AgentSessionIDPrefix + agent.PodUid)
	_, err := mgr.State().RestoreAgent(sctx, id, agent, nil, time.Now().Add(-2*time.Minute))
	require.NoError(t, err)
	session := &rpc.SessionInfo{SessionId: string(id)}
	deltaWatch, err := client.WatchInterceptsDelta(ctx, session)
	require.NoError(t, err)
	_, err = deltaWatch.Recv()
	require.NoError(t, err)
	fullWatch, err := client.WatchIntercepts(ctx, session)
	require.NoError(t, err)
	_, err = fullWatch.Recv()
	require.NoError(t, err)

	_, err = deltaWatch.Recv()
	require.ErrorIs(t, err, io.EOF)
	_, err = fullWatch.Recv()
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, mgr.State().GetAgent(id))
	require.False(t, mutator.GetMap(sctx).IsInactive(types.UID(agent.PodUid)))

	_, err = client.ReconnectAgent(ctx, &rpc.ReconnectAgentRequest{Session: session, Agent: agent})
	require.NoError(t, err)
	deltaWatch, err = client.WatchInterceptsDelta(ctx, session)
	require.NoError(t, err)
	_, err = deltaWatch.Recv()
	require.NoError(t, err)
	_, err = client.Remain(ctx, &rpc.RemainRequest{Session: session})
	require.NoError(t, err)
}

func TestReconnectAgentRejectsInactivePod(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx := testutil.NewContext(t, true)
	conn, mgr, sctx := getTestClientConnAndService(ctx, t, nil)
	defer conn.Close()
	agent := testdata.GetTestAgents(t)["hello"]
	mutator.GetMap(sctx).Inactivate(types.UID(agent.PodUid))
	_, err := rpc.NewManagerClient(conn).ReconnectAgent(ctx, &rpc.ReconnectAgentRequest{
		Session: &rpc.SessionInfo{SessionId: state.AgentSessionIDPrefix + agent.PodUid},
		Agent:   agent,
	})
	require.Equal(t, codes.Aborted, status.Code(err))
	require.Zero(t, mgr.State().CountAgents())
}
