package intercept

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilteredStreamLocalConnectionControls(t *testing.T) {
	for _, protocol := range []int{1, 2} {
		t.Run(fmt.Sprintf("HTTP%d", protocol), func(t *testing.T) {
			ctx := t.Context()
			target := newFilteredStreamTarget(t, protocol, "/held")
			directClient := newFilteredStreamClient(t, protocol)
			direct := requireFilteredStreamLocal(t, ctx, directClient, target.url(), "/direct", target, protocol)
			directClient.CloseIdleConnections()
			client := newFilteredStreamClient(t, protocol)
			first := requireFilteredStreamLocal(t, ctx, client, target.url(), "/first", target, protocol)
			second := requireFilteredStreamLocal(t, ctx, client, target.url(), "/second", target, protocol)
			require.NotEqual(t, direct.connection, first.connection)
			require.Equal(t, first.connection, second.connection)
			require.Equal(t, filteredStreamObservation{Marker: target.marker, Path: "/first", Protocol: protocol, Connection: first.connection}, target.observations("/first")[0])

			holdCtx, cancelHold := context.WithCancel(ctx)
			defer cancelHold()
			heldDone := make(chan filteredStreamResponse, 1)
			go func() { heldDone <- doFilteredStreamRequest(holdCtx, client, target.url()+target.holdPath, headerVal) }()
			held := waitForFilteredStreamHold(t, target, heldDone)
			require.Equal(t, first.connection, held.Connection)
			if protocol == 2 {
				parallel := requireFilteredStreamLocal(t, ctx, client, target.url(), "/parallel", target, protocol)
				require.Equal(t, first.connection, parallel.connection)
			}
			cancelHold()
			require.ErrorIs(t, waitForFilteredStreamRequest(t, heldDone).err, context.Canceled)
			require.Equal(t, held, waitForFilteredStreamEvent(t, target.canceled))
			afterFirst := requireFilteredStreamLocal(t, ctx, client, target.url(), "/after-first", target, protocol)
			afterSecond := requireFilteredStreamLocal(t, ctx, client, target.url(), "/after-second", target, protocol)
			require.Equal(t, afterFirst.connection, afterSecond.connection)
			if protocol == 2 {
				require.Equal(t, first.connection, afterFirst.connection)
			}
			require.Empty(t, target.observations("/unseen"))
		})
	}
}

func TestFilteredStreamClusterControlRequiresPhysicalEchoAndNoTarget(t *testing.T) {
	marker := "rtest-selected-test"
	response := filteredStreamResponse{status: http.StatusOK, protocol: 2, body: "HTTP/2.0 GET /wrong\n\nHost: echo\nRequest served by echo-pod\n"}
	require.True(t, response.isCluster("/wrong", marker, 2))
	require.False(t, response.isCluster("/missing", marker, 2))
	require.False(t, response.isCluster("/wrong", marker, 1))
	withoutRemote := response
	withoutRemote.body = "HTTP/2.0 GET /wrong\n"
	require.False(t, withoutRemote.isCluster("/wrong", marker, 2))
	withTarget := response
	withTarget.marker = marker
	require.False(t, withTarget.isCluster("/wrong", marker, 2))
	withTarget = response
	withTarget.connection = 1
	require.False(t, withTarget.isCluster("/wrong", marker, 2))
	withTarget = response
	withTarget.body += marker
	require.False(t, withTarget.isCluster("/wrong", marker, 2))
}
