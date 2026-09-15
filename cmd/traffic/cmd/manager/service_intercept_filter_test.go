package manager

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/state"
	"github.com/telepresenceio/telepresence/v2/pkg/cache"
)

func TestFilterInterceptDeltasRemovesInterceptThatStopsMatching(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	source := make(chan cache.Delta[string, *state.Intercept], 2)
	filtered := filterInterceptDeltas(ctx, source, func(_ string, intercept *state.Intercept) bool {
		return intercept.Spec.Agent == "selected"
	})
	selected := &state.Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id:   "client:intercept",
		Spec: &rpc.InterceptSpec{Agent: "selected"},
	}}
	source <- cache.Delta[string, *state.Intercept]{
		Upserts: map[string]*state.Intercept{selected.Id: selected},
	}
	first := <-filtered
	require.Contains(t, first.Upserts, selected.Id)
	require.Empty(t, first.Removals)

	dropped := &state.Intercept{InterceptInfo: &rpc.InterceptInfo{
		Id:   selected.Id,
		Spec: &rpc.InterceptSpec{Agent: "dropped"},
	}}
	source <- cache.Delta[string, *state.Intercept]{
		Upserts: map[string]*state.Intercept{dropped.Id: dropped},
	}
	second := <-filtered
	require.Empty(t, second.Upserts)
	require.Contains(t, second.Removals, selected.Id)
	require.Same(t, selected, second.Removals[selected.Id])
}

func TestWatcherInterceptInfoPreservesClientsAndProjectsAgents(t *testing.T) {
	info := &rpc.InterceptInfo{
		Spec:              &rpc.InterceptSpec{Agent: "selected"},
		Id:                "client:intercept",
		ClientSession:     &rpc.SessionInfo{SessionId: "client"},
		Disposition:       rpc.InterceptDispositionType_ACTIVE,
		Message:           "active",
		PodName:           "selected-pod",
		ApiPort:           9900,
		PodIp:             "10.0.0.1",
		SftpPort:          9901,
		FtpPort:           9902,
		ClientMountPoint:  "/client",
		MountPoint:        "/agent",
		MechanismArgsDesc: "header match",
		Environment:       map[string]string{"SECRET": "private"},
		Mounts:            map[string]int32{"/agent": 1},
		ModifiedAt:        timestamppb.Now(),
		ServiceWorkloads: []*rpc.InterceptWorkload{{
			WorkloadName: "selected",
		}},
	}

	client := watcherInterceptInfo(info, false)
	require.Same(t, info, client)
	require.Equal(t, "private", client.Environment["SECRET"])
	require.Equal(t, "/agent", client.MountPoint)
	require.Contains(t, client.Mounts, "/agent")

	agent := watcherInterceptInfo(info, true)
	require.NotSame(t, info, agent)
	require.Nil(t, agent.Environment)
	require.Nil(t, agent.Mounts)
	require.Empty(t, agent.MountPoint)

	want := proto.Clone(info).(*rpc.InterceptInfo)
	want.Environment = nil
	want.Mounts = nil
	want.MountPoint = ""
	require.True(t, proto.Equal(want, agent))
	require.Equal(t, "private", info.Environment["SECRET"])
	require.Contains(t, info.Mounts, "/agent")
	require.Equal(t, "/agent", info.MountPoint)
}

func TestAgentInterceptProjectionKeepsLargeSnapshotBelowGRPCLimit(t *testing.T) {
	const count = 1_024
	const grpcDefaultMaxReceive = 4 << 20
	environmentValue := strings.Repeat("x", 8<<10)
	full := &rpc.InterceptInfoDelta{Upserts: make(map[string]*rpc.InterceptInfo, count)}
	projected := &rpc.InterceptInfoDelta{Upserts: make(map[string]*rpc.InterceptInfo, count)}

	for i := range count {
		id := fmt.Sprintf("client-%d:intercept", i)
		info := &rpc.InterceptInfo{
			Id:          id,
			Spec:        &rpc.InterceptSpec{Agent: "selected"},
			Disposition: rpc.InterceptDispositionType_ACTIVE,
			Environment: map[string]string{"APPLICATION_ENV": environmentValue},
			Mounts:      map[string]int32{"/agent/mount": 1},
			MountPoint:  "/agent/mount",
		}
		full.Upserts[id] = watcherInterceptInfo(info, false)
		projected.Upserts[id] = watcherInterceptInfo(info, true)
	}

	require.Greater(t, proto.Size(full), grpcDefaultMaxReceive)
	require.Less(t, proto.Size(projected), grpcDefaultMaxReceive)
}
