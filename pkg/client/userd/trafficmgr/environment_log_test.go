package trafficmgr

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
)

func TestInvalidAPIPortDoesNotLogEnvironmentValue(t *testing.T) {
	var logs strings.Builder
	ctx := clog.WithLogger(context.Background(), slog.New(slog.NewTextHandler(&logs, nil)))
	s := &session{Cluster: &k8s.Cluster{Kubeconfig: &k8s.Kubeconfig{Context: ctx}}, currentIntercepts: map[string]*intercept{
		"example": {InterceptInfo: &manager.InterceptInfo{
			Spec:        &manager.InterceptSpec{Agent: "example", Namespace: "default"},
			Disposition: manager.InterceptDispositionType_ACTIVE,
			Environment: map[string]string{agentconfig.EnvAPIPort: "first-sentinel\nsecond-sentinel"},
		}},
	}}
	s.reconcileAPIServers()
	require.Contains(t, logs.String(), agentconfig.EnvAPIPort)
	require.NotContains(t, logs.String(), "sentinel")
}
