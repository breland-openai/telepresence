package mutator

import (
	"context"
	"encoding/json/v2"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func TestInjectionTraceDoesNotExposeRemoteEnvironment(t *testing.T) {
	var logs strings.Builder
	ctx := clog.WithLogger(context.Background(), slog.New(slog.NewTextHandler(&logs,
		&slog.HandlerOptions{Level: clog.LevelTrace})))
	ctx = managerutil.WithEnv(ctx, &managerutil.Env{AgentInitContainerEnabled: true})
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: "example-pod", Namespace: "default"},
		Spec: core.PodSpec{Containers: []core.Container{{
			Name: "example",
			Env: []core.EnvVar{
				{Name: "ORDINARY", Value: "first-sentinel\nsecond-sentinel"},
				{Name: "PASSWORD", Value: "credential-sentinel"},
			},
		}}},
	}
	tpl := &core.PodTemplateSpec{ObjectMeta: pod.ObjectMeta, Spec: *pod.Spec.DeepCopy()}
	cfg := &agentconfig.Sidecar{
		AgentName: "example", WorkloadName: "example", Namespace: "default",
		WorkloadKind: k8sapi.DeploymentKind, AgentImage: "example.test/traffic:latest",
		Containers: []*agentconfig.Container{{
			Name: "example", EnvPrefix: "A_", Replace: agentconfig.ReplacePolicyContainer,
		}},
	}
	patches, err := createPatch(ctx, cfg, pod, tpl)
	require.NoError(t, err)
	payload, err := json.Marshal(patches)
	require.NoError(t, err)
	require.Contains(t, string(payload), annotation.ReplaceAnnotationKey("example"))
	require.Contains(t, logs.String(), "Patch add /spec/containers/-")
	require.Contains(t, logs.String(), "example-pod")
	for _, sentinel := range []string{"first-sentinel", "second-sentinel", "credential-sentinel"} {
		require.Contains(t, string(payload), sentinel, "admission must still get the actual environment")
		require.NotContains(t, logs.String(), sentinel)
	}
}

func TestContainerComparisonDoesNotExposeRemoteEnvironment(t *testing.T) {
	for _, level := range []slog.Level{slog.LevelDebug, clog.LevelTrace} {
		for _, change := range []string{"unchanged", "changed", "added", "kubernetes-defaults"} {
			t.Run(level.String()+"/"+change, func(t *testing.T) {
				var logs strings.Builder
				ctx := clog.WithLogger(context.Background(), slog.New(slog.NewTextHandler(&logs,
					&slog.HandlerOptions{Level: level})))
				a := &core.Container{Name: "traffic-agent", Env: []core.EnvVar{{Name: "ORDINARY", Value: "original-synthetic-value"}}}
				b := a.DeepCopy()
				switch change {
				case "changed":
					b.Env[0].Value = "replacement-synthetic-value"
				case "added":
					b.Env = append(b.Env, core.EnvVar{Name: "ADDED", Value: "added-synthetic-value"})
				case "kubernetes-defaults":
					b.ImagePullPolicy = core.PullIfNotPresent
					b.TerminationMessagePath = "/dev/termination-log"
					b.TerminationMessagePolicy = core.TerminationMessageReadFile
				}
				beforeA, beforeB := a.DeepCopy(), b.DeepCopy()
				require.Equal(t, change == "unchanged" || change == "kubernetes-defaults", containerEqual(ctx, a, b))
				require.Equal(t, beforeA, a)
				require.Equal(t, beforeB, b)
				for _, value := range []string{"original-synthetic-value", "replacement-synthetic-value", "added-synthetic-value"} {
					require.NotContains(t, logs.String(), value)
				}
			})
		}
	}
}
