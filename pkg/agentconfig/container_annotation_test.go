package agentconfig

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
)

func TestAgentContainerConfigAnnotationIsDecodableByInit(t *testing.T) {
	builder := scBuilder(nil, nil, nil)
	builder.Config.WorkloadName = "api"
	builder.Config.Namespace = "staging"
	builder.Config.ClientConnectionTTL = 24 * time.Hour
	builder.Config.WatchRetryInterval = 3 * time.Second

	agent, annotations, err := builder.AgentContainer(context.Background())
	require.NoError(t, err)
	require.NotNil(t, agent)
	raw := annotations[annotation.Config]
	require.NotEmpty(t, raw)
	require.Contains(t, raw, `"clientConnectionTTL":"24h0m0s"`)
	require.Contains(t, raw, `"watchRetryInterval":"3s"`)

	init := InitContainer(builder.Config, agent.SecurityContext, "")
	for _, env := range init.Env {
		if env.Name != EnvAgentConfig {
			continue
		}
		require.NotNil(t, env.ValueFrom)
		require.NotNil(t, env.ValueFrom.FieldRef)
		require.Equal(t, "v1", env.ValueFrom.FieldRef.APIVersion)
		require.Equal(t, fmt.Sprintf("metadata.annotations['%s']", annotation.Config), env.ValueFrom.FieldRef.FieldPath)

		// agent-init's loadConfig passes this exact downward-API value to UnmarshalJSON.
		cfg, err := UnmarshalJSON(raw)
		require.NoError(t, err)
		require.Equal(t, builder.Config.WorkloadName, cfg.WorkloadName)
		require.Equal(t, builder.Config.Namespace, cfg.Namespace)
		require.Equal(t, builder.Config.ClientConnectionTTL, cfg.ClientConnectionTTL)
		require.Equal(t, builder.Config.WatchRetryInterval, cfg.WatchRetryInterval)
		require.Len(t, cfg.Containers, 1)
		require.Len(t, cfg.Containers[0].Intercepts, 1)
		require.Equal(t, uint16(8080), cfg.Containers[0].Intercepts[0].ContainerPort)
		return
	}
	t.Fatalf("init container has no %s environment variable", EnvAgentConfig)
}

func TestAgentContainerRejectsUnencodableConfig(t *testing.T) {
	builder := scBuilder(nil, nil, nil)
	builder.Config.WorkloadName = "api"
	builder.Config.Namespace = "staging"
	builder.Config.AgentName = string([]byte{0xff})

	agent, annotations, err := builder.AgentContainer(context.Background())
	require.ErrorContains(t, err, "unable to marshal agent config for api.staging")
	require.ErrorContains(t, errors.Unwrap(err), "invalid UTF-8")
	require.Nil(t, agent)
	require.Nil(t, annotations)
}
