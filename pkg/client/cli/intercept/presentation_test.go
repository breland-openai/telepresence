package intercept

import (
	"context"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/env"
	"github.com/telepresenceio/telepresence/v2/pkg/json"
	"github.com/telepresenceio/telepresence/v2/pkg/proc"
)

func TestInfoPresentation(t *testing.T) {
	values := map[string]string{"ORDINARY": "ordinary-sentinel", "TOKEN": "credential-sentinel", "MULTILINE": "first-sentinel\nsecond-sentinel"}
	original := maps.Clone(values)
	info := &Info{Environment: values}
	for _, show := range []bool{false, true} {
		presentation := info.Presentation(show)
		data, err := json.Marshal(presentation)
		require.NoError(t, err)
		for _, sentinel := range []string{"ordinary-sentinel", "credential-sentinel", "first-sentinel", "second-sentinel"} {
			if show {
				require.Contains(t, string(data), sentinel)
			} else {
				require.NotContains(t, string(data), sentinel)
			}
			require.NotContains(t, info.String(), sentinel)
		}
		if !show {
			require.Contains(t, string(data), env.Redacted)
		}
		presentation.Environment["ORDINARY"] = "changed"
		require.Equal(t, original, info.Environment)
		child := proc.CommandStd(context.Background(), info.Environment, "example")
		for key, value := range original {
			require.Contains(t, child.Env, key+"="+value)
		}
	}
	require.Nil(t, (*Info)(nil).Presentation(false))
	require.Nil(t, (&Info{}).Presentation(false).Environment)
	require.Empty(t, (&Info{Environment: map[string]string{}}).Presentation(false).Environment)
}
