package cmd

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/telepresenceio/telepresence/rpc/v2/connector"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/env"
	"github.com/telepresenceio/telepresence/v2/pkg/client/cli/output"
)

func TestListEnvironmentOutput(t *testing.T) {
	values := map[string]string{"ORDINARY": "ordinary-sentinel", "API_TOKEN": "credential-sentinel", "MULTILINE": "first-sentinel\nsecond-sentinel"}
	workload := &connector.WorkloadInfo{
		Name: "example", WorkloadResourceType: "Deployment",
		InterceptInfo: []*manager.InterceptInfo{{Spec: &manager.InterceptSpec{Name: "example"}, Environment: values}},
		IngestInfo:    []*connector.IngestInfo{{Workload: "example", Environment: values}},
	}
	original := proto.Clone(workload)
	for _, flag := range []string{"format", "output"} {
		for _, format := range []string{"default", "json", "yaml", "json-stream"} {
			for _, show := range []bool{false, true} {
				for _, debug := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/show=%t/debug=%t", flag, format, show, debug), func(t *testing.T) {
						var stdout, stderr strings.Builder
						cmd := list()
						cmd.SetContext(client.WithConfig(context.Background(), client.GetDefaultConfig()))
						cmd.SetOut(&stdout)
						cmd.SetErr(&stderr)
						cmd.Flags().String(flag, "default", "")
						cmd.PreRunE = output.SetFormat
						cmd.RunE = func(cmd *cobra.Command, _ []string) error {
							showFlag, err := cmd.Flags().GetBool("show-env")
							require.NoError(t, err)
							debugFlag, err := cmd.Flags().GetBool("debug")
							require.NoError(t, err)
							state := listCommand{showEnv: showFlag, debug: debugFlag}
							state.printList(cmd.Context(), []*connector.WorkloadInfo{workload, workload}, cmd.OutOrStdout(), output.WantsFormatted(cmd))
							if output.WantsStream(cmd) {
								state.printList(cmd.Context(), []*connector.WorkloadInfo{workload}, cmd.OutOrStdout(), true)
							}
							return nil
						}
						cmd.SetArgs([]string{"--" + flag + "=" + format, fmt.Sprintf("--show-env=%t", show), fmt.Sprintf("--debug=%t", debug)})
						_, _, err := output.Execute(cmd)
						require.NoError(t, err)
						combined := stdout.String() + stderr.String()
						for _, sentinel := range []string{"ordinary-sentinel", "credential-sentinel", "first-sentinel", "second-sentinel"} {
							if show && format != "default" {
								require.Contains(t, stdout.String(), sentinel)
							} else {
								require.NotContains(t, combined, sentinel)
							}
						}
						if format != "default" {
							require.Contains(t, stdout.String(), "ORDINARY")
							require.Contains(t, stdout.String(), "intercept_info")
							require.Contains(t, stdout.String(), "ingest_info")
							if !show {
								require.Contains(t, stdout.String(), env.Redacted)
							}
						}
						require.True(t, proto.Equal(original, workload), "presentation changed RPC data")
					})
				}
			}
		}
	}
}

func TestWorkloadPresentationEmpty(t *testing.T) {
	require.Nil(t, workloadPresentation(nil, false))
	require.Empty(t, workloadPresentation([]*connector.WorkloadInfo{}, false))
	for _, show := range []bool{false, true} {
		workloads := []*connector.WorkloadInfo{nil, {InterceptInfo: []*manager.InterceptInfo{nil, {}}, IngestInfo: []*connector.IngestInfo{nil, {}}}}
		result := workloadPresentation(workloads, show)
		require.Len(t, result, 2)
		require.Nil(t, result[0])
		require.Empty(t, result[1].InterceptInfo[1].Environment)
		require.Empty(t, result[1].IngestInfo[1].Environment)
	}
}

func TestEnvironmentOptInFlags(t *testing.T) {
	for _, cmd := range []*cobra.Command{list(), ingestCmd(), interceptCmd(), replaceCmd(), wiretapCmd()} {
		t.Run(cmd.Name(), func(t *testing.T) {
			flag := cmd.Flags().Lookup("show-env")
			require.NotNil(t, flag)
			require.Equal(t, "false", flag.DefValue)
			require.Contains(t, flag.Usage, "sensitive")
			require.NoError(t, cmd.ParseFlags([]string{"--show-env"}))
			value, err := cmd.Flags().GetBool("show-env")
			require.NoError(t, err)
			require.True(t, value)
		})
	}
}
