package daemon

import (
	"context"
	"net/netip"
	"os"
	"reflect"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
)

func TestManagerTokenFileIsForwardedOnlyWhenExplicitlyConfigured(t *testing.T) {
	t.Setenv(k8s.ManagerTokenFileEnv, "/temporary/manager-token")
	req := NewDefaultRequest()
	require.Equal(t, "/temporary/manager-token", req.Environment[k8s.ManagerTokenFileEnv])
	t.Setenv(k8s.ManagerTokenFileEnv, "")
	req = NewDefaultRequest()
	value, configured := req.Environment[k8s.ManagerTokenFileEnv]
	require.True(t, configured, "an explicitly empty file is an invalid configuration, not kubeconfig fallback")
	require.Empty(t, value)
	require.NoError(t, os.Unsetenv(k8s.ManagerTokenFileEnv))
	req = NewDefaultRequest()
	require.NotContains(t, req.Environment, k8s.ManagerTokenFileEnv)
	require.NotContains(t, req.Environment, "-"+k8s.ManagerTokenFileEnv, "an implicit command must not clear an existing connection credential")
}

func TestWithDefaultRequestIgnoresLocalNamespace(t *testing.T) {
	cmd := &cobra.Command{Use: "intercept"}
	cmd.SetContext(context.Background())
	cmd.Flags().StringP("namespace", "n", "", "local namespace")
	require.NoError(t, cmd.Flags().Set("namespace", "beta"))

	ctx, err := WithDefaultRequest(cmd)
	require.NoError(t, err)
	req := MustGetRequest(ctx)
	_, ok := req.KubeFlags["namespace"]
	require.False(t, ok, "local attachment namespace must not become an implicit connect namespace")
}

func Test_parseSubnetViaWorkload(t *testing.T) {
	tests := []struct {
		name    string
		dps     string
		want    prefixViaWL
		wantErr bool
	}{
		{
			"empty",
			"",
			prefixViaWL{},
			true,
		},
		{
			"workload with dot",
			"127.1.2.3/32=workload.namespace",
			prefixViaWL{},
			true,
		},
		{
			"invalid subnet",
			"bad=workload",
			prefixViaWL{},
			true,
		},
		{
			"ok",
			"127.1.2.3/32=workload",
			prefixViaWL{
				subnet:   netip.MustParsePrefix("127.1.2.3/32"),
				workload: "workload",
			},
			false,
		},
		{
			"all",
			"all=workload",
			prefixViaWL{
				symbolic: "all",
				workload: "workload",
			},
			false,
		},
		{
			"also",
			"also=workload",
			prefixViaWL{
				symbolic: "also",
				workload: "workload",
			},
			false,
		},
		{
			"pods",
			"pods=workload",
			prefixViaWL{
				symbolic: "pods",
				workload: "workload",
			},
			false,
		},
		{
			"service",
			"service=workload",
			prefixViaWL{
				symbolic: "service",
				workload: "workload",
			},
			false,
		},
		{
			"other",
			"other=workload",
			prefixViaWL{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSubnetViaWorkload(tt.dps)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDomainProxy(%q) error = %v, wantErr %v", tt.dps, err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseDomainProxy(%q) got = %v, want %v", tt.dps, got, tt.want)
			}
		})
	}
}

func Test_parseProxyVias(t *testing.T) {
	tests := []struct {
		name     string
		proxyVia []string
		want     []*daemon.SubnetViaWorkload
		wantErr  bool
	}{
		{
			name:     "single",
			proxyVia: []string{"127.1.2.0/24=workload"},
			want: []*daemon.SubnetViaWorkload{{
				Subnet:   "127.1.2.0/24",
				Workload: "workload",
			}},
			wantErr: false,
		},
		{
			name:     "multi",
			proxyVia: []string{"127.1.2.0/24=workload1", "127.1.3.0/24=workload2"},
			want: []*daemon.SubnetViaWorkload{
				{
					Subnet:   "127.1.2.0/24",
					Workload: "workload1",
				},
				{
					Subnet:   "127.1.3.0/24",
					Workload: "workload2",
				},
			},
			wantErr: false,
		},
		{
			name:     "multi-overlap",
			proxyVia: []string{"127.1.2.0/16=workload1", "127.1.3.0/16=workload2"},
			want:     nil,
			wantErr:  true,
		},
		{
			name:     "symbolic-overlap",
			proxyVia: []string{"also=workload1", "also=workload2"},
			want:     nil,
			wantErr:  true,
		},
		{
			name:     "symbolic-overlap-all",
			proxyVia: []string{"also=workload1", "all=workload2"},
			want:     nil,
			wantErr:  true,
		},
		{
			name:     "multi-mixed",
			proxyVia: []string{"127.1.2.0/16=workload1", "also=workload2"},
			want: []*daemon.SubnetViaWorkload{
				{
					Subnet:   "127.1.2.0/16",
					Workload: "workload1",
				},
				{
					Subnet:   "also",
					Workload: "workload2",
				},
			},
			wantErr: false,
		},
		{
			name:     "multi-mixed-all",
			proxyVia: []string{"127.1.2.0/16=workload1", "all=workload2"},
			want: []*daemon.SubnetViaWorkload{
				{
					Subnet:   "127.1.2.0/16",
					Workload: "workload1",
				},
				{
					Subnet:   "also",
					Workload: "workload2",
				},
				{
					Subnet:   "pods",
					Workload: "workload2",
				},
				{
					Subnet:   "service",
					Workload: "workload2",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseProxyVias(tt.proxyVia)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseProxyVias() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseProxyVias() got = %v, want %v", got, tt.want)
			}
		})
	}
}
