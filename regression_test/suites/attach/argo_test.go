package attach

import (
	"context"
	"encoding/json/v2"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
)

type argoKubectlCall struct {
	namespace string
	args      []string
}

func TestInstallArgoManifest(t *testing.T) {
	lookup := argoKubectlCall{"", []string{"get", "clusterrolebinding", "argo-rollouts", "--ignore-not-found", "-o", "json"}}
	apply := argoKubectlCall{"rtest-argo", []string{
		"apply", "--server-side", "-f", "https://github.com/argoproj/argo-rollouts/releases/latest/download/install.yaml",
	}}
	patch := func(expected, namespace string) argoKubectlCall {
		return argoKubectlCall{"", []string{
			"patch", "clusterrolebinding", "argo-rollouts", "--type=json", "--field-manager=kubectl", "-p",
			`[{"op":"test","path":"/subjects/0/namespace","value":"` + expected + `"},{"op":"replace","path":"/subjects/0/namespace","value":"` + namespace + `"}]`,
		}}
	}
	account := func(name, namespace string) rbacv1.Subject {
		return rbacv1.Subject{Kind: "ServiceAccount", Name: name, Namespace: namespace}
	}
	conflict := errors.New("conflict with another manager: .subjects")
	cases := []struct {
		name     string
		subjects []rbacv1.Subject
		applyErr error
		want     []argoKubectlCall
	}{
		{
			name: "fresh",
			want: []argoKubectlCall{lookup, apply, patch("argo-rollouts", "rtest-argo")},
		},
		{
			name:     "upstream binding",
			subjects: []rbacv1.Subject{account("argo-rollouts", "argo-rollouts")},
			want:     []argoKubectlCall{lookup, apply, patch("argo-rollouts", "rtest-argo")},
		},
		{
			name:     "fixture owned binding",
			subjects: []rbacv1.Subject{account("argo-rollouts", "rtest-argo")},
			want:     []argoKubectlCall{lookup, patch("rtest-argo", "argo-rollouts"), apply, patch("argo-rollouts", "rtest-argo")},
		},
		{
			name:     "other namespace conflict",
			subjects: []rbacv1.Subject{account("argo-rollouts", "other")},
			applyErr: conflict,
			want:     []argoKubectlCall{lookup, apply},
		},
		{
			name:     "other account conflict",
			subjects: []rbacv1.Subject{account("other", "rtest-argo")},
			applyErr: conflict,
			want:     []argoKubectlCall{lookup, apply},
		},
		{
			name:     "additional subject conflict",
			subjects: []rbacv1.Subject{account("argo-rollouts", "rtest-argo"), account("other", "rtest-argo")},
			applyErr: conflict,
			want:     []argoKubectlCall{lookup, apply},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls []argoKubectlCall
			kubectl := func(_ context.Context, namespace string, args ...string) (string, error) {
				calls = append(calls, argoKubectlCall{namespace, args})
				switch args[0] {
				case "get":
					if tc.subjects == nil {
						return "", nil
					}
					data, err := json.Marshal(rbacv1.ClusterRoleBinding{Subjects: tc.subjects})
					return string(data), err
				case "apply":
					return "", tc.applyErr
				default:
					return "", nil
				}
			}
			err := installArgoManifest(t.Context(), kubectl)
			if tc.applyErr != nil {
				require.ErrorIs(t, err, tc.applyErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, calls)
			for _, call := range calls {
				require.NotContains(t, strings.Join(call.args, " "), "--force-conflicts")
			}
		})
	}
}
