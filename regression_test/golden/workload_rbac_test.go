package golden

import (
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	sigyaml "sigs.k8s.io/yaml"
)

func workloadRoleRules(t *testing.T, output map[string]string, template, kind, name, namespace string) []rbacv1.PolicyRule {
	t.Helper()
	decoder := yaml.NewYAMLOrJSONDecoder(strings.NewReader(output[template]), 4096)
	for {
		var role struct {
			metav1.TypeMeta `json:",inline"`
			Metadata        metav1.ObjectMeta   `json:"metadata"`
			Rules           []rbacv1.PolicyRule `json:"rules"`
		}
		if err := decoder.Decode(&role); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode %s: %v", template, err)
		}
		if role.Kind == kind && role.Metadata.Name == name && role.Metadata.Namespace == namespace {
			return role.Rules
		}
	}
	t.Fatalf("%s does not contain %s %s/%s", template, kind, namespace, name)
	return nil
}

func replicaSetWorkloadEnabled(t *testing.T, output map[string]string) bool {
	t.Helper()
	var statefulSet appsv1.StatefulSet
	if err := sigyaml.Unmarshal([]byte(output[statefulsetTpl]), &statefulSet); err != nil {
		t.Fatalf("decode manager StatefulSet: %v", err)
	}
	for _, container := range statefulSet.Spec.Template.Spec.Containers {
		for _, env := range container.Env {
			if env.Name == "ENABLED_WORKLOAD_KINDS" {
				return slices.Contains(strings.Fields(env.Value), "ReplicaSet")
			}
		}
	}
	t.Fatal("manager StatefulSet did not contain ENABLED_WORKLOAD_KINDS")
	return false
}

func checkReplicaSetRoleRules(t *testing.T, output map[string]string, template, kind, roleName, namespace string, wantVerbs []string) {
	t.Helper()
	var found []rbacv1.PolicyRule
	for _, rule := range workloadRoleRules(t, output, template, kind, roleName, namespace) {
		if slices.Contains(rule.APIGroups, "apps") && slices.Contains(rule.Resources, "replicasets") {
			found = append(found, rule)
		}
	}
	if wantVerbs == nil {
		if len(found) != 0 {
			t.Errorf("%s/%s ReplicaSet rules = %v, want none", namespace, roleName, found)
		}
		return
	}
	if len(found) != 1 {
		t.Errorf("%s/%s ReplicaSet rules = %v, want exactly one with verbs %v", namespace, roleName, found, wantVerbs)
		return
	}
	want := rbacv1.PolicyRule{APIGroups: []string{"apps"}, Resources: []string{"replicasets"}, Verbs: wantVerbs}
	if !reflect.DeepEqual(found[0], want) {
		t.Errorf("%s/%s ReplicaSet rule = %+v, want %+v", namespace, roleName, found[0], want)
	}
}

func TestManagerReplicaSetOwnerRBAC(t *testing.T) {
	const (
		clusterTemplate   = "telepresence-oss/templates/trafficManagerRbac/cluster-scope.yaml"
		namespaceTemplate = "telepresence-oss/templates/trafficManagerRbac/namespace-scope.yaml"
		workloadNamespace = "rtest-app"
	)
	cases := []struct {
		name            string
		workloads       map[string]any
		disableInjector bool
		wantVerbs       []string
		wantReplicaSet  bool
	}{
		{name: "defaults", wantVerbs: []string{"get", "list", "watch", "patch"}, wantReplicaSet: true},
		{name: "deployments_only", workloads: map[string]any{"replicaSets": false, "argoRollouts": false}, wantVerbs: []string{"get"}},
		{name: "deployments_only_without_injector", workloads: map[string]any{"replicaSets": false, "argoRollouts": false}, disableInjector: true, wantVerbs: []string{"get"}},
		{name: "rollouts_only", workloads: map[string]any{"deployments": false, "replicaSets": false, "argoRollouts": true}, wantVerbs: []string{"get"}},
		{name: "both_owners", workloads: map[string]any{"deployments": true, "replicaSets": false, "argoRollouts": true}, wantVerbs: []string{"get"}},
		{name: "both_owners_disabled", workloads: map[string]any{"deployments": false, "replicaSets": false, "argoRollouts": false}},
		{name: "replica_sets_enabled", workloads: map[string]any{"deployments": true, "replicaSets": true, "argoRollouts": true}, wantVerbs: []string{"get", "list", "watch", "patch"}, wantReplicaSet: true},
		{name: "replica_sets_enabled_without_injector", workloads: map[string]any{"deployments": true, "replicaSets": true, "argoRollouts": true}, disableInjector: true, wantVerbs: []string{"get", "list", "watch"}, wantReplicaSet: true},
	}
	scopes := []struct {
		name       string
		template   string
		kind       string
		roleName   string
		namespaces []string
	}{
		{"cluster", clusterTemplate, "ClusterRole", "traffic-manager-" + releaseNamespace, []string{""}},
		{"namespace", namespaceTemplate, "Role", "traffic-manager", []string{workloadNamespace, releaseNamespace}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, scope := range scopes {
				t.Run(scope.name, func(t *testing.T) {
					values := map[string]any{"managerRbac": map[string]any{"create": true}}
					if tc.workloads != nil {
						workloads := make(map[string]any, len(tc.workloads))
						for key, enabled := range tc.workloads {
							workloads[key] = map[string]any{"enabled": enabled}
						}
						values["workloads"] = workloads
					}
					if tc.disableInjector {
						values["agentInjector"] = map[string]any{"enabled": false}
					}
					if scope.name == "namespace" {
						values["namespaces"] = []string{workloadNamespace}
					}
					output := renderChart(t, values)
					if enabled := replicaSetWorkloadEnabled(t, output); enabled != tc.wantReplicaSet {
						t.Errorf("ReplicaSet workload enabled = %v, want %v", enabled, tc.wantReplicaSet)
					}
					for _, namespace := range scope.namespaces {
						checkReplicaSetRoleRules(t, output, scope.template, scope.kind, scope.roleName, namespace, tc.wantVerbs)
					}
				})
			}
		})
	}
}
