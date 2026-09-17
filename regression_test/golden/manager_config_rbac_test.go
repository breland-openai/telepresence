package golden

import (
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
)

func permitsConfigMap(rules []rbacv1.PolicyRule, verb, name string) bool {
	for _, rule := range rules {
		if (slices.Contains(rule.APIGroups, "") || slices.Contains(rule.APIGroups, "*")) &&
			(slices.Contains(rule.Resources, "configmaps") || slices.Contains(rule.Resources, "*")) &&
			(slices.Contains(rule.Verbs, verb) || slices.Contains(rule.Verbs, "*")) &&
			(len(rule.ResourceNames) == 0 || slices.Contains(rule.ResourceNames, name)) {
			return true
		}
	}
	return false
}

func TestManagerConfigMapWriteRBAC(t *testing.T) {
	const (
		clusterTemplate   = "telepresence-oss/templates/trafficManagerRbac/cluster-scope.yaml"
		namespaceTemplate = "telepresence-oss/templates/trafficManagerRbac/namespace-scope.yaml"
		appNamespace      = "rtest-app"
		otherNamespace    = "rtest-other"
	)
	cases := []struct {
		name              string
		managedNamespaces []string
		disableInjector   bool
	}{
		{name: "cluster_scoped"},
		{name: "namespace_scoped", managedNamespaces: []string{appNamespace, otherNamespace}},
		{name: "manager_explicitly_scoped", managedNamespaces: []string{appNamespace, releaseNamespace}},
		{name: "namespace_scoped_without_injector", managedNamespaces: []string{appNamespace}, disableInjector: true},
	}
	configMaps := []string{"traffic-manager", "traffic-manager-install", "unrelated"}
	verbs := []string{"get", "patch", "update", "delete"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := map[string]any{"managerRbac": map[string]any{"create": true}}
			if tc.managedNamespaces != nil {
				values["namespaces"] = tc.managedNamespaces
			}
			if tc.disableInjector {
				values["agentInjector"] = map[string]any{"enabled": false}
			}
			output := renderChart(t, values)
			managerRules := workloadRoleRules(t, output, namespaceTemplate, "Role", "traffic-manager", releaseNamespace)
			for _, configMap := range configMaps {
				for _, verb := range verbs {
					want := configMap != "unrelated" && verb != "delete"
					if got := permitsConfigMap(managerRules, verb, configMap); got != want {
						t.Errorf("manager namespace grants %s on ConfigMap %s: got %t, want %t", verb, configMap, got, want)
					}
				}
			}

			for _, namespace := range tc.managedNamespaces {
				if namespace == releaseNamespace {
					continue
				}
				rules := workloadRoleRules(t, output, namespaceTemplate, "Role", "traffic-manager", namespace)
				for _, configMap := range configMaps {
					for _, verb := range verbs {
						want := verb == "get"
						if got := permitsConfigMap(rules, verb, configMap); got != want {
							t.Errorf("workload namespace %s grants %s on ConfigMap %s: got %t, want %t", namespace, verb, configMap, got, want)
						}
					}
				}
			}

			template := clusterTemplate
			clusterRole := "traffic-manager-" + releaseNamespace
			if tc.managedNamespaces != nil {
				template = namespaceTemplate
				clusterRole = "traffic-manager-cluster-wide-" + releaseNamespace
			}
			clusterRules := workloadRoleRules(t, output, template, "ClusterRole", clusterRole, "")
			for _, configMap := range configMaps {
				for _, verb := range verbs {
					if permitsConfigMap(clusterRules, verb, configMap) {
						t.Errorf("ClusterRole grants %s on ConfigMap %s outside the manager's Role", verb, configMap)
					}
				}
			}
		})
	}
}
