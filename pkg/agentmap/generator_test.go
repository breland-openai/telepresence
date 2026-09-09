package agentmap

import (
	"context"
	"strings"
	"testing"

	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func TestGenerateDoesNotMutateSharedWorkloadTemplate(t *testing.T) {
	workload := deploymentWithInactivePort("")
	workload.GetPodTemplate().Spec.Containers = []core.Container{{
		Name:  "app",
		Ports: []core.ContainerPort{{ContainerPort: 9900}},
	}}
	config := &GeneratorConfig{AgentPort: 9900}
	start := make(chan struct{})
	errs := make(chan error, 16)

	for range cap(errs) {
		go func() {
			<-start
			_, err := config.Generate(context.Background(), workload, nil)
			errs <- err
		}()
	}
	close(start)
	for range cap(errs) {
		if err := <-errs; err == nil || !strings.Contains(err.Error(), "same port") {
			t.Fatalf("Generate() error = %v, want the conflicting agent port", err)
		}
	}
	if namespace := workload.GetPodTemplate().Namespace; namespace != "" {
		t.Fatalf("Generate() mutated the shared pod template namespace to %q", namespace)
	}
}

func TestGenerateAuthoritativeRouteReadinessIsOptIn(t *testing.T) {
	ctx := k8sapi.WithK8sInterface(context.Background(), fake.NewClientset())
	for _, test := range []struct {
		enabled    bool
		namespaces []string
		want       bool
	}{
		{enabled: false},
		{enabled: true, want: true},
		{enabled: true, namespaces: []string{"ambassador"}},
		{enabled: true, namespaces: []string{"ambassador", "default"}, want: true},
		{namespaces: []string{"default"}},
	} {
		workload := deploymentWithInactivePort("")
		workload.GetPodTemplate().Labels = map[string]string{"app": "app"}
		workload.GetPodTemplate().Spec.Containers = []core.Container{{Name: "app"}}
		config := &GeneratorConfig{AgentPort: 9900, RequireAuthoritativeRoutes: test.enabled, AuthoritativeRouteNamespaces: test.namespaces}
		sidecar, err := config.Generate(ctx, workload, nil)
		if err != nil {
			t.Fatal(err)
		}
		if sidecar.RequireAuthoritativeRoutes != test.want {
			t.Fatalf("RequireAuthoritativeRoutes = %t, want %t", sidecar.RequireAuthoritativeRoutes, test.want)
		}
		wire, err := agentconfig.MarshalTight(sidecar)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(wire, `"requireAuthoritativeRoutes"`) != test.want {
			t.Fatalf("new field must only be sent when explicitly enabled: %s", wire)
		}
		decoded, err := agentconfig.UnmarshalJSON(wire)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.RequireAuthoritativeRoutes != test.want {
			t.Fatalf("decoded RequireAuthoritativeRoutes = %t, want %t", decoded.RequireAuthoritativeRoutes, test.want)
		}
	}
}

func TestGenerateRouteIntentAgentImageIsNamespaceScoped(t *testing.T) {
	const standard, candidate = "registry.example/tel2:stable", "registry.example/tel2@sha256:abc123"
	ctx := k8sapi.WithK8sInterface(context.Background(), fake.NewClientset())
	for _, test := range []struct {
		name, namespace, candidate, want string
		scoped                           []string
	}{
		{name: "allowed", namespace: "ambassador", candidate: candidate, scoped: []string{"ambassador"}, want: candidate},
		{name: "not allowed", namespace: "application", candidate: candidate, scoped: []string{"ambassador"}, want: standard},
		{name: "default image unchanged", namespace: "ambassador", scoped: []string{"ambassador"}, want: standard},
		{name: "empty namespace list retains feature all", namespace: "application", candidate: candidate, want: candidate},
	} {
		t.Run(test.name, func(t *testing.T) {
			workload := deploymentWithInactivePort("")
			workload.SetNamespace(test.namespace)
			workload.GetPodTemplate().Labels = map[string]string{"app": "app"}
			workload.GetPodTemplate().Spec.Containers = []core.Container{{Name: "app"}}
			cfg := &GeneratorConfig{AgentPort: 9900, QualifiedAgentImage: standard, RouteIntentAgentImage: test.candidate, AuthoritativeRouteNamespaces: test.scoped}
			if got := cfg.AgentImageForNamespace(test.namespace); got != test.want {
				t.Fatalf("selected image %q, expected %q", got, test.want)
			}
			previous := &agentconfig.Sidecar{AgentImage: standard}
			generated, err := cfg.Generate(ctx, workload, previous)
			if err != nil {
				t.Fatal(err)
			}
			if generated.AgentImage != test.want {
				t.Fatalf("generated image %q, expected %q", generated.AgentImage, test.want)
			}
			if previous.AgentImage != standard {
				t.Fatal("generation mutated cached prior-image config")
			}
		})
	}
}

func TestApplyInactivePortAnnotation(t *testing.T) {
	workload := deploymentWithInactivePort("8399")
	intercepts := []*agentconfig.Intercept{
		{ServiceName: "app", ContainerPort: 8000, AgentPort: 9900, Protocol: types.ProtoTCP},
		{ServiceName: "app-websocket", ContainerPort: 8000, AgentPort: 9900, Protocol: types.ProtoTCP},
	}
	containers := []*agentconfig.Container{{Name: "app", Intercepts: intercepts}}

	if err := applyInactivePortAnnotation(workload, containers); err != nil {
		t.Fatal(err)
	}
	for _, intercept := range intercepts {
		if intercept.InactivePort != 8399 {
			t.Fatalf("InactivePort = %d, want 8399", intercept.InactivePort)
		}
	}
}

func TestApplyInactivePortAnnotationRejectsMultipleTargets(t *testing.T) {
	workload := deploymentWithInactivePort("8399")
	containers := []*agentconfig.Container{{
		Name: "app",
		Intercepts: []*agentconfig.Intercept{
			{ContainerPort: 8000, AgentPort: 9900, Protocol: types.ProtoTCP},
			{ContainerPort: 9000, AgentPort: 9901, Protocol: types.ProtoTCP},
		},
	}}

	err := applyInactivePortAnnotation(workload, containers)
	if err == nil || !strings.Contains(err.Error(), "requires exactly one intercepted container port") {
		t.Fatalf("applyInactivePortAnnotation() error = %v", err)
	}
}

func deploymentWithInactivePort(port string) k8sapi.Workload {
	return k8sapi.Deployment(&apps.Deployment{
		ObjectMeta: meta.ObjectMeta{Name: "app", Namespace: "default"},
		Spec: apps.DeploymentSpec{Template: core.PodTemplateSpec{ObjectMeta: meta.ObjectMeta{
			Annotations: map[string]string{annotation.InjectInactivePort: port},
		}}},
	})
}

func TestManagerHostUsesClusterDomain(t *testing.T) {
	got := ManagerHost("traffic", "cluster.local.")
	want := "traffic-manager.traffic.svc.cluster.local"
	if got != want {
		t.Fatalf("ManagerHost() = %q, want %q", got, want)
	}
}

func TestManagerHostKeepsLegacyHostWhenClusterDomainUnknown(t *testing.T) {
	got := ManagerHost("traffic", "")
	want := "traffic-manager.traffic"
	if got != want {
		t.Fatalf("ManagerHost() = %q, want %q", got, want)
	}
}
