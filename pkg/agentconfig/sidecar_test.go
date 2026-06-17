package agentconfig

import (
	"fmt"
	"sync"
	"testing"

	core "k8s.io/api/core/v1"
)

func TestMarshalTightDoesNotMutateSidecar(t *testing.T) {
	sc := &Sidecar{
		AgentImage:          "registry.example/traffic-agent:latest",
		PullPolicy:          string(core.PullAlways),
		PullSecrets:         []core.LocalObjectReference{{Name: "agent-pull-secret"}},
		InitResources:       &core.ResourceRequirements{},
		SecurityContext:     &core.SecurityContext{},
		InitSecurityContext: &core.SecurityContext{},
	}

	tight, err := MarshalTight(sc)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalJSON(tight)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.AgentImage != "" ||
		decoded.PullPolicy != "" ||
		decoded.PullSecrets != nil ||
		decoded.InitResources != nil ||
		decoded.SecurityContext != nil ||
		decoded.InitSecurityContext != nil {
		t.Fatalf("tight config retained container creation fields: %#v", decoded)
	}

	if sc.AgentImage == "" ||
		sc.PullPolicy == "" ||
		sc.PullSecrets == nil ||
		sc.InitResources == nil ||
		sc.SecurityContext == nil ||
		sc.InitSecurityContext == nil {
		t.Fatalf("MarshalTight mutated source config: %#v", sc)
	}
}

func TestMarshalTightConcurrentContainerBuilds(t *testing.T) {
	const (
		agentImage = "registry.example/traffic-agent:latest"
		workers    = 8
		iterations = 1000
	)

	sc := &Sidecar{AgentImage: agentImage}
	start := make(chan struct{})
	errCh := make(chan error, workers*2)
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				if _, err := MarshalTight(sc); err != nil {
					errCh <- err
					return
				}
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				if image := InitContainer(sc, nil).Image; image != agentImage {
					errCh <- fmt.Errorf("injected container image = %q, want %q", image, agentImage)
					return
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
