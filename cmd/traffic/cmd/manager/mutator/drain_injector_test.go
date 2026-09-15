package mutator

import (
	"context"
	"encoding/json/v2"
	"testing"
	"time"

	admission "k8s.io/api/admission/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sTypes "k8s.io/apimachinery/pkg/types"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
)

func TestDeleteAdmissionPreservesDrainingAgent(t *testing.T) {
	tests := []struct {
		name         string
		configure    func(*managerutil.Env, *core.Pod, *admission.AdmissionRequest, Map)
		wantInactive bool
	}{
		{
			name: "managed drain preserves existing session",
		},
		{
			name: "unrelated application container is ignored",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers = append([]core.Container{{Name: "app"}}, pod.Spec.Containers...)
			},
		},
		{
			name: "no traffic agent",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers = nil
			},
			wantInactive: true,
		},
		{
			name: "different container cannot claim the hook",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Name = "another-sidecar"
			},
			wantInactive: true,
		},
		{
			name: "missing lifecycle",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Lifecycle = nil
			},
			wantInactive: true,
		},
		{
			name: "missing prestop",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Lifecycle.PreStop = nil
			},
			wantInactive: true,
		},
		{
			name: "missing exec",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Lifecycle.PreStop.Exec = nil
			},
			wantInactive: true,
		},
		{
			name: "different binary",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Lifecycle.PreStop.Exec.Command[0] = "/usr/local/bin/other"
			},
			wantInactive: true,
		},
		{
			name: "different subcommand",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Lifecycle.PreStop.Exec.Command[1] = "agent-ready"
			},
			wantInactive: true,
		},
		{
			name: "extra argument",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Lifecycle.PreStop.Exec.Command = append(
					pod.Spec.Containers[0].Lifecycle.PreStop.Exec.Command, "unexpected")
			},
			wantInactive: true,
		},
		{
			name: "malformed hook duration",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Lifecycle.PreStop.Exec.Command[2] = "invalid"
			},
			wantInactive: true,
		},
		{
			name: "zero hook duration",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Lifecycle.PreStop.Exec.Command[2] = "0s"
			},
			wantInactive: true,
		},
		{
			name: "negative hook duration",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Lifecycle.PreStop.Exec.Command[2] = "-1s"
			},
			wantInactive: true,
		},
		{
			name: "hook exceeds original termination grace",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Lifecycle.PreStop.Exec.Command[2] = "3m"
			},
			wantInactive: true,
		},
		{
			name: "reduced manager timeout preserves existing valid hook",
			configure: func(env *managerutil.Env, _ *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				env.AgentPreStopDrainTimeout = 10 * time.Second
			},
		},
		{
			name: "hook exceeds termination grace reserve",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.Containers[0].Lifecycle.PreStop.Exec.Command[2] = "26s"
			},
			wantInactive: true,
		},
		{
			name: "disabled manager drain",
			configure: func(env *managerutil.Env, _ *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				env.AgentPreStopDrainTimeout = 0
			},
			wantInactive: true,
		},
		{
			name: "negative manager drain",
			configure: func(env *managerutil.Env, _ *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				env.AgentPreStopDrainTimeout = -time.Second
			},
			wantInactive: true,
		},
		{
			name: "forced deletion in raw options",
			configure: func(_ *managerutil.Env, _ *core.Pod, req *admission.AdmissionRequest, _ Map) {
				req.Options.Raw = []byte(`{"gracePeriodSeconds":0}`)
			},
			wantInactive: true,
		},
		{
			name: "graceful deletion in raw options",
			configure: func(_ *managerutil.Env, _ *core.Pod, req *admission.AdmissionRequest, _ Map) {
				req.Options.Raw = []byte(`{"gracePeriodSeconds":30}`)
			},
		},
		{
			name: "forced deletion in decoded options",
			configure: func(_ *managerutil.Env, _ *core.Pod, req *admission.AdmissionRequest, _ Map) {
				req.Options.Object = &meta.DeleteOptions{GracePeriodSeconds: new(int64(0))}
			},
			wantInactive: true,
		},
		{
			name: "graceful deletion in decoded options",
			configure: func(_ *managerutil.Env, _ *core.Pod, req *admission.AdmissionRequest, _ Map) {
				req.Options.Object = &meta.DeleteOptions{GracePeriodSeconds: new(int64(30))}
			},
		},
		{
			name: "unexpected decoded options",
			configure: func(_ *managerutil.Env, _ *core.Pod, req *admission.AdmissionRequest, _ Map) {
				req.Options.Object = &meta.CreateOptions{}
			},
			wantInactive: true,
		},
		{
			name: "malformed raw options",
			configure: func(_ *managerutil.Env, _ *core.Pod, req *admission.AdmissionRequest, _ Map) {
				req.Options.Raw = []byte(`{"gracePeriodSeconds":`)
			},
			wantInactive: true,
		},
		{
			name: "zero pod termination grace",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.Spec.TerminationGracePeriodSeconds = new(int64(0))
			},
			wantInactive: true,
		},
		{
			name: "zero existing deletion grace",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, _ Map) {
				pod.DeletionGracePeriodSeconds = new(int64(0))
			},
			wantInactive: true,
		},
		{
			name: "shortened positive deletion grace preserves available drain",
			configure: func(_ *managerutil.Env, _ *core.Pod, req *admission.AdmissionRequest, _ Map) {
				req.Options.Raw = []byte(`{"gracePeriodSeconds":20}`)
			},
		},
		{
			name: "dry run does not inactivate",
			configure: func(env *managerutil.Env, _ *core.Pod, req *admission.AdmissionRequest, _ Map) {
				env.AgentPreStopDrainTimeout = 0
				req.DryRun = new(true)
			},
		},
		{
			name: "manager eviction remains inactive",
			configure: func(_ *managerutil.Env, pod *core.Pod, _ *admission.AdmissionRequest, agentConfigs Map) {
				agentConfigs.Inactivate(pod.UID)
			},
			wantInactive: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := &managerutil.Env{AgentPreStopDrainTimeout: 2 * time.Minute}
			pod := &core.Pod{
				ObjectMeta: meta.ObjectMeta{Name: "echo-agent", Namespace: "default", UID: k8sTypes.UID("echo-agent-uid")},
				Spec: core.PodSpec{
					Containers: []core.Container{{
						Name: agentconfig.ContainerName,
						Lifecycle: &core.Lifecycle{PreStop: &core.LifecycleHandler{Exec: &core.ExecAction{
							Command: []string{"/usr/local/bin/traffic", "agent-drain", "25s"},
						}}},
					}},
				},
			}
			req := &admission.AdmissionRequest{Resource: podResource, Namespace: pod.Namespace, Operation: admission.Delete}
			agentConfigs := NewWatcher()
			if tt.configure != nil {
				tt.configure(env, pod, req, agentConfigs)
			}
			encodedPod, err := json.Marshal(pod)
			if err != nil {
				t.Fatal(err)
			}
			req.OldObject = runtime.RawExtension{Raw: encodedPod}

			ctx := managerutil.WithEnv(context.Background(), env)
			injector := agentInjector{agentConfigs: agentConfigs}
			patches, err := injector.Inject(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if len(patches) > 0 {
				t.Fatalf("DELETE admission unexpectedly returned patches: %#v", patches)
			}
			if got := agentConfigs.IsInactive(pod.UID); got != tt.wantInactive {
				t.Fatalf("IsInactive(%q) = %v, want %v", pod.UID, got, tt.wantInactive)
			}
		})
	}
}
