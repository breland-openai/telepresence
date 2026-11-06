package state

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/mutator"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/pkg/annotation"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func TestServiceCreateCanceledDuringSecondaryCheck(t *testing.T) {
	for _, tc := range []struct {
		name      string
		wantCode  codes.Code
		propagate bool
	}{
		{"control", codes.OK, false},
		{"secondary returns cancellation", codes.Canceled, true},
		{"secondary finishes after cancellation", codes.Canceled, false},
		{"secondary finishes after deadline", codes.DeadlineExceeded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := sharedServiceSpec()
			primary := serviceDeployment(spec.Agent, map[string]string{"app": "shared", "track": "stable"})
			primary.Spec.Replicas = ptr.To(int32(1))
			primary.Status.ReadyReplicas = 1
			secondary := serviceDeployment(spec.Agent+"-other", map[string]string{"app": "shared", "track": "other"})
			secondary.Spec.Replicas = ptr.To(int32(0))
			svc := &core.Service{
				ObjectMeta: meta.ObjectMeta{Name: spec.ServiceName, Namespace: spec.Namespace, UID: types.UID(spec.ServiceUid)},
				Spec:       core.ServiceSpec{Selector: map[string]string{"app": "shared"}, Ports: []core.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt(8080)}}},
			}
			ctx := serviceDiscoveryContext(t, svc, primary, secondary)
			ctx = managerutil.WithEnv(ctx, &managerutil.Env{AgentInjectPolicy: agentconfig.Never, EnabledWorkloadKinds: k8sapi.Kinds{k8sapi.DeploymentKind}})
			ctx = managerutil.WithResolvedAgentImageRetriever(ctx, managerutil.ImageFromEnv(sidecarArrivalImage))
			mm := &sidecarRestoreObserver{Map: mutator.NewWatcher()}
			ctx = mutator.WithMap(ctx, mm)
			_, st := newServiceInterceptState(t)
			st.backgroundCtx = mutator.WithMap(t.Context(), mm)
			cfg, err := st.getOrCreateAgentConfig(ctx, k8sapi.Deployment(primary), false, true, spec, agentconfig.ReplacePolicyIntercept)
			require.NoError(t, err)
			encoded, err := agentconfig.MarshalTight(cfg)
			require.NoError(t, err)
			primary.Spec.Template.Annotations = map[string]string{annotation.Config: encoded}
			require.NoError(t, informer.GetK8sFactory(ctx, spec.Namespace).Apps().V1().Deployments().Informer().GetStore().Update(primary))
			client := newClientSessionState(ctx, "client", &rpc.ClientInfo{Name: "client"}, time.Now())
			st.clients.Store("client", client)
			agent := serviceAgent(primary.Name, "primary", "10.0.0.1")
			st.agents.Store("primary", agent)
			createCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if tc.wantCode == codes.DeadlineExceeded {
				var stop context.CancelFunc
				createCtx, stop = context.WithTimeout(createCtx, 40*time.Millisecond)
				defer stop()
			}
			checks := 0
			mm.dispatch = func(ctx context.Context, wl k8sapi.Workload, _ *agentconfig.Sidecar) error {
				require.Equal(t, secondary.Name, wl.GetName())
				checks++
				switch tc.wantCode {
				case codes.Canceled:
					cancel()
				case codes.DeadlineExceeded:
					<-ctx.Done()
				}
				if tc.propagate {
					return ctx.Err()
				}
				return nil
			}
			req := &rpc.CreateInterceptRequest{Session: &rpc.SessionInfo{SessionId: "client"}, InterceptSpec: spec}
			_, created, err := st.AddIntercept(createCtx, req)
			require.Equal(t, 1, checks)
			id := "client:" + spec.Name
			if tc.wantCode == codes.OK {
				require.NoError(t, err)
				require.Equal(t, string(svc.UID), created.Spec.ServiceUid)
				require.ElementsMatch(t, sharedServiceWorkloads(primary.Name, secondary.Name), created.ServiceWorkloads)
				return
			}
			stored, exists := st.GetIntercept(id)
			review := st.ApplyAgentReview(ctx, id, agent, &rpc.ReviewInterceptRequest{Id: id, Disposition: rpc.InterceptDispositionType_ACTIVE})
			snapshot := <-st.WatchIntercepts(t.Context(), func(_ string, i *Intercept) bool { return i.ClientSession.GetSessionId() == "client" })
			if exists {
				t.Fatalf("stored=%v, after late approval=%v, client-visible=%d", stored.Disposition, review.Disposition, len(snapshot.Upserts))
			}
			require.Nil(t, created)
			require.Equal(t, tc.wantCode, status.Code(err))
			require.Nil(t, review)
			require.Empty(t, snapshot.Upserts)
		})
	}
}
