package state

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	argofake "github.com/datawire/argo-rollouts-go-client/pkg/client/clientset/versioned/fake"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
	"github.com/telepresenceio/telepresence/v2/pkg/informer"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

type agentSessionTestTransport func(*http.Request) (*http.Response, error)

func (transport agentSessionTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func awaitSessionTest[T any](t *testing.T, output <-chan T) T {
	t.Helper()
	select {
	case value := <-output:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for registration-test event")
		var zero T
		return zero
	}
}

func TestOwnerLookupDoesNotBlockAgentSessions(t *testing.T) {
	for _, mode := range []string{"registration", "expiry", "departure"} {
		t.Run(mode, func(t *testing.T) {
			root, st := newServiceInterceptState(t)
			ownerRequest := make(chan context.Context, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			openGate := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(openGate)

			transport := agentSessionTestTransport(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodGet || req.URL.Path != "/apis/apps/v1/namespaces/default/deployments/uncached-owner" {
					return nil, fmt.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
				}
				select {
				case ownerRequest <- req.Context():
				default:
				}
				select {
				case <-release:
					return nil, fmt.Errorf("registration-test completed the owner lookup")
				case <-req.Context().Done():
					return nil, req.Context().Err()
				}
			})
			config := &rest.Config{Host: "http://offline.invalid", QPS: 50, Burst: 100}
			client, err := kubernetes.NewForConfigAndClient(config, &http.Client{Transport: transport})
			require.NoError(t, err)
			root = k8sapi.WithJoinedClientSetInterface(root, client, argofake.NewSimpleClientset())
			root = informer.WithFactory(root, "")
			root = managerutil.WithEnv(root, &managerutil.Env{
				ManagerNamespace:     "ambassador",
				EnabledWorkloadKinds: k8sapi.Kinds{k8sapi.DeploymentKind},
			})
			st.backgroundCtx = root
			factory := informer.GetK8sFactory(root, "")
			labels := map[string]string{"app": "example-service"}
			require.NoError(t, factory.Core().V1().Services().Informer().GetStore().Add(&core.Service{
				ObjectMeta: meta.ObjectMeta{Name: "example-service", Namespace: "default", UID: "shared-service-uid"},
				Spec:       core.ServiceSpec{Selector: labels},
			}))
			intercept := &Intercept{InterceptInfo: &rpc.InterceptInfo{
				Id: "registration-test", Spec: sharedServiceSpec(), Disposition: rpc.InterceptDispositionType_WAITING,
				ServiceWorkloads: sharedServiceWorkloads("example-service"),
			}}
			intercept.addParticipant(serviceAgent("example-service", "first", "192.0.2.10").AgentInfo)
			require.NoError(t, factory.Core().V1().Pods().Informer().GetStore().Add(servicePod("selected-pod", "uncached-owner", labels)))
			st.intercepts.Store(intercept.Id, intercept)

			now := time.Now()
			request, cancel := context.WithCancel(root)
			defer cancel()
			holder := make(chan error, 1)
			first := serviceAgent("example-service", "first", "192.0.2.10").AgentInfo
			if mode == "registration" {
				go func() {
					_, err := st.AddAgent(request, first, nil, now)
					holder <- err
				}()
			} else {
				old := newAgentSessionState(root, tunnel.SessionID(AgentSessionIDPrefix+first.PodUid), first, now.Add(-time.Minute))
				st.agents.Store(old.sessionID(), old)
				go func() {
					if mode == "expiry" {
						st.expireAgentSession(old, now)
					} else {
						st.RemoveAgentSession(old)
					}
					holder <- nil
				}()
			}

			outbound := awaitSessionTest(t, ownerRequest)
			unrelated := serviceAgent("unrelated", "unrelated", "192.0.2.11").AgentInfo
			unrelated.Namespace = "elsewhere"
			registered := make(chan error, 1)
			go func() {
				_, err := st.AddAgent(root, unrelated, nil, now)
				registered <- err
			}()
			require.NoError(t, awaitSessionTest(t, registered))
			require.NoError(t, outbound.Err(), "the owner lookup must still be stalled")
			canceled := serviceAgent("canceled", "canceled", "192.0.2.12").AgentInfo
			canceledCtx, cancelRetry := context.WithCancel(root)
			cancelRetry()
			_, err = st.AddAgent(canceledCtx, canceled, nil, now)
			require.Equal(t, codes.Canceled, status.Code(err))
			require.Nil(t, st.GetAgent(tunnel.SessionID(AgentSessionIDPrefix+canceled.PodUid)))
			_, hasDeadline := outbound.Deadline()
			require.True(t, hasDeadline)
			if mode == "registration" {
				cancel()
				require.Equal(t, codes.Canceled, status.Code(awaitSessionTest(t, holder)))
				require.Nil(t, st.GetAgent(tunnel.SessionID(AgentSessionIDPrefix+first.PodUid)))
			}
			var published *Intercept
			if mode == "expiry" {
				retry := proto.Clone(first).(*rpc.AgentInfo)
				retry.Namespace = "elsewhere"
				retryCtx, cancel := context.WithTimeout(root, 25*time.Millisecond)
				_, err := st.AddAgent(retryCtx, retry, nil, now)
				cancel()
				require.Equal(t, codes.DeadlineExceeded, status.Code(err))
				require.Nil(t, st.GetAgent(tunnel.SessionID(AgentSessionIDPrefix+first.PodUid)))

				replacement := proto.Clone(first).(*rpc.AgentInfo)
				replacement.PodIp = "192.0.2.13"
				st.RestoreAgents([]*rpc.AgentInfo{replacement}, now)
				current := st.GetAgent(tunnel.SessionID(AgentSessionIDPrefix + replacement.PodUid))
				require.NotNil(t, current)
				published = st.ApplyAgentReview(root, intercept.Id, current, &rpc.ReviewInterceptRequest{
					PodIp: replacement.PodIp, Disposition: rpc.InterceptDispositionType_ACTIVE,
				})
				require.Equal(t, rpc.InterceptDispositionType_ACTIVE, published.Disposition)
				require.Equal(t, replacement.PodIp, published.PodIp)
			}

			openGate()
			if mode != "registration" {
				require.NoError(t, awaitSessionTest(t, holder))
			}
			if published != nil {
				current, _ := st.intercepts.Load(intercept.Id)
				require.Same(t, published, current, "stale expiry must not touch the replacement route")
			}
		})
	}
}

func TestInterceptRestoreLookupDoesNotBlockAgentSessions(t *testing.T) {
	root, st := newServiceInterceptState(t)
	started := make(chan struct{}, 1)
	transport := agentSessionTestTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/apis/apps/v1/namespaces/default/deployments/owner" {
			return nil, fmt.Errorf("unexpected path: %s", req.URL.Path)
		}
		started <- struct{}{}
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	client, err := kubernetes.NewForConfigAndClient(&rest.Config{Host: "http://offline.invalid"}, &http.Client{Transport: transport})
	require.NoError(t, err)
	root = k8sapi.WithJoinedClientSetInterface(root, client, argofake.NewSimpleClientset())
	restoreCtx, stopRestore := context.WithCancel(root)
	defer stopRestore()
	restored := make(chan struct{}, 1)
	go func() {
		st.RestoreIntercepts(restoreCtx, []*rpc.InterceptInfo{{
			Id:   "restored",
			Spec: &rpc.InterceptSpec{Agent: "owner", Namespace: "default", WorkloadKind: "Deployment"},
		}}, time.Now())
		restored <- struct{}{}
	}()
	awaitSessionTest(t, started)

	register := func(ctx context.Context, pod string) <-chan error {
		result := make(chan error, 1)
		go func() {
			agent := serviceAgent("unrelated", pod, "192.0.2.11").AgentInfo
			_, err := st.AddAgent(ctx, agent, nil, time.Now())
			result <- err
		}()
		return result
	}
	require.NoError(t, awaitSessionTest(t, register(root, "peer")))
	stopRestore()
	awaitSessionTest(t, restored)
}
