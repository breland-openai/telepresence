package k8s

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

func recipientTestService(namespace string, selector map[string]string) *core.Service {
	return &core.Service{ObjectMeta: metav1.ObjectMeta{Name: "traffic-manager", Namespace: namespace}, Spec: core.ServiceSpec{Selector: selector}}
}

func recipientTestPod(namespace, name, id, account, component string, phase core.PodPhase) *core.Pod {
	return &core.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID(id), Labels: map[string]string{"component": component}},
		Spec:       core.PodSpec{ServiceAccountName: account}, Status: core.PodStatus{Phase: phase},
	}
}

func TestDevboxRecipientRejectsWrongNamespaceAccountAndMalformedUIDBeforeCredentials(t *testing.T) {
	for _, audience := range []string{"", devboxProxyStaging} {
		for name, target := range map[string]devboxManagerTarget{
			"wrong namespace":      {pod: devboxTestPodA, namespace: "developer-owned", serviceAccount: "traffic-manager"},
			"wrong serviceaccount": {pod: devboxTestPodA, namespace: "ambassador", serviceAccount: "developer-owned"},
			"empty serviceaccount": {pod: devboxTestPodA, namespace: "ambassador"},
			"malformed pod UID":    {pod: "attacker", namespace: "ambassador", serviceAccount: "traffic-manager"},
		} {
			t.Run(name+"/"+audience, func(t *testing.T) {
				for _, rootCallback := range []bool{false, true} {
					kc := devboxTestKubeconfig(devboxTestRest("production-0", devboxProxyProduction))
					var tokenCalls atomic.Int32
					kc.devboxManagerTokenBroker().makeSource = func(*clientcmdapi.ExecConfig) managerTokenSource {
						return staticTokenSourceFunc(func(context.Context) (string, error) {
							tokenCalls.Add(1)
							return "must-not-issue", nil
						})
					}
					if rootCallback {
						kc.managerTokenCallbackNegotiated = true
						kc.managerTokenCallback = &managerCallbackTokenSource{}
					}
					auth, err := connectionManagerAuthenticationForVersion(t.Context(), kc, target, audience)
					require.ErrorContains(t, err, "credential recipient is not trusted")
					require.Nil(t, auth.credentials)
					require.Zero(t, tokenCalls.Load())
				}
			})
		}
	}

	malicious := devboxManagerTarget{pod: "not-a-uid", namespace: "developer-owned", serviceAccount: "developer-owned"}
	for _, audience := range []string{"", devboxProxyStaging} {
		native := devboxTestKubeconfig(&rest.Config{BearerToken: "existing-native"})
		auth, err := connectionManagerAuthenticationForVersion(t.Context(), native, malicious, audience)
		require.NoError(t, err)
		metadata, err := auth.credentials.GetRequestMetadata(t.Context())
		require.NoError(t, err)
		require.Equal(t, "Bearer existing-native", metadata["authorization"])

		explicit := devboxTestKubeconfig(devboxTestRest("production-0", devboxProxyProduction))
		explicit.ManagerTokenFileSet = true
		explicit.ManagerTokenFile = filepath.Join(t.TempDir(), "selected-by-user")
		require.NoError(t, os.WriteFile(explicit.ManagerTokenFile, []byte("explicit-native"), 0o600))
		auth, err = connectionManagerAuthenticationForVersion(t.Context(), explicit, malicious, audience)
		require.NoError(t, err)
		metadata, err = auth.credentials.GetRequestMetadata(t.Context())
		require.NoError(t, err)
		require.Equal(t, "Bearer explicit-native", metadata["authorization"])
	}
}

func TestDevboxRecipientKubernetesProofRequiresExactServicePodAccountAndRunningPhase(t *testing.T) {
	const namespace, account = "ambassador", "traffic-manager"
	selector := map[string]string{"component": "trusted-manager"}
	service := recipientTestService(namespace, selector)
	trusted := recipientTestPod(namespace, "trusted", devboxTestPodA, account, "trusted-manager", core.PodRunning)
	for name, tc := range map[string]struct {
		objects   []runtime.Object
		uid       string
		account   string
		wantValid bool
	}{
		"service absent":              {objects: []runtime.Object{trusted}, uid: devboxTestPodA, account: account},
		"service wrong namespace":     {objects: []runtime.Object{recipientTestService("developer-owned", selector), trusted}, uid: devboxTestPodA, account: account},
		"service selector empty":      {objects: []runtime.Object{recipientTestService(namespace, nil), trusted}, uid: devboxTestPodA, account: account},
		"pod UID missing":             {objects: []runtime.Object{service, trusted}, uid: devboxTestPodB, account: account},
		"pod account wrong":           {objects: []runtime.Object{service, recipientTestPod(namespace, "attacker", devboxTestPodA, "developer-owned", "trusted-manager", core.PodRunning)}, uid: devboxTestPodA, account: account},
		"claimed account wrong":       {objects: []runtime.Object{service, trusted}, uid: devboxTestPodA, account: "developer-owned"},
		"pod does not match Service":  {objects: []runtime.Object{service, recipientTestPod(namespace, "attacker", devboxTestPodA, account, "unselected", core.PodRunning)}, uid: devboxTestPodA, account: account},
		"pod not running":             {objects: []runtime.Object{service, recipientTestPod(namespace, "pending", devboxTestPodA, account, "trusted-manager", core.PodPending)}, uid: devboxTestPodA, account: account},
		"pod is in another namespace": {objects: []runtime.Object{service, recipientTestPod("developer-owned", "attacker", devboxTestPodA, account, "trusted-manager", core.PodRunning)}, uid: devboxTestPodA, account: account},
		"trusted among decoys":        {objects: []runtime.Object{service, trusted, recipientTestPod(namespace, "attacker", devboxTestPodB, "developer-owned", "trusted-manager", core.PodRunning)}, uid: devboxTestPodA, account: account, wantValid: true},
	} {
		t.Run(name, func(t *testing.T) {
			api := fake.NewClientset(tc.objects...)
			kc := devboxTestKubeconfig(devboxTestRest("production-0", devboxProxyProduction))
			kc.Context = k8sapi.WithK8sInterface(t.Context(), api)
			err := kc.verifyDevboxManagerPod(t.Context(), namespace, tc.uid, tc.account)
			if tc.wantValid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "unable to verify the traffic-manager credential recipient")
			}
			for _, action := range api.Actions() {
				require.Equal(t, namespace, action.GetNamespace())
				if action.GetResource().Resource == "pods" {
					list := action.(k8stesting.ListAction)
					require.Equal(t, "component=trusted-manager", list.GetListRestrictions().Labels.String())
				}
			}
		})
	}
}

func TestDevboxRecipientRootCallbackUsesKubernetesProofBeforeObtainingBearer(t *testing.T) {
	const namespace, account = "ambassador", "traffic-manager"
	api := fake.NewClientset(
		recipientTestService(namespace, map[string]string{"component": "trusted-manager"}),
		recipientTestPod(namespace, "actual", devboxTestPodA, account, "trusted-manager", core.PodRunning),
		recipientTestPod(namespace, "attacker", devboxTestPodB, "developer-owned", "trusted-manager", core.PodRunning),
	)
	kc := devboxTestKubeconfig(devboxTestRest("production-0", devboxProxyProduction))
	kc.Context = k8sapi.WithK8sInterface(t.Context(), api)
	kc.devboxManagerPodVerifier = nil
	var tokenCalls atomic.Int32
	kc.devboxManagerTokenBroker().makeSource = func(*clientcmdapi.ExecConfig) managerTokenSource {
		return staticTokenSourceFunc(func(context.Context) (string, error) {
			tokenCalls.Add(1)
			return "synthetic-manager-bearer", nil
		})
	}
	require.Nil(t, kc.NegotiatedDevboxManagerTokenProvider("developer-owned"))
	provider := kc.NegotiatedDevboxManagerTokenProvider(namespace)
	require.NotNil(t, provider)
	for _, uid := range []string{devboxTestPodB, devboxTestClient, "invalid"} {
		token, err := provider(t.Context(), uid, devboxProxyStaging)
		require.Error(t, err)
		require.Empty(t, token)
	}
	require.Zero(t, tokenCalls.Load())
	token, err := provider(t.Context(), devboxTestPodA, devboxProxyStaging)
	require.NoError(t, err)
	require.Equal(t, "synthetic-manager-bearer", token)
	require.EqualValues(t, 1, tokenCalls.Load())

	_, err = provider(t.Context(), devboxTestPodA, devboxProxyProduction)
	require.Error(t, err)
	require.EqualValues(t, 1, tokenCalls.Load())
}

func TestDevboxRecipientBrokerCachesSuccessfulProofOnlyAndRevalidatesExpiry(t *testing.T) {
	const namespace, account = "ambassador", "traffic-manager"
	b := &devboxManagerTokenBroker{}
	var calls int
	fail := true
	verify := func(_ context.Context, gotNamespace, gotUID, gotAccount string) error {
		calls++
		require.Equal(t, namespace, gotNamespace)
		require.Contains(t, []string{devboxTestPodA, devboxTestPodB}, gotUID)
		require.Equal(t, account, gotAccount)
		if fail {
			return errors.New("not proven")
		}
		return nil
	}
	for range 2 {
		require.ErrorContains(t, b.verifyManagerPod(t.Context(), namespace, devboxTestPodA, account, verify), "not proven")
	}
	require.Equal(t, 2, calls)
	fail = false
	for range 2 {
		require.NoError(t, b.verifyManagerPod(t.Context(), namespace, devboxTestPodA, account, verify))
	}
	require.Equal(t, 3, calls)
	require.NoError(t, b.verifyManagerPod(t.Context(), namespace, devboxTestPodB, account, verify))
	require.Equal(t, 4, calls)
	b.mu.Lock()
	b.verifiedPods[devboxTestPodA] = time.Now().Add(-2 * devboxManagerPodProofTTL)
	b.mu.Unlock()
	fail = true
	for range 2 {
		require.ErrorContains(t, b.verifyManagerPod(t.Context(), namespace, devboxTestPodA, account, verify), "not proven")
	}
	require.Equal(t, 6, calls)
	require.NoError(t, b.verifyManagerPod(t.Context(), namespace, devboxTestPodB, account, verify))
	require.Equal(t, 6, calls)
}

func TestDevboxRecipientDiscoveryNeverFallsBackToDeveloperNamespace(t *testing.T) {
	const trustedNamespace, developerNamespace = "ambassador", "developer-owned"
	for _, hasTrusted := range []bool{false, true} {
		objects := []runtime.Object{recipientTestService(developerNamespace, map[string]string{"component": "attacker"})}
		if hasTrusted {
			objects = append(objects, recipientTestService(trustedNamespace, map[string]string{"component": "trusted-manager"}))
		}
		api := fake.NewClientset(objects...)
		kc := devboxTestKubeconfig(devboxTestRest("production-0", devboxProxyProduction))
		kc.Context = k8sapi.WithK8sInterface(t.Context(), api)
		kc.Namespace = developerNamespace
		cluster := &Cluster{Kubeconfig: kc, currentMappedNamespaces: map[string]bool{developerNamespace: true}}
		namespace, err := cluster.determineTrafficManagerNamespace()
		if hasTrusted {
			require.NoError(t, err)
			require.Equal(t, trustedNamespace, namespace)
		} else {
			require.ErrorContains(t, err, "system-trusted namespace")
			require.Empty(t, namespace)
		}
		require.Len(t, api.Actions(), 1)
		require.Equal(t, trustedNamespace, api.Actions()[0].GetNamespace())
	}
}
