package k8s

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/telepresenceio/telepresence/v2/pkg/dos"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

const (
	devboxIdentityFile       = "/var/run/secrets/azure/tokens/azure-identity-token"
	devboxAuthority          = "https://login.microsoftonline.com/"
	devboxManagerPodProofTTL = 15 * time.Second
)

// NegotiatedDevboxManagerTokenProvider grants a root daemon access to a
// credential scoped to an allowlisted audience and a resolved manager pod.
func (kf *Kubeconfig) NegotiatedDevboxManagerTokenProvider(managerNamespace string) func(context.Context, string, string) (string, error) {
	namespace, serviceAccount := kf.trustedDevboxManagerPolicy().managerLocation()
	if broker := kf.devboxManagerTokenBroker(); broker != nil && managerNamespace == namespace {
		return func(ctx context.Context, managerPod, audience string) (string, error) {
			source, err := broker.forManager(managerPod, audience)
			if err != nil {
				return "", err
			}
			verify := kf.devboxManagerPodVerifier
			if verify == nil {
				verify = kf.verifyDevboxManagerPod
			}
			if err = broker.verifyManagerPod(ctx, namespace, managerPod, serviceAccount, verify); err != nil {
				return "", err
			}
			return source.Token(ctx)
		}
	}
	return nil
}

func (kf *Kubeconfig) verifyDevboxManagerPod(ctx context.Context, namespace, podID, serviceAccount string) error {
	client := k8sapi.GetK8sInterface(kf).CoreV1()
	service, err := client.Services(namespace).Get(ctx, "traffic-manager", metav1.GetOptions{})
	if err != nil || len(service.Spec.Selector) == 0 {
		return errors.New("unable to verify the traffic-manager credential recipient")
	}
	pods, err := client.Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(service.Spec.Selector).String()})
	if err != nil {
		return errors.New("unable to verify the traffic-manager credential recipient")
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if string(pod.UID) == podID && pod.Namespace == namespace && pod.Spec.ServiceAccountName == serviceAccount && pod.Status.Phase == core.PodRunning {
			return nil
		}
	}
	return errors.New("unable to verify the traffic-manager credential recipient")
}

func (kf *Kubeconfig) devboxManagerTokenBroker() *devboxManagerTokenBroker {
	if kf == nil || kf.RestConfig == nil {
		return nil
	}
	kf.devboxManagerTokenOnce.Do(func() {
		policy := kf.trustedDevboxManagerPolicy()
		if ec := policy.officialProxyExec(kf.RestConfig); ec != nil {
			if audience := policy.managerAudience(kf.RestConfig.Host); audience != "" {
				kf.devboxManagerTokens = &devboxManagerTokenBroker{exec: ec, managerAudience: audience}
			}
		}
	})
	return kf.devboxManagerTokens
}

func (kf *Kubeconfig) trustedDevboxManagerPolicy() *devboxManagerPolicy {
	if kf == nil {
		return nil
	}
	kf.devboxManagerPolicyOnce.Do(func() {
		loader := kf.devboxManagerPolicyLoader
		if loader == nil {
			loader = loadSystemDevboxManagerPolicy
		}
		kf.devboxManagerPolicyValue = loader()
	})
	return kf.devboxManagerPolicyValue
}

type managerCredentialGate struct {
	mu          sync.RWMutex
	credentials *managerTokenCredentials
}

func (g *managerCredentialGate) activate(credentials *managerTokenCredentials) {
	g.mu.Lock()
	g.credentials = credentials
	g.mu.Unlock()
}

func (g *managerCredentialGate) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	g.mu.RLock()
	credentials := g.credentials
	g.mu.RUnlock()
	if credentials == nil {
		return nil, nil
	}
	return credentials.GetRequestMetadata(ctx, uri...)
}

func (*managerCredentialGate) RequireTransportSecurity() bool { return false }

type devboxManagerTarget struct{ pod, namespace, serviceAccount string }

func connectionManagerAuthenticationForVersion(ctx context.Context, kc *Kubeconfig, target devboxManagerTarget, audience string) (managerConnectionAuth, error) {
	if kc == nil {
		return kubeconfigManagerAuthentication(ctx, nil), nil
	}
	if kc.ManagerTokenFileSet || (kc.managerTokenCallback != nil && !kc.managerTokenCallbackNegotiated) {
		return connectionManagerAuthentication(ctx, kc)
	}
	policy := kc.trustedDevboxManagerPolicy()
	managedCredentials := kc.managerTokenCallbackNegotiated || usesDevboxWorkloadIdentity(kc.RestConfig)
	if managedCredentials {
		namespace, serviceAccount := policy.managerLocation()
		if target.namespace != namespace || target.serviceAccount != serviceAccount || !canonicalDevboxUUID(target.pod) {
			return managerConnectionAuth{}, errors.New("traffic-manager credential recipient is not trusted by system policy")
		}
	}
	if audience == "" {
		return kubeconfigManagerAuthentication(ctx, kc), nil
	}
	if !canonicalDevboxUUID(audience) {
		return managerConnectionAuth{}, errors.New("traffic-manager advertised an unsupported Devbox credential audience")
	}
	var trustedAudience string
	if kc.RestConfig != nil {
		trustedAudience = policy.managerAudience(kc.RestConfig.Host)
	}
	if audience != trustedAudience {
		if managedCredentials {
			return managerConnectionAuth{}, errors.New("traffic-manager Devbox credential audience is not trusted by system policy")
		}
		return kubeconfigManagerAuthentication(ctx, kc), nil
	}
	var source managerTokenSource
	if kc.managerTokenCallbackNegotiated {
		callback, ok := kc.managerTokenCallback.(*managerCallbackTokenSource)
		if !ok || !canonicalDevboxUUID(target.pod) {
			return managerConnectionAuth{}, errors.New("traffic-manager Devbox credential was not bound to a valid manager pod")
		}
		bound := *callback
		bound.managerPod, bound.audience = target.pod, audience
		source = &safeDevboxTokenSource{source: &bound}
	} else if broker := kc.devboxManagerTokenBroker(); broker != nil {
		var err error
		if source, err = broker.forManager(target.pod, audience); err != nil {
			return managerConnectionAuth{}, err
		}
	}
	if source == nil {
		if usesDevboxWorkloadIdentity(kc.RestConfig) {
			return managerConnectionAuth{}, errors.New("kubeconfig is not trusted for the negotiated traffic-manager Devbox credential")
		}
		return kubeconfigManagerAuthentication(ctx, kc), nil
	}
	if _, err := source.Token(ctx); err != nil {
		return managerConnectionAuth{}, err
	}
	return managerConnectionAuth{credentials: newRequiredManagerTokenCredentials(source), hasBearer: true}, nil
}

type devboxManagerTokenKey struct{ pod, audience string }

type devboxManagerTokenBroker struct {
	exec            *clientcmdapi.ExecConfig
	managerAudience string
	mu              sync.Mutex
	sources         map[devboxManagerTokenKey]managerTokenSource
	verifiedPods    map[string]time.Time
	makeSource      func(*clientcmdapi.ExecConfig) managerTokenSource
}

func (b *devboxManagerTokenBroker) verifyManagerPod(
	ctx context.Context, namespace, podID, serviceAccount string, verify func(context.Context, string, string, string) error,
) error {
	b.mu.Lock()
	last := b.verifiedPods[podID]
	b.mu.Unlock()
	if !last.IsZero() && time.Since(last) < devboxManagerPodProofTTL {
		return nil
	}
	if err := verify(ctx, namespace, podID, serviceAccount); err != nil {
		return err
	}
	b.mu.Lock()
	if b.verifiedPods == nil || len(b.verifiedPods) >= 16 {
		b.verifiedPods = make(map[string]time.Time)
	}
	b.verifiedPods[podID] = time.Now()
	b.mu.Unlock()
	return nil
}

func (b *devboxManagerTokenBroker) forManager(managerPod, audience string) (managerTokenSource, error) {
	if !canonicalDevboxUUID(managerPod) || !canonicalDevboxUUID(audience) || audience != b.managerAudience {
		return nil, errors.New("traffic-manager Devbox credential request has an invalid manager or audience")
	}
	key := devboxManagerTokenKey{pod: managerPod, audience: audience}
	b.mu.Lock()
	defer b.mu.Unlock()
	if source := b.sources[key]; source != nil {
		return source, nil
	}
	if b.sources == nil || len(b.sources) >= 16 {
		b.sources = make(map[devboxManagerTokenKey]managerTokenSource)
	}
	clone := b.exec.DeepCopy()
	clone.Args[10] = "api://" + audience
	var underlying managerTokenSource
	if b.makeSource != nil {
		underlying = b.makeSource(clone)
	} else {
		exec := newExecTokenSource(clone)
		exec.requireExpiry = true
		underlying = exec
	}
	source := &safeDevboxTokenSource{source: underlying}
	b.sources[key] = source
	return source, nil
}

func (p *devboxManagerPolicy) officialProxyExec(rc *rest.Config) *clientcmdapi.ExecConfig {
	if p == nil || !officialDevboxProxyRESTConfig(rc) {
		return nil
	}
	audience := p.proxyAudience(rc.Host)
	if audience == "" {
		return nil
	}
	ec := rc.ExecProvider
	switch ec.Command {
	case "kubelogin", "/usr/bin/kubelogin", "/usr/local/bin/kubelogin":
	default:
		return nil
	}
	args := ec.Args
	if len(args) != 11 || args[0] != "get-token" || args[1] != "--login" || args[2] != "workloadidentity" ||
		args[3] != "--client-id" || !canonicalDevboxUUID(args[4]) || args[5] != "--tenant-id" || args[6] != p.TenantID ||
		args[7] != "--federated-token-file" || args[8] != devboxIdentityFile || args[9] != "--server-id" || args[10] != "api://"+audience ||
		len(ec.Env) != 1 || ec.Env[0].Name != "AZURE_AUTHORITY_HOST" || ec.Env[0].Value != devboxAuthority {
		return nil
	}
	return ec.DeepCopy()
}

func usesDevboxWorkloadIdentity(rc *rest.Config) bool {
	if rc == nil || rc.ExecProvider == nil {
		return false
	}
	args := rc.ExecProvider.Args
	return len(args) >= 3 && args[0] == "get-token" && args[1] == "--login" && args[2] == "workloadidentity"
}

func officialDevboxProxyRESTConfig(rc *rest.Config) bool {
	return rc != nil && rc.ExecProvider != nil && rc.BearerToken == "" && rc.BearerTokenFile == "" && rc.AuthProvider == nil &&
		rc.Username == "" && rc.Password == "" && rc.CertFile == "" && rc.KeyFile == "" && len(rc.CertData) == 0 && len(rc.KeyData) == 0 &&
		rc.Impersonate.UserName == "" && rc.Impersonate.UID == "" && len(rc.Impersonate.Groups) == 0 && len(rc.Impersonate.Extra) == 0 &&
		!rc.Insecure && rc.ServerName == "" && rc.CAFile == "" && len(rc.CAData) == 0 && rc.Proxy == nil && rc.Transport == nil && rc.WrapTransport == nil
}

func canonicalDevboxUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

func (p *devboxManagerPolicy) approvedProxyTarget(server string) (string, string) {
	u, err := url.Parse(server)
	if p == nil || err != nil || u.Scheme != "https" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" ||
		strings.Contains(server, "#") || (u.Host != u.Hostname() && u.Host != u.Hostname()+":443") {
		return "", ""
	}
	cluster, present := strings.CutPrefix(u.Path, "/clusters/")
	if !present || !officialDevboxClusterName(cluster) {
		return "", ""
	}
	if audience := p.ProxyAudiences[u.Hostname()]; audience != "" {
		return audience, cluster
	}
	return "", ""
}

func (p *devboxManagerPolicy) proxyAudience(server string) string {
	audience, _ := p.approvedProxyTarget(server)
	return audience
}

func (p *devboxManagerPolicy) managerAudience(server string) string {
	if _, cluster := p.approvedProxyTarget(server); cluster != "" {
		return p.ClusterAudiences[cluster]
	}
	return ""
}

func officialDevboxClusterName(cluster string) bool {
	if cluster == "" || len(cluster) > 63 || cluster[0] == '-' || cluster[len(cluster)-1] == '-' {
		return false
	}
	for _, char := range cluster {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
			return false
		}
	}
	return true
}

type safeDevboxTokenSource struct{ source managerTokenSource }

func (s *safeDevboxTokenSource) Token(ctx context.Context) (string, error) {
	token, err := s.source.Token(dos.WithStderr(ctx, io.Discard))
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if err != nil || len(token) > maxManagerTokenFileSize || !validManagerBearerToken(token) {
		return "", errors.New("unable to obtain the negotiated Devbox traffic-manager credential")
	}
	return token, nil
}
