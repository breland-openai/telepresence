package k8s

import (
	"encoding/json/v2"
	"net/netip"
	"strings"
)

const maxDevboxManagerPolicySize = 16 * 1024

type devboxManagerPolicy struct {
	Version               int               `json:"version"`
	TenantID              string            `json:"tenantID"`
	ManagerNamespace      string            `json:"managerNamespace"`
	ManagerServiceAccount string            `json:"managerServiceAccount"`
	ProxyAudiences        map[string]string `json:"proxyAudiences"`
	ClusterAudiences      map[string]string `json:"clusterAudiences"`
}

func parseDevboxManagerPolicy(data []byte) *devboxManagerPolicy {
	var policy devboxManagerPolicy
	if len(data) > maxDevboxManagerPolicySize || json.Unmarshal(data, &policy, json.RejectUnknownMembers(true)) != nil ||
		policy.Version != 1 || !canonicalDevboxUUID(policy.TenantID) || !officialDevboxClusterName(policy.ManagerNamespace) ||
		!canonicalDevboxServiceAccount(policy.ManagerServiceAccount) || len(policy.ProxyAudiences) == 0 || len(policy.ProxyAudiences) > 32 ||
		len(policy.ClusterAudiences) == 0 || len(policy.ClusterAudiences) > 128 {
		return nil
	}
	for host, audience := range policy.ProxyAudiences {
		if !canonicalDevboxProxyHost(host) || !canonicalDevboxUUID(audience) {
			return nil
		}
	}
	for cluster, audience := range policy.ClusterAudiences {
		if !officialDevboxClusterName(cluster) || !policy.allowsAudience(audience) {
			return nil
		}
	}
	return &policy
}

func canonicalDevboxServiceAccount(name string) bool {
	if name == "" || len(name) > 253 {
		return false
	}
	for label := range strings.SplitSeq(name, ".") {
		if !officialDevboxClusterName(label) {
			return false
		}
	}
	return true
}

func (p *devboxManagerPolicy) managerLocation() (string, string) {
	if p == nil {
		return defaultManagerNamespace, "traffic-manager"
	}
	return p.ManagerNamespace, p.ManagerServiceAccount
}

func canonicalDevboxProxyHost(host string) bool {
	if len(host) > 253 || !strings.Contains(host, ".") {
		return false
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return false
	}
	for label := range strings.SplitSeq(host, ".") {
		if !officialDevboxClusterName(label) {
			return false
		}
	}
	return true
}

func (p *devboxManagerPolicy) allowsAudience(audience string) bool {
	if p == nil || !canonicalDevboxUUID(audience) {
		return false
	}
	for _, trusted := range p.ProxyAudiences {
		if trusted == audience {
			return true
		}
	}
	return false
}
