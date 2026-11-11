package auth

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/telepresenceio/telepresence/v2/regression_test/framework/cli"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/managers"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/rt"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/workloads"
)

// interceptFailTimeout bounds how long a doomed intercept attempt (no
// direct agent path and no QUIC tunnel) may take to fail: well under the
// intercept timeout it would otherwise wait out.
const interceptFailTimeout = 60 * time.Second

// connectOnlyRules grants exactly what the chart's traffic-manager-connect
// Role grants without clientRbac.legacyAccess: pods/portforward create
// scoped to the known traffic-manager pod name, plus
// connections.telepresence.io create. No attachments grant here -- that is
// bound in the app namespace instead, by appAttachOnlyManifest.
const connectOnlyRules = `  - apiGroups: [""]
    resources: ["pods/portforward"]
    resourceNames: ["traffic-manager-0"]
    verbs: ["create"]
  - apiGroups: ["telepresence.io"]
    resources: ["connections"]
    verbs: ["create"]`

// appAttachOnlyManifest is grantIdentityManifest's app-namespace
// counterpart: a Role granting attachments.telepresence.io create/get in
// the app namespace (%[2]s), bound to the ServiceAccount named %[1]s that
// lives in the manager namespace (%[3]s). No pods/portforward grant at
// all, so a port-forward dial to an agent pod in this namespace is
// refused.
const appAttachOnlyManifest = `apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: %[1]s
  namespace: %[2]s
rules:
  - apiGroups: ["telepresence.io"]
    resources: ["attachments"]
    verbs: ["create", "get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: %[1]s
  namespace: %[2]s
subjects:
  - kind: ServiceAccount
    name: %[1]s
    namespace: %[3]s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: %[1]s
`

// createAppAttachRole applies appAttachOnlyManifest for name in appNS,
// deleting any stale leftover first, and registers t.Cleanup to remove it.
func createAppAttachRole(t *testing.T, ctx context.Context, r *rt.Runtime, name, appNS string) {
	t.Helper()
	deleteAppAttachRole(t, ctx, r, name, appNS)

	manifest := fmt.Sprintf(appAttachOnlyManifest, name, appNS, managers.ManagerNamespace)
	path := filepath.Join(t.TempDir(), name+"-app.yaml")
	if err := os.WriteFile(path, []byte(manifest), 0o644); err != nil {
		t.Fatalf("writing app-namespace RBAC manifest for %s: %v", name, err)
	}
	if _, err := r.Kubectl(ctx, appNS, "apply", "-f", path); err != nil {
		t.Fatalf("applying app-namespace RBAC manifest for %s: %v", name, err)
	}
	t.Cleanup(func() { deleteAppAttachRole(t, ctx, r, name, appNS) })
}

// deleteAppAttachRole removes the RoleBinding and Role createAppAttachRole
// creates, ignoring a missing resource.
func deleteAppAttachRole(t *testing.T, ctx context.Context, r *rt.Runtime, name, appNS string) {
	t.Helper()
	for _, kind := range []string{"rolebinding", "role"} {
		if _, err := r.Kubectl(ctx, appNS, "delete", kind, name, "--ignore-not-found"); err != nil {
			t.Fatalf("deleting %s %s in %s: %v", kind, name, appNS, err)
		}
	}
}

// AttachWithoutPortForward covers an identity with a policy grant to attach but
// no usable channel for intercepted traffic.
type AttachWithoutPortForward struct {
	rt.Suite
}

func init() {
	rt.Register(&AttachWithoutPortForward{}, rt.InArea("auth"), rt.NeedsManager(authGrantSpec("telepresence")))
}

// Test_InterceptWithoutPodsPortForward rejects the attachment before injecting an agent.
func (s *AttachWithoutPortForward) Test_InterceptWithoutPodsPortForward() {
	t := s.T()
	ctx := s.Ctx()
	r := s.R()
	ns := s.AppNamespace()
	s.Manager()

	name := createGrantIdentity(t, ctx, r, "rtest-auth-lazy-pf", connectOnlyRules)
	createAppAttachRole(t, ctx, r, name, ns)

	freeDefaultConnection(t, ns)
	quitDefensively(t, r, ctx, "AttachWithoutPortForward:Test_InterceptWithoutPodsPortForward")

	// --mapped-namespaces ns keeps the client from watching every namespace
	// in the (shared, long-lived) cluster: with the default "all namespaces"
	// mode, the client-side accessibility scan (one SelfSubjectAccessReview
	// per cluster namespace) can take far longer than this test's fail-fast
	// budget on a cluster that has accumulated many namespaces.
	rt.Mutate(t, rt.ConnectionFixture(ns, rt.ConnExtraArgs("--as", identity(name), "--mapped-namespaces", ns)))

	wl := s.Workload(workloads.Echo("lazy-pf"))
	ls := s.LocalEcho()

	interceptOpts := append(rt.ToLocal(ls, "http")(), cli.MountFalse()()...)
	iArgs := append([]string{"intercept", wl.Name, "--namespace", ns}, interceptOpts...)

	podUIDs := func() string {
		out, err := r.Kubectl(ctx, ns, "get", "pods", "-l", "app="+wl.Name,
			"-o", `jsonpath={range .items[*]}{.metadata.uid}{" "}{.spec.containers[*].name}{"\n"}{end}`)
		s.Require().NoError(err)
		result := strings.TrimSpace(out)
		s.Require().NotEmpty(result, "expected a live workload pod")
		return result
	}
	before := podUIDs()
	start := time.Now()
	_, stderr, err := r.CLI().Run(ctx, iArgs...)
	elapsed := time.Since(start)

	s.Require().Error(err, "intercept should fail without a direct agent path or a QUIC tunnel")
	s.Contains(stderr, "pods/portforward")
	s.Contains(stderr, "QUIC tunnel")
	s.Less(elapsed, interceptFailTimeout, "intercept should fail quickly instead of waiting out a timeout")

	s.Contains(stderr, "No intercept was created")
	s.Equal(before, podUIDs(), "a rejected attachment must preserve the workload's running pods")
}
