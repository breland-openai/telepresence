package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	authv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/managers"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/rt"
)

const interceptRouteObserverRules = `  - apiGroups: ["telepresence.io"]
    resources: ["interceptroutes"]
    verbs: ["watch"]`

func (s *AuthPermissive) Test_RoutingObserverRequiresProjectedIdentityAndExplicitGrants() {
	t := s.T()
	if os.Getenv("RTEST_MANAGER_VERSION") != "" {
		t.Skip("the internal routing observer is exercised against the built manager")
	}
	ctx := s.Ctx()
	r := s.R()
	s.Manager()
	env := rt.Env{Ctx: ctx, T: t, R: r}
	first := rt.PrivateNamespace(env, "route-observer-a")
	second := rt.PrivateNamespace(env, "route-observer-b")
	wrongGrant := rt.PrivateNamespace(env, "route-observer-client")
	name := createNoGrantsServiceAccount(t, ctx, r, "rtest-auth-route-observer")
	grantInNamespace(t, ctx, r, name, first, interceptRouteObserverRules)
	grantInNamespace(t, ctx, r, name, second, interceptRouteObserverRules)
	grantInNamespace(t, ctx, r, name, wrongGrant, portForwardOnlyRules)

	reviewDir := t.TempDir()
	reviewPath := func(namespace, verb, group, resource, subresource string) string {
		review := &authv1.SubjectAccessReview{
			TypeMeta: metav1.TypeMeta{APIVersion: "authorization.k8s.io/v1", Kind: "SubjectAccessReview"},
			Spec: authv1.SubjectAccessReviewSpec{
				User: identity(name),
				ResourceAttributes: &authv1.ResourceAttributes{
					Namespace: namespace, Verb: verb, Group: group, Resource: resource, Subresource: subresource,
				},
			},
		}
		encoded, encodeErr := json.Marshal(review)
		s.Require().NoError(encodeErr)
		path := filepath.Join(reviewDir, namespace+".json")
		s.Require().NoError(os.WriteFile(path, encoded, 0o600))
		return path
	}
	firstReview := reviewPath(first, "watch", "telepresence.io", "interceptroutes", "")
	secondReview := reviewPath(second, "watch", "telepresence.io", "interceptroutes", "")
	wrongGrantReview := reviewPath(wrongGrant, "create", "", "pods", "portforward")
	allowed := func(path string) bool {
		out, reviewErr := r.Kubectl(ctx, "", "create", "-f", path, "-o", "jsonpath={.status.allowed}")
		return reviewErr == nil && strings.TrimSpace(out) == "true"
	}
	s.Require().Eventually(func() bool {
		return allowed(firstReview) && allowed(secondReview) && allowed(wrongGrantReview)
	}, 30*time.Second, 500*time.Millisecond, "the API server must see all three distinct RBAC grants")

	projectedToken := func(serviceAccount, audience string) string {
		out, tokenErr := r.Kubectl(ctx, managers.ManagerNamespace, "create", "token", serviceAccount, "--audience", audience)
		s.Require().NoError(tokenErr, "creating a ServiceAccount token for the requested audience")
		token := strings.TrimSpace(out)
		s.Require().NotEmpty(token)
		return token
	}
	token := projectedToken(name, agentconfig.ManagerTokenAudience)
	ordinaryClientToken := projectedToken(managers.TestServiceAccount, agentconfig.ManagerTokenAudience)
	wrongAudienceToken := projectedToken(name, "rtest-routing-observer-unrelated")
	defaultAPIToken := kubectlCreateToken(t, ctx, r, name)
	s.Require().NotEmpty(defaultAPIToken)
	mc, closeFn, err := rt.ManagerClient(env, managers.ManagerNamespace)
	s.Require().NoError(err)
	defer closeFn()

	receive := func(bearer string, namespaces ...string) (*manager.InterceptRouteSnapshot, error) {
		wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if bearer != "" {
			wctx = metadata.AppendToOutgoingContext(wctx, "authorization", "Bearer "+bearer)
		}
		stream, watchErr := mc.WatchInterceptRoutes(wctx, &manager.WatchInterceptRoutesRequest{Namespaces: namespaces})
		if watchErr != nil {
			return nil, watchErr
		}
		return stream.Recv()
	}

	for _, tc := range []struct {
		name       string
		bearer     string
		namespaces []string
		code       codes.Code
	}{
		{name: "anonymous under permissive authentication", namespaces: []string{first, second}, code: codes.Unauthenticated},
		{name: "Kubernetes-issued token for a different audience", bearer: wrongAudienceToken, namespaces: []string{first, second}, code: codes.Unauthenticated},
		{name: "Kubernetes-issued default API token for the same observer", bearer: defaultAPIToken, namespaces: []string{first, second}, code: codes.Unauthenticated},
		{name: "chart-granted ordinary client", bearer: ordinaryClientToken, namespaces: []string{managers.ManagerNamespace}, code: codes.PermissionDenied},
		{name: "only the standard client port-forward grant", bearer: token, namespaces: []string{wrongGrant}, code: codes.PermissionDenied},
		{name: "an allowed namespace together with an unauthorized namespace", bearer: token, namespaces: []string{first, wrongGrant}, code: codes.PermissionDenied},
		{name: "no explicit scope", bearer: token, code: codes.InvalidArgument},
	} {
		snapshot, watchErr := receive(tc.bearer, tc.namespaces...)
		s.Require().Equal(tc.code, status.Code(watchErr), tc.name)
		s.Nil(snapshot, "%s must not return a partial snapshot or instance identifier", tc.name)
	}

	snapshot, err := receive(token, first, second, first)
	s.Require().NoError(err)
	s.Require().NotNil(snapshot)
	s.Empty(snapshot.GetRoutes(), "fresh private namespaces must produce a complete empty aggregate")
	s.Require().NoError(uuid.Validate(snapshot.GetManagerInstanceId()), "an empty snapshot still identifies its manager process")
	reopened, err := receive(token, second, first)
	s.Require().NoError(err)
	s.Require().NotNil(reopened)
	s.Empty(reopened.GetRoutes())
	s.Equal(snapshot.GetManagerInstanceId(), reopened.GetManagerInstanceId(), "reopening against the same manager must preserve its process identity")
}
