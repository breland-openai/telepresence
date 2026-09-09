package auth

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
)

const (
	delegatedTestObject   = "c966015a-cd5f-4d6f-b096-2f8a575770a1"
	devboxEntraTenant     = "33333333-3333-4333-8333-333333333333"
	devboxProxyProduction = "22222222-2222-4222-8222-222222222222"
	devboxProxyStaging    = "11111111-1111-4111-8111-111111111111"
)

func devboxTestToken(t *testing.T, changes map[string]string) string {
	t.Helper()
	claims := map[string]string{"aud": "api://" + devboxProxyProduction, "iss": "https://sts.windows.net/" + devboxEntraTenant + "/", "tid": devboxEntraTenant, "oid": delegatedTestObject}
	for key, value := range changes {
		claims[key] = value
	}
	body, err := json.Marshal(claims)
	require.NoError(t, err)
	return "header." + base64.RawURLEncoding.EncodeToString(body) + ".untrusted-signature-checked-only-by-the-delegate"
}

func devboxTestIdentity() authenticationv1.UserInfo {
	return authenticationv1.UserInfo{
		Username: "aad:sp:" + devboxEntraTenant + ":" + delegatedTestObject,
		UID:      delegatedTestObject,
		Groups:   []string{user.AllAuthenticated, "11111111-2222-3333-4444-555555555555"},
		Extra:    map[string]authenticationv1.ExtraValue{"oid": {delegatedTestObject}},
	}
}

func respondDevboxIdentity(t *testing.T, w http.ResponseWriter, identity authenticationv1.UserInfo) {
	t.Helper()
	body, err := json.Marshal(&authenticationv1.SelfSubjectReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "authentication.k8s.io/v1", Kind: "SelfSubjectReview"},
		Status:   authenticationv1.SelfSubjectReviewStatus{UserInfo: identity},
	})
	require.NoError(t, err)
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(body)
}

func testDelegatedAuthenticator(ci *fake.Clientset, server *httptest.Server) *Authenticator {
	return NewAuthenticator(ci, func(a *Authenticator) {
		a.delegated = newDelegatedSelfSubjectReviewer(server.URL+"/clusters/staging"+selfSubjectReviewPath, devboxProxyProduction, devboxEntraTenant, server.Client().Transport)
	})
}

func TestDelegatedReviewKeepsExactIdentityGroupsAndCombinedCache(t *testing.T) {
	var network, local atomic.Int32
	token := devboxTestToken(t, nil)
	identity := devboxTestIdentity()
	for range 688 {
		identity.Groups = append(identity.Groups, uuid.NewString())
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		network.Add(1)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/clusters/staging"+selfSubjectReviewPath, r.URL.Path)
		assert.Equal(t, "Bearer "+token, r.Header.Get("Authorization"))
		assert.Empty(t, r.Header.Get("Impersonate-User"))
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var request authenticationv1.SelfSubjectReview
		require.NoError(t, json.Unmarshal(body, &request))
		assert.Equal(t, "SelfSubjectReview", request.Kind)
		assert.Empty(t, request.Status.UserInfo.Username)
		respondDevboxIdentity(t, w, identity)
	}))
	t.Cleanup(server.Close)
	ci := fake.NewClientset()
	k8sapi.InstallFakeTokenReviews(ci, func(_ string, _ []string) *authenticationv1.TokenReviewStatus {
		local.Add(1)
		return &authenticationv1.TokenReviewStatus{}
	})
	a := testDelegatedAuthenticator(ci, server)
	for range 2 {
		p, err := a.Authenticate(t.Context(), token)
		require.NoError(t, err)
		require.NotNil(t, p)
		assert.Equal(t, identity.Username, p.Username)
		assert.Equal(t, identity.UID, p.UID)
		assert.Equal(t, identity.Groups, p.Groups)
		assert.Equal(t, []string{delegatedTestObject}, p.Extra["oid"])
		assert.Empty(t, p.PodUID)
	}
	assert.EqualValues(t, 2, local.Load())
	assert.EqualValues(t, 1, network.Load())
}

func TestDelegatedReviewNeverReceivesLocallyTrustedOrUnrelatedCredentials(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(server.Close)
	token := devboxTestToken(t, nil)
	ci := fake.NewClientset()
	k8sapi.InstallFakeTokenReviews(ci, func(presented string, _ []string) *authenticationv1.TokenReviewStatus {
		if presented == token {
			return cacheTestStatus()
		}
		return &authenticationv1.TokenReviewStatus{}
	})
	a := testDelegatedAuthenticator(ci, server)
	p, err := a.Authenticate(t.Context(), token)
	require.NoError(t, err)
	assert.Equal(t, "user", p.Username)
	for _, value := range []string{
		"opaque-secret", "header.broken.signature", devboxTestToken(t, map[string]string{"aud": "6dae42f8-4368-4678-94ff-3960e28e3630"}),
		devboxTestToken(t, map[string]string{"iss": "https://attacker.invalid/"}), devboxTestToken(t, map[string]string{"tid": uuid.NewString()}),
		devboxTestToken(t, map[string]string{"oid": "missing"}), strings.Repeat("s", 33000),
	} {
		p, err = a.Authenticate(t.Context(), value)
		require.ErrorIs(t, err, ErrInvalidToken)
		assert.Nil(t, p)
	}
	assert.Zero(t, calls.Load())
}

func TestDelegatedReviewOnlyForwardsItsConfiguredEnvironmentAudience(t *testing.T) {
	for _, tc := range []struct{ name, allowed, rejected string }{
		{"staging", devboxProxyStaging, devboxProxyProduction},
		{"production", devboxProxyProduction, devboxProxyStaging},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _, eligible := devboxTokenIdentity(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), tc.allowed, devboxEntraTenant)
				assert.True(t, eligible)
				respondDevboxIdentity(t, w, devboxTestIdentity())
			}))
			t.Cleanup(server.Close)
			r := newDelegatedSelfSubjectReviewer(server.URL+"/clusters/staging"+selfSubjectReviewPath, tc.allowed, devboxEntraTenant, server.Client().Transport)
			for _, aud := range []string{tc.rejected, "api://" + tc.rejected, "unknown-audience"} {
				identity, ok, err := r.review(t.Context(), devboxTestToken(t, map[string]string{"aud": aud}))
				require.NoError(t, err)
				require.False(t, ok)
				require.Nil(t, identity)
			}
			require.Zero(t, calls.Load(), "the other environment's bearer must never be forwarded")
			for _, aud := range []string{tc.allowed, "api://" + tc.allowed} {
				identity, ok, err := r.review(t.Context(), devboxTestToken(t, map[string]string{"aud": aud}))
				require.NoError(t, err)
				require.True(t, ok)
				require.NotNil(t, identity)
			}
			require.EqualValues(t, 2, calls.Load())
		})
	}
	for _, audience := range []string{devboxProxyStaging, devboxProxyProduction} {
		_, _, eligible := devboxTokenIdentity(devboxTestToken(t, map[string]string{"aud": audience}), "", devboxEntraTenant)
		require.False(t, eligible, "there is no implicit audience when the operator has not configured one")
		_, _, eligible = devboxTokenIdentity(devboxTestToken(t, map[string]string{"aud": audience}), audience, "")
		require.False(t, eligible, "there is no implicit tenant when the operator has not configured one")
	}
}

func TestDelegatedReviewClaimsRequireTheExactConfiguredTenantAndIssuer(t *testing.T) {
	for _, issuer := range []string{
		"https://sts.windows.net/" + devboxEntraTenant + "/",
		"https://login.microsoftonline.com/" + devboxEntraTenant + "/v2.0",
	} {
		token := devboxTestToken(t, map[string]string{"iss": issuer})
		tenant, object, eligible := devboxTokenIdentity(token, devboxProxyProduction, devboxEntraTenant)
		require.True(t, eligible)
		require.Equal(t, devboxEntraTenant, tenant)
		require.Equal(t, delegatedTestObject, object)
		_, _, eligible = devboxTokenIdentity(token, devboxProxyProduction, devboxProxyStaging)
		require.False(t, eligible)
	}
	for _, issuer := range []string{
		"https://sts.windows.net/" + devboxProxyStaging + "/",
		"https://login.microsoftonline.com/" + devboxProxyStaging + "/v2.0",
		"https://login.microsoftonline.com/" + devboxEntraTenant + "/v2.0/",
		"https://login.microsoftonline.com.attacker.test/" + devboxEntraTenant + "/v2.0",
	} {
		token := devboxTestToken(t, map[string]string{"iss": issuer})
		_, _, eligible := devboxTokenIdentity(token, devboxProxyProduction, devboxEntraTenant)
		require.False(t, eligible)
	}
}

func TestDelegatedReviewOptionSetsAudienceAndFailsClosedForWrongAuthority(t *testing.T) {
	const endpoint = "https://staging.proxy.example.test/clusters/staging" + selfSubjectReviewPath
	for _, tc := range []struct {
		audience, tenant string
		authorities      []string
		valid            bool
	}{
		{},
		{audience: devboxProxyStaging, tenant: devboxEntraTenant, authorities: []string{"staging.proxy.example.test"}, valid: true},
		{audience: devboxProxyProduction, tenant: devboxEntraTenant, authorities: []string{"production.proxy.example.test"}},
		{audience: devboxProxyStaging, authorities: []string{"staging.proxy.example.test"}},
	} {
		a := &Authenticator{}
		WithDelegatedSelfSubjectReview(endpoint, tc.audience, tc.tenant, tc.authorities)(a)
		r, ok := a.delegated.(*delegatedSelfSubjectReviewer)
		require.True(t, ok)
		require.Equal(t, tc.audience, r.audience)
		require.Equal(t, tc.tenant, r.tenant)
		require.Equal(t, tc.valid, r.valid)
	}
	a := &Authenticator{}
	WithDelegatedSelfSubjectReview("", "", "", nil)(a)
	require.Nil(t, a.delegated)
}

func TestValidateDelegatedDevboxProxyConfigPairsOnlyConfiguredAuthorities(t *testing.T) {
	const stagingHost = "staging.proxy.example.test"
	const stagingAlternate = "staging-alternate.proxy.example.test"
	const prodHost = "production.proxy.example.test"
	const prodAlternate = "production-alternate.proxy.example.test"
	endpoint := func(host string) string { return "https://" + host + "/clusters/staging" + selfSubjectReviewPath }
	authorities := func(audience string) []string {
		if audience == devboxProxyProduction {
			return []string{prodHost, prodAlternate}
		}
		return []string{stagingHost, stagingAlternate}
	}
	for _, tc := range []struct{ host, audience string }{
		{stagingHost, devboxProxyStaging},
		{stagingAlternate, devboxProxyStaging},
		{stagingHost + ":443", devboxProxyStaging},
		{prodHost, devboxProxyProduction},
		{prodAlternate, devboxProxyProduction},
	} {
		for _, mode := range []Mode{ModePermissive, ModeEnforcing} {
			require.NoError(t, ValidateDelegatedDevboxProxyConfig(mode, endpoint(tc.host), tc.audience, devboxEntraTenant, authorities(tc.audience)))
		}
	}
	for _, tc := range []struct{ name, endpoint, audience string }{
		{"missing endpoint", "", devboxProxyStaging},
		{"unknown audience", endpoint(stagingHost), "unknown-audience"},
		{"noncanonical audience", endpoint(stagingHost), "api://" + devboxProxyStaging},
		{"staging sent to prod", endpoint(prodHost), devboxProxyStaging},
		{"production sent to staging", endpoint(stagingHost), devboxProxyProduction},
		{"untrusted host", endpoint("proxy.attacker.invalid"), devboxProxyStaging},
		{"suffix attack", endpoint(stagingHost + ".attacker.invalid"), devboxProxyStaging},
		{"nondefault port", endpoint(stagingHost + ":8443"), devboxProxyStaging},
		{"plaintext", strings.Replace(endpoint(stagingHost), "https:", "http:", 1), devboxProxyStaging},
		{"fragment", endpoint(stagingHost) + "#x", devboxProxyStaging},
		{"empty fragment", endpoint(stagingHost) + "#", devboxProxyStaging},
		{"query", endpoint(stagingHost) + "?x=y", devboxProxyStaging},
		{"empty query", endpoint(stagingHost) + "?", devboxProxyStaging},
		{"uppercase host", endpoint(strings.ToUpper(stagingHost)), devboxProxyStaging},
		{"trailing dot", endpoint(stagingHost + "."), devboxProxyStaging},
		{"zero-padded port", endpoint(stagingHost + ":0443"), devboxProxyStaging},
		{"no cluster", "https://" + stagingHost + selfSubjectReviewPath, devboxProxyStaging},
		{"nested cluster", "https://" + stagingHost + "/clusters/staging/extra" + selfSubjectReviewPath, devboxProxyStaging},
		{"invalid cluster", "https://" + stagingHost + "/clusters/-staging" + selfSubjectReviewPath, devboxProxyStaging},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, ValidateDelegatedDevboxProxyConfig(ModePermissive, tc.endpoint, tc.audience, devboxEntraTenant, authorities(tc.audience)))
		})
	}
	for _, wrong := range [][]string{nil, {stagingHost, stagingHost}, {"*.example.test"}, {"example.test:443"}, {"EXAMPLE.TEST"}, {"example.test."}, {"127.0.0.1"}, {"::1"}, {"localhost"}, {"proxy.invalid/path"}} {
		require.Error(t, ValidateDelegatedDevboxProxyConfig(ModePermissive, endpoint(stagingHost), devboxProxyStaging, devboxEntraTenant, wrong))
	}
	for _, tenant := range []string{"", "incorrect", "00000000-0000-0000-0000-000000000000"} {
		require.Error(t, ValidateDelegatedDevboxProxyConfig(ModePermissive, endpoint(stagingHost), devboxProxyStaging, tenant, authorities(devboxProxyStaging)))
	}
	require.ErrorContains(t, ValidateDelegatedDevboxProxyConfig(ModeDisabled, endpoint(stagingHost), devboxProxyStaging, devboxEntraTenant, authorities(devboxProxyStaging)), "authentication")
	require.ErrorContains(t, ValidateDelegatedDevboxProxyConfig("", endpoint(stagingHost), devboxProxyStaging, devboxEntraTenant, authorities(devboxProxyStaging)), "authentication")
	require.Error(t, ValidateDelegatedDevboxProxyConfig(ModePermissive, "https://generic.example"+selfSubjectReviewPath, "", "", nil))
	require.NoError(t, ValidateDelegatedDevboxProxyConfig(ModeDisabled, "", "", "", nil))
}

func TestDelegatedReviewNeverMasksLocalReviewOutage(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		respondDevboxIdentity(t, w, devboxTestIdentity())
	}))
	t.Cleanup(server.Close)
	ci := fake.NewClientset()
	failure := errors.New("local API unavailable")
	ci.PrependReactor("create", "tokenreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, failure
	})
	a := testDelegatedAuthenticator(ci, server)
	p, err := a.Authenticate(t.Context(), devboxTestToken(t, nil))
	require.ErrorIs(t, err, failure)
	assert.Nil(t, p)
	assert.Zero(t, calls.Load())
}

func TestDelegatedReviewRejectsNoncanonicalOrEscalatedSubjects(t *testing.T) {
	cases := map[string]func(*authenticationv1.UserInfo){
		"anonymous":       func(i *authenticationv1.UserInfo) { i.Username = user.Anonymous },
		"service account": func(i *authenticationv1.UserInfo) { i.Username = "system:serviceaccount:namespace:name" },
		"different tenant": func(i *authenticationv1.UserInfo) {
			i.Username = "aad:sp:" + uuid.NewString() + ":" + delegatedTestObject
		},
		"different object": func(i *authenticationv1.UserInfo) {
			i.UID = uuid.NewString()
			i.Username = "aad:sp:" + devboxEntraTenant + ":" + i.UID
		},
		"uid mismatch":           func(i *authenticationv1.UserInfo) { i.UID = uuid.NewString() },
		"no authenticated group": func(i *authenticationv1.UserInfo) { i.Groups = i.Groups[1:] },
		"masters group":          func(i *authenticationv1.UserInfo) { i.Groups = append(i.Groups, user.SystemPrivilegedGroup) },
		"unknown extra":          func(i *authenticationv1.UserInfo) { i.Extra["pod-uid"] = []string{uuid.NewString()} },
		"oid mismatch":           func(i *authenticationv1.UserInfo) { i.Extra["oid"] = []string{uuid.NewString()} },
	}
	for name, update := range cases {
		t.Run(name, func(t *testing.T) {
			identity := devboxTestIdentity()
			update(&identity)
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				respondDevboxIdentity(t, w, identity)
			}))
			t.Cleanup(server.Close)
			r := newDelegatedSelfSubjectReviewer(server.URL+selfSubjectReviewPath, devboxProxyProduction, devboxEntraTenant, server.Client().Transport)
			p, ok, err := r.review(t.Context(), devboxTestToken(t, nil))
			require.ErrorContains(t, err, "did not verify")
			assert.Nil(t, p)
			assert.False(t, ok)
		})
	}
}

func TestDelegatedReviewRejectsRedirectsMalformedDataAndUnavailableEndpoints(t *testing.T) {
	var redirectCalls atomic.Int32
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectCalls.Add(1)
		respondDevboxIdentity(t, w, devboxTestIdentity())
	}))
	t.Cleanup(redirect.Close)
	cases := []struct {
		name string
		code int
		body string
		err  bool
	}{
		{"redirect", http.StatusTemporaryRedirect, "", true},
		{"unauthorized", http.StatusUnauthorized, "sensitive-response", false},
		{"forbidden", http.StatusForbidden, "sensitive-response", false},
		{"outage", http.StatusServiceUnavailable, "sensitive-response", true},
		{"malformed", http.StatusOK, "sensitive-response", true},
		{"too large", http.StatusOK, strings.Repeat("s", maxSelfSubjectReviewSize+1), true},
		{"wrong object", http.StatusOK, `{"kind":"TokenReview","apiVersion":"authentication.k8s.io/v1"}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", redirect.URL+selfSubjectReviewPath)
				w.WriteHeader(tc.code)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			t.Cleanup(server.Close)
			r := newDelegatedSelfSubjectReviewer(server.URL+selfSubjectReviewPath, devboxProxyProduction, devboxEntraTenant, server.Client().Transport)
			token := devboxTestToken(t, nil)
			p, ok, err := r.review(t.Context(), token)
			if tc.err {
				require.Error(t, err)
				assert.NotContains(t, err.Error(), "sensitive-response")
				assert.NotContains(t, err.Error(), token)
			} else {
				require.NoError(t, err)
			}
			assert.Nil(t, p)
			assert.False(t, ok)
		})
	}
	assert.Zero(t, redirectCalls.Load())
	for _, endpoint := range []string{
		"http://proxy.invalid" + selfSubjectReviewPath, "https://user@proxy.invalid" + selfSubjectReviewPath,
		"https://proxy.invalid/other", "https://proxy.invalid" + selfSubjectReviewPath + "?token=secret", "https://proxy.invalid/a/../" + selfSubjectReviewPath[1:],
	} {
		r := newDelegatedSelfSubjectReviewer(endpoint, devboxProxyProduction, devboxEntraTenant, http.DefaultTransport)
		_, _, err := r.review(context.Background(), devboxTestToken(t, nil))
		require.ErrorContains(t, err, "invalid delegated")
	}
}
