package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
)

func testWebhookConfig(t *testing.T, handler http.Handler) (WebhookConfig, *httptest.Server) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	token := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600))
	require.NoError(t, os.WriteFile(token, []byte("manager-credential-v1"), 0o600))
	return WebhookConfig{URL: server.URL + "/review", Audiences: []string{"client-prod", "client-staging"}, CAFile: ca, CallerTokenFile: token}, server
}

func validWebhookStatus(auds ...string) authenticationv1.TokenReviewStatus {
	return authenticationv1.TokenReviewStatus{Authenticated: true, Audiences: auds, User: authenticationv1.UserInfo{
		Username: "alice", UID: "oid-alice", Groups: []string{"developers"},
		Extra: map[string]authenticationv1.ExtraValue{"example.test/claim": {"value"}},
	}}
}

func writeWebhookReview(t *testing.T, w http.ResponseWriter, status authenticationv1.TokenReviewStatus) {
	t.Helper()
	err := json.NewEncoder(w).Encode(authenticationv1.TokenReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "authentication.k8s.io/v1", Kind: "TokenReview"}, Status: status,
	})
	assert.NoError(t, err)
}

func webhookJWT(expiresAt int64) string {
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, expiresAt)))
	return "header." + payload + ".signature"
}

func TestWebhookProtocolCredentialRotationAndNativeIsolation(t *testing.T) {
	var calls atomic.Int32
	cfg, _ := testWebhookConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/review", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, fmt.Sprintf("Bearer manager-credential-v%d", call), r.Header.Get("Authorization"))
		var review authenticationv1.TokenReview
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&review)) {
			return
		}
		assert.Equal(t, "authentication.k8s.io/v1", review.APIVersion)
		assert.Equal(t, "TokenReview", review.Kind)
		assert.Equal(t, []string{"client-prod", "client-staging"}, review.Spec.Audiences)
		assert.Equal(t, fmt.Sprintf("opaque-user-token-%d", call), review.Spec.Token)
		writeWebhookReview(t, w, validWebhookStatus("unrequested", "client-staging"))
	}))
	w, err := NewWebhookReviewer(cfg)
	require.NoError(t, err)
	client := fake.NewClientset()
	var native atomic.Int32
	client.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		native.Add(1)
		review := action.(k8stesting.CreateAction).GetObject().(*authenticationv1.TokenReview)
		assert.Equal(t, []string{agentconfig.ManagerTokenAudience}, review.Spec.Audiences)
		assert.Equal(t, "native-agent-token", review.Spec.Token)
		return true, &authenticationv1.TokenReview{Status: validWebhookStatus(agentconfig.ManagerTokenAudience)}, nil
	})
	a := NewAuthenticator(client, WithExternalWebhook(w))
	for i := 1; i <= 2; i++ {
		require.NoError(t, os.WriteFile(cfg.CallerTokenFile, []byte(fmt.Sprintf("manager-credential-v%d\n", i)), 0o600))
		p, err := a.Authenticate(context.Background(), fmt.Sprintf("opaque-user-token-%d", i))
		require.NoError(t, err)
		assert.Equal(t, &Principal{Username: "alice", UID: "oid-alice", Groups: []string{"developers"}, Extra: map[string][]string{"example.test/claim": {"value"}}}, p)
	}
	assert.EqualValues(t, 2, calls.Load())
	assert.Zero(t, native.Load())
	p, err := a.AuthenticateManagerKubernetes(context.Background(), "native-agent-token")
	require.NoError(t, err)
	assert.Equal(t, "alice", p.Username)
	assert.EqualValues(t, 1, native.Load())
	assert.EqualValues(t, 2, calls.Load())
}

func TestWebhookFailuresNeverFallBack(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		want    string
		invalid bool
	}{
		{"denied", func(w http.ResponseWriter, _ *http.Request) {
			writeWebhookReview(t, w, authenticationv1.TokenReviewStatus{})
		}, "invalid bearer token", true},
		{"http401", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "never-echo-this-sensitive-body", http.StatusUnauthorized)
		}, "HTTP 401", false},
		{"http503", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "never-echo-this-sensitive-body", http.StatusServiceUnavailable)
		}, "HTTP 503", false},
		{"audience mismatch", func(w http.ResponseWriter, _ *http.Request) { writeWebhookReview(t, w, validWebhookStatus("wrong")) }, "no matching audience", false},
		{"missing audience", func(w http.ResponseWriter, _ *http.Request) { writeWebhookReview(t, w, validWebhookStatus()) }, "no matching audience", false},
		{"reported error", func(w http.ResponseWriter, _ *http.Request) {
			writeWebhookReview(t, w, authenticationv1.TokenReviewStatus{Error: "never-echo-this-sensitive-body"})
		}, "reported a review error", false},
		{"empty username", func(w http.ResponseWriter, _ *http.Request) {
			s := validWebhookStatus("client-prod")
			s.User.Username = ""
			writeWebhookReview(t, w, s)
		}, "empty username", false},
		{"anonymous username", func(w http.ResponseWriter, _ *http.Request) {
			s := validWebhookStatus("client-prod")
			s.User.Username = "system:anonymous"
			writeWebhookReview(t, w, s)
		}, "anonymous or unauthenticated", false},
		{"unauthenticated group", func(w http.ResponseWriter, _ *http.Request) {
			s := validWebhookStatus("client-prod")
			s.User.Groups = append(s.User.Groups, "system:unauthenticated")
			writeWebhookReview(t, w, s)
		}, "anonymous or unauthenticated", false},
		{"malformed", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"not":"a review"}`)) }, "invalid v1 TokenReview", false},
		{"oversized", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(strings.Repeat("x", webhookMaxResponse+1)))
		}, "too large", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := testWebhookConfig(t, tc.handler)
			w, err := NewWebhookReviewer(cfg)
			require.NoError(t, err)
			ci := fake.NewClientset()
			a := NewAuthenticator(ci, WithExternalWebhook(w))
			p, err := a.Authenticate(context.Background(), "user-token")
			require.ErrorContains(t, err, tc.want)
			if tc.invalid {
				require.ErrorIs(t, err, ErrInvalidToken)
			}
			assert.NotContains(t, err.Error(), "never-echo-this-sensitive-body")
			assert.Nil(t, p)
			assert.Empty(t, ci.Actions())
		})
	}
}

func TestWebhookTLSAndRedirectFailClosed(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { targetCalls.Add(1) }))
	defer target.Close()
	cfg, _ := testWebhookConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	w, err := NewWebhookReviewer(cfg)
	require.NoError(t, err)
	_, err = NewAuthenticator(fake.NewClientset(), WithExternalWebhook(w)).Authenticate(context.Background(), "secret")
	require.ErrorContains(t, err, "HTTP 307")
	assert.Zero(t, targetCalls.Load())
	cfg.CAFile = ""
	w, err = NewWebhookReviewer(cfg)
	require.NoError(t, err)
	_, err = NewAuthenticator(fake.NewClientset(), WithExternalWebhook(w)).Authenticate(context.Background(), "secret")
	require.ErrorContains(t, err, "certificate")
	for _, badURL := range []string{"http://example.com/review", "https://user:pass@example.com/review", "https://example.com/review?token=secret", "https://example.com/review#fragment"} {
		cfg.URL = badURL
		_, err = NewWebhookReviewer(cfg)
		require.ErrorContains(t, err, "HTTPS")
	}
}

func TestWebhookOnlyCachesVerifiedUnexpiredJWTs(t *testing.T) {
	var calls atomic.Int32
	var deny atomic.Bool
	cfg, _ := testWebhookConfig(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		if deny.Load() {
			writeWebhookReview(t, w, authenticationv1.TokenReviewStatus{})
			return
		}
		writeWebhookReview(t, w, validWebhookStatus("client-prod"))
	}))
	w, err := NewWebhookReviewer(cfg)
	require.NoError(t, err)
	a := NewAuthenticator(fake.NewClientset(), WithExternalWebhook(w))
	future := webhookJWT(time.Now().Unix() + 60)
	for range 2 {
		_, err = a.Authenticate(context.Background(), future)
		require.NoError(t, err)
	}
	assert.EqualValues(t, 1, calls.Load(), "verified unexpired JWT is cached")
	for range 2 {
		_, err = a.Authenticate(context.Background(), "opaque")
		require.NoError(t, err)
	}
	assert.EqualValues(t, 3, calls.Load(), "opaque tokens are never positively cached")
	past := webhookJWT(time.Now().Unix() - 1)
	for range 2 {
		_, err = a.Authenticate(context.Background(), past)
		require.NoError(t, err)
	}
	assert.EqualValues(t, 5, calls.Load(), "unverified expiry can only disable caching; the reviewer decides")
	deny.Store(true)
	for range 2 {
		_, err = a.Authenticate(context.Background(), webhookJWT(time.Now().Unix()+600))
		require.ErrorIs(t, err, ErrInvalidToken)
	}
	assert.EqualValues(t, 6, calls.Load(), "future exp never grants authentication and failures are cached")
	assert.True(t, webhookTokenHasFutureExpiry(future, time.Now()))
	assert.False(t, webhookTokenHasFutureExpiry(future, time.Now().Add(2*time.Minute)))
}

func TestWebhookRejectsDisjointJWTAudienceWithoutDisclosingToken(t *testing.T) {
	var calls atomic.Int32
	cfg, _ := testWebhookConfig(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeWebhookReview(t, w, validWebhookStatus("client-staging"))
	}))
	w, err := NewWebhookReviewer(cfg)
	require.NoError(t, err)
	ci := fake.NewClientset()
	a := NewAuthenticator(ci, WithExternalWebhook(w))
	jwt := func(aud any) string {
		payload, err := json.Marshal(map[string]any{"aud": aud, "exp": time.Now().Unix() + 600})
		require.NoError(t, err)
		return "header." + base64.RawURLEncoding.EncodeToString(payload) + ".signature"
	}
	for _, aud := range []any{"unrelated-prod", []string{"unrelated-prod", "another-prod"}} {
		p, err := a.Authenticate(context.Background(), jwt(aud))
		require.ErrorIs(t, err, ErrInvalidToken)
		require.Nil(t, p)
	}
	require.Zero(t, calls.Load(), "never send the original JWT to the wrong configured webhook")
	require.Empty(t, ci.Actions(), "do not fall back to native Kubernetes")
	for _, aud := range []any{"client-staging", []string{"unrelated-prod", "client-staging"}} {
		_, err := a.Authenticate(context.Background(), jwt(aud))
		require.NoError(t, err)
	}
	require.EqualValues(t, 2, calls.Load(), "matching unverified aud still always needs webhook verification")
}
