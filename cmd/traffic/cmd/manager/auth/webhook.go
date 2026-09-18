package auth

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"
	"unicode"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
)

const (
	webhookTimeout     = 5 * time.Second
	webhookMaxResponse = 1 << 20
)

// WebhookConfig configures a Kubernetes v1 TokenReview webhook for external
// clients. Its caller token authenticates the manager separately from the token
// sent in the TokenReview body. An empty CAFile uses the system trust store.
type WebhookConfig struct {
	URL             string
	Audiences       []string
	CAFile          string
	CallerTokenFile string
}

// WebhookReviewer uses the standard Kubernetes TokenReview webhook protocol.
type WebhookReviewer struct {
	url             string
	audiences       []string
	callerTokenFile string
	client          *http.Client
}

// NewWebhookReviewer validates the configuration and constructs an HTTPS-only
// reviewer. Errors never cause a fallback to the cluster's TokenReview API.
func NewWebhookReviewer(cfg WebhookConfig) (*WebhookReviewer, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("authentication webhook URL must be absolute HTTPS without credentials, query, or fragment")
	}
	if cfg.CallerTokenFile == "" {
		return nil, errors.New("authentication webhook requires a caller token file")
	}
	if len(cfg.Audiences) == 0 {
		return nil, errors.New("authentication webhook requires at least one audience")
	}
	for _, aud := range cfg.Audiences {
		if aud == "" || strings.IndexFunc(aud, unicode.IsSpace) >= 0 {
			return nil, errors.New("authentication webhook audiences must not be empty or contain whitespace")
		}
	}
	if _, err := readWebhookCallerToken(cfg.CallerTokenFile); err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read authentication webhook CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("authentication webhook CA does not contain a PEM certificate")
		}
		tlsConfig.RootCAs = pool
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &WebhookReviewer{
		url: u.String(), audiences: slices.Clone(cfg.Audiences), callerTokenFile: cfg.CallerTokenFile,
		client: &http.Client{
			Transport: transport,
			Timeout:   webhookTimeout,
			// Even same-origin redirects are not part of TokenReview; in particular,
			// never send either credential to a redirect target.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

func readWebhookCallerToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read authentication webhook caller token: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" || strings.ContainsAny(token, "\r\n\t ") {
		return "", errors.New("authentication webhook caller token file is empty or invalid")
	}
	return token, nil
}

// webhookTokenHasFutureExpiry never authenticates a token. A syntactically valid
// JWT exp is only used to make an existing positive webhook cache entry ineligible
// at expiry. Tokens without a trustworthy format are always checked by the webhook.
func webhookTokenHasFutureExpiry(token string, now time.Time) bool {
	payload := webhookJWTPayload(token)
	if payload == nil {
		return false
	}
	var claims struct {
		Exp json.Number `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return false
	}
	exp, err := claims.Exp.Int64()
	return err == nil && now.Unix() < exp
}

func webhookJWTPayload(token string) []byte {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	return payload
}

// A parseable disjoint JWT audience provides a reject-only credential disclosure
// guard. Opaque or unrecognized bearer forms are left to the configured webhook.
func webhookTokenHasDisjointAudience(token string, allowed []string) bool {
	payload := webhookJWTPayload(token)
	if payload == nil {
		return false
	}
	var claims struct {
		Aud json.RawMessage `json:"aud"`
	}
	if json.Unmarshal(payload, &claims) != nil {
		return false
	}
	var single string
	var audiences []string
	if json.Unmarshal(claims.Aud, &single) == nil && single != "" {
		audiences = []string{single}
	} else if json.Unmarshal(claims.Aud, &audiences) != nil || len(audiences) == 0 {
		return false
	}
	for _, aud := range audiences {
		if aud == "" {
			return false
		}
		if slices.Contains(allowed, aud) {
			return false
		}
	}
	return true
}

func (w *WebhookReviewer) review(ctx context.Context, review *authenticationv1.TokenReview) (*authenticationv1.TokenReview, error) {
	// Read on every cache miss so projected service-account and Secret tokens can
	// rotate without restarting the manager or retaining a stale credential.
	callerToken, err := readWebhookCallerToken(w.callerTokenFile)
	if err != nil {
		return nil, err
	}
	review.TypeMeta = metav1.TypeMeta{APIVersion: authenticationv1.SchemeGroupVersion.String(), Kind: "TokenReview"}
	data, err := json.Marshal(review)
	if err != nil {
		return nil, errors.New("encode authentication webhook request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("construct authentication webhook request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+callerToken)
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("authentication webhook request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Never print response bodies, which might echo a sensitive request.
		return nil, fmt.Errorf("authentication webhook returned HTTP %d", resp.StatusCode)
	}
	data, err = io.ReadAll(io.LimitReader(resp.Body, webhookMaxResponse+1))
	if err != nil || len(data) > webhookMaxResponse {
		return nil, errors.New("authentication webhook response unreadable or too large")
	}
	var result authenticationv1.TokenReview
	if json.Unmarshal(data, &result) != nil || result.APIVersion != authenticationv1.SchemeGroupVersion.String() || result.Kind != "TokenReview" {
		return nil, errors.New("authentication webhook returned an invalid v1 TokenReview")
	}
	if result.Status.Error != "" {
		return nil, errors.New("authentication webhook reported a review error")
	}
	if result.Status.Authenticated {
		if result.Status.User.Username == "" {
			return nil, errors.New("authentication webhook authenticated an empty username")
		}
		if result.Status.User.Username == user.Anonymous || slices.Contains(result.Status.User.Groups, user.AllUnauthenticated) {
			return nil, errors.New("authentication webhook returned an anonymous or unauthenticated identity")
		}
		accepted := make([]string, 0, len(result.Status.Audiences))
		for _, aud := range result.Status.Audiences {
			if slices.Contains(review.Spec.Audiences, aud) {
				accepted = append(accepted, aud)
			}
		}
		if len(accepted) == 0 {
			return nil, errors.New("authentication webhook returned no matching audience")
		}
		result.Status.Audiences = accepted
	}
	return &result, nil
}
