package auth

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

const (
	selfSubjectReviewPath    = "/apis/authentication.k8s.io/v1/selfsubjectreviews"
	maxSelfSubjectReviewSize = 1 << 20
	selfSubjectReviewTimeout = 4 * time.Second
)

type selfSubjectReviewer interface {
	review(context.Context, string) (*authenticator.Response, bool, error)
}

type delegatedSelfSubjectReviewer struct {
	endpoint string
	audience string
	tenant   string
	client   *http.Client
	valid    bool
}

// WithDelegatedSelfSubjectReview enables verification of Devbox identities
// with an explicitly trusted Kubernetes API proxy after local token rejection.
func WithDelegatedSelfSubjectReview(endpoint, audience, tenant string, authorities []string) Option {
	return func(a *Authenticator) {
		if endpoint != "" || audience != "" || tenant != "" || len(authorities) > 0 {
			r := newDelegatedSelfSubjectReviewer(endpoint, audience, tenant, http.DefaultTransport)
			if ValidateDelegatedDevboxProxyConfig(ModePermissive, endpoint, audience, tenant, authorities) != nil {
				r.valid = false
			}
			a.delegated = r
		}
	}
}

// ValidateDelegatedDevboxProxyConfig restricts bearer forwarding to a complete
// operator-supplied tenant, audience, endpoint, and exact authority allowlist.
func ValidateDelegatedDevboxProxyConfig(mode Mode, endpoint, audience, tenant string, authorities []string) error {
	if endpoint == "" && audience == "" && tenant == "" && len(authorities) == 0 {
		return nil
	}
	if mode != ModePermissive && mode != ModeEnforcing {
		return errors.New("requires authentication mode permissive or enforcing")
	}
	if endpoint == "" || audience == "" || tenant == "" || len(authorities) == 0 {
		return errors.New("requires an endpoint, canonical audience UUID, canonical tenant UUID, and an exact DNS authority allowlist together")
	}
	if !validCanonicalUUID(audience) || !validCanonicalUUID(tenant) {
		return errors.New("audience and tenant must be canonical nonzero UUIDs")
	}
	if len(authorities) > 32 {
		return errors.New("at most 32 DNS authorities may be trusted")
	}
	seen := make(map[string]struct{}, len(authorities))
	for _, authority := range authorities {
		_, ipErr := netip.ParseAddr(authority)
		if ipErr == nil || !strings.Contains(authority, ".") || len(validation.IsDNS1123Subdomain(authority)) != 0 {
			return errors.New("trusted authorities must be exact canonical lowercase fully qualified DNS names without ports")
		}
		if _, duplicate := seen[authority]; duplicate {
			return errors.New("trusted DNS authorities must be unique")
		}
		seen[authority] = struct{}{}
	}
	u, valid := parseDelegatedReviewEndpoint(endpoint)
	if !valid || (u.Host != u.Hostname() && u.Host != u.Hostname()+":443") || !validDevboxClusterReviewPath(u.Path) {
		return errors.New("requires a canonical HTTPS Devbox cluster SelfSubjectReview endpoint on port 443")
	}
	if _, allowed := seen[u.Hostname()]; !allowed {
		return errors.New("does not match any exact delegated proxy authority configured for that audience")
	}
	return nil
}

func validDevboxClusterReviewPath(path string) bool {
	const prefix = "/clusters/"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, selfSubjectReviewPath) {
		return false
	}
	cluster := strings.TrimSuffix(strings.TrimPrefix(path, prefix), selfSubjectReviewPath)
	if cluster == "" || len(cluster) > 63 || cluster[0] == '-' || cluster[len(cluster)-1] == '-' {
		return false
	}
	for _, ch := range cluster {
		if ch != '-' && (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') {
			return false
		}
	}
	return true
}

func parseDelegatedReviewEndpoint(endpoint string) (*url.URL, bool) {
	u, err := url.Parse(endpoint)
	valid := err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Opaque == "" && u.RawQuery == "" &&
		!u.ForceQuery && u.Fragment == "" && u.RawPath == "" && strings.HasSuffix(u.Path, selfSubjectReviewPath) &&
		!strings.Contains(endpoint, "#") && !strings.Contains(u.Path, "//") && !strings.Contains(u.Path, "/../") && !strings.Contains(u.Path, "/./")
	return u, valid
}

func newDelegatedSelfSubjectReviewer(endpoint, audience, tenant string, transport http.RoundTripper) *delegatedSelfSubjectReviewer {
	_, valid := parseDelegatedReviewEndpoint(endpoint)
	return &delegatedSelfSubjectReviewer{
		endpoint: endpoint,
		audience: audience,
		tenant:   tenant,
		valid:    valid,
		client: &http.Client{
			Transport: transport,
			Timeout:   selfSubjectReviewTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (r *delegatedSelfSubjectReviewer) review(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	tenant, object, eligible := devboxTokenIdentity(token, r.audience, r.tenant)
	if !eligible {
		return nil, false, nil
	}
	if !r.valid {
		return nil, false, errors.New("invalid delegated self-subject review endpoint")
	}
	const requestBody = `{"apiVersion":"authentication.k8s.io/v1","kind":"SelfSubjectReview","spec":{}}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, strings.NewReader(requestBody))
	if err != nil {
		return nil, false, errors.New("unable to construct delegated self-subject review")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		return nil, false, errors.New("delegated self-subject review transport unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, false, errors.New("delegated self-subject review returned an unexpected status")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSelfSubjectReviewSize+1))
	if err != nil || len(body) > maxSelfSubjectReviewSize {
		return nil, false, errors.New("unable to read delegated self-subject review")
	}
	var subject authenticationv1.SelfSubjectReview
	if err = json.Unmarshal(body, &subject); err != nil || subject.APIVersion != "authentication.k8s.io/v1" || subject.Kind != "SelfSubjectReview" {
		return nil, false, errors.New("invalid delegated self-subject review response")
	}
	identity := subject.Status.UserInfo
	if !validDevboxSubject(identity, tenant, object) {
		return nil, false, errors.New("delegated self-subject review did not verify a Devbox identity")
	}
	extra := make(map[string][]string, len(identity.Extra))
	for key, value := range identity.Extra {
		extra[key] = append([]string(nil), value...)
	}
	return &authenticator.Response{User: &user.DefaultInfo{
		Name: identity.Username, UID: identity.UID, Groups: append([]string(nil), identity.Groups...), Extra: extra,
	}}, true, nil
}

func validDevboxSubject(identity authenticationv1.UserInfo, tenant, object string) bool {
	parts := strings.Split(identity.Username, ":")
	if len(parts) != 4 || parts[0] != "aad" || parts[1] != "sp" || !validCanonicalUUID(parts[2]) ||
		!validCanonicalUUID(parts[3]) || parts[2] != tenant || parts[3] != object || identity.UID != parts[3] ||
		!slices.Contains(identity.Groups, user.AllAuthenticated) {
		return false
	}
	for _, group := range identity.Groups {
		if group != user.AllAuthenticated && !validCanonicalUUID(group) {
			return false
		}
	}
	for key, value := range identity.Extra {
		if key != "oid" || len(value) != 1 || value[0] != identity.UID {
			return false
		}
	}
	return true
}

// Unverified claims only determine whether this token may be sent to the trusted
// proxy and which identity its authenticated response must return.
func devboxTokenIdentity(token, requiredAudience, requiredTenant string) (tenant, object string, eligible bool) {
	if len(token) > 32<<10 || !validCanonicalUUID(requiredAudience) || !validCanonicalUUID(requiredTenant) {
		return "", "", false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", false
	}
	var claims struct {
		Audience string `json:"aud"`
		Issuer   string `json:"iss"`
		Tenant   string `json:"tid"`
		Object   string `json:"oid"`
	}
	if err = json.Unmarshal(body, &claims); err != nil || claims.Tenant != requiredTenant || !validCanonicalUUID(claims.Object) {
		return "", "", false
	}
	audience := strings.TrimPrefix(claims.Audience, "api://")
	if audience != requiredAudience {
		return "", "", false
	}
	if claims.Issuer != "https://sts.windows.net/"+requiredTenant+"/" &&
		claims.Issuer != "https://login.microsoftonline.com/"+requiredTenant+"/v2.0" {
		return "", "", false
	}
	return claims.Tenant, claims.Object, true
}

func validCanonicalUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}
