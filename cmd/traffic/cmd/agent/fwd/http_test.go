package fwd

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/v2/pkg/matcher"
)

func TestEnsureGRPCTrailersHeaderForReverseProxy(t *testing.T) {
	backendTe := make(chan string, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		backendTe <- request.Header.Get("Te")
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()

	targetURL, err := url.Parse(backend.URL)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	frontend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		proxy.ServeHTTP(writer, ensureGRPCTrailersHeader(request))
	}))
	defer frontend.Close()

	request, err := http.NewRequest(http.MethodPost, frontend.URL, nil)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/grpc")

	response, err := frontend.Client().Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	select {
	case got := <-backendTe:
		assert.Equal(t, "trailers", got)
	case <-time.After(time.Second):
		t.Fatal("backend did not receive the proxied gRPC request")
	}
}

func TestEnsureGRPCTrailersHeaderOnlyAddsForGRPC(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		te          string
		wantTe      string
		wantClone   bool
	}{
		{
			name:        "gRPC media type with suffix and parameters",
			contentType: "application/grpc+proto; charset=utf-8",
			wantTe:      "trailers",
			wantClone:   true,
		},
		{
			name:        "gRPC request that already advertises trailers",
			contentType: "application/grpc",
			te:          "trailers",
			wantTe:      "trailers",
		},
		{
			name:        "gRPC-web request",
			contentType: "application/grpc-web+proto",
		},
		{
			name:        "non-gRPC request",
			contentType: "application/json",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodPost, "http://example.com", nil)
			require.NoError(t, err)
			request.Header.Set("Content-Type", test.contentType)
			if test.te != "" {
				request.Header.Set("Te", test.te)
			}
			originalHeader := request.Header.Clone()

			got := ensureGRPCTrailersHeader(request)

			assert.Equal(t, test.wantTe, got.Header.Get("Te"))
			assert.Equal(t, originalHeader, request.Header)
			if test.wantClone {
				assert.NotSame(t, request, got)
			} else {
				assert.Same(t, request, got)
			}
		})
	}
}

func TestHTTPInterceptor_shouldInterceptRequest(t *testing.T) {
	headerFilters := map[string]string{
		"X-User-ID":     "dev123",
		"X-Environment": "staging",
	}
	pathFilters := []string{":path-prefix:/api/v1/", ":path-prefix:/admin/"}

	tests := []struct {
		name            string
		headers         map[string]string
		path            string
		shouldIntercept bool
	}{
		{
			name: "matching headers and path",
			headers: map[string]string{
				"X-User-ID":     "dev123",
				"X-Environment": "staging",
			},
			path:            "/api/v1/users",
			shouldIntercept: true,
		},
		{
			name: "matching headers but no path filters",
			headers: map[string]string{
				"X-User-ID":     "dev123",
				"X-Environment": "staging",
			},
			path:            "/some/other/path",
			shouldIntercept: false,
		},
		{
			name: "missing required header",
			headers: map[string]string{
				"X-User-ID": "dev123",
				// Missing X-Environment
			},
			path:            "/api/v1/users",
			shouldIntercept: false,
		},
		{
			name: "wrong header value",
			headers: map[string]string{
				"X-User-ID":     "prod456",
				"X-Environment": "staging",
			},
			path:            "/api/v1/users",
			shouldIntercept: false,
		},
		{
			name: "wildcard header match",
			headers: map[string]string{
				"X-User-ID":     "dev456",
				"X-Environment": "staging",
			},
			path:            "/api/v1/users",
			shouldIntercept: false, // Exact match required by default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, "http://example.com"+tt.path, nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			result := shouldInterceptRequest(req, headerFilters, pathFilters)
			assert.Equal(t, tt.shouldIntercept, result)
		})
	}
}

func TestHTTPInterceptor_matchesPattern(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		pattern string
		matches bool
	}{
		{"exact match", "dev123", "dev123", true},
		{"no match", "dev123", "prod456", false},
		{"wildcard match", "dev123", "dev.*", true},
		{"wildcard no match", "prod123", "dev.*", false},
		{"complex wildcard", "dev-user-123", "dev-.*-123", true},
		{"empty value", "", "dev*", false},
		{"empty pattern", "dev123", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matcher.NewValue(tt.pattern).Matches(tt.value)
			assert.Equal(t, tt.matches, result)
		})
	}
}

func TestHTTPInterceptor_noFilters(t *testing.T) {
	headerFilters := map[string]string{}
	pathFilters := []string{}

	req, _ := http.NewRequest(http.MethodGet, "http://example.com/any/path", nil)

	// No filters means intercept everything
	result := shouldInterceptRequest(req, headerFilters, pathFilters)
	assert.True(t, result)
}
