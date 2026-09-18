package manager

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/auth"
	"github.com/telepresenceio/telepresence/v2/cmd/traffic/cmd/manager/managerutil"
)

func TestServeExternal_RefusesMissingCertDirInEveryInternalMode(t *testing.T) {
	for _, mode := range []auth.Mode{auth.ModeDisabled, auth.ModePermissive, auth.ModeEnforcing} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := managerutil.WithEnv(context.Background(), &managerutil.Env{ExternalPort: 8443, AuthenticationMode: mode})
			err := serveExternal(ctx, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "EXTERNAL_TLS_CERT_DIR")
		})
	}
}

func TestServeExternal_RefusesInvalidWebhookInPermissiveMode(t *testing.T) {
	ctx := managerutil.WithEnv(context.Background(), &managerutil.Env{
		ExternalPort: 8443, ExternalTLSCertDir: "/some/tls", AuthenticationMode: auth.ModePermissive,
		ExternalAuthWebhookURL: "http://invalid.example.test/review",
	})
	err := serveExternal(ctx, nil)
	require.ErrorContains(t, err, "HTTPS")

	ctx = managerutil.WithEnv(context.Background(), &managerutil.Env{
		ExternalPort: 8443, ExternalTLSCertDir: "/some/tls", AuthenticationMode: auth.ModePermissive,
		ExternalAuthWebhookAudiences: []string{"missing-url"},
	})
	err = serveExternal(ctx, nil)
	require.ErrorContains(t, err, "EXTERNAL_AUTH_WEBHOOK_URL")
}
