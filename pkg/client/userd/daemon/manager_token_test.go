package daemon

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/telepresenceio/telepresence/v2/pkg/client/k8s"
)

func TestManagerTokenCallbackCapabilityIsPerConnectionAndRotates(t *testing.T) {
	svc := &service{}
	dir := t.TempDir()
	first := filepath.Join(dir, "first")
	second := filepath.Join(dir, "second")
	require.NoError(t, os.WriteFile(first, []byte("first-credential"), 0o600))
	require.NoError(t, os.WriteFile(second, []byte("second-credential"), 0o600))
	firstCap, revoke, err := svc.RegisterManagerTokenCallback(t.Context(), first)
	require.NoError(t, err)
	secondCap, revokeSecond, err := svc.RegisterManagerTokenCallback(t.Context(), second)
	require.NoError(t, err)
	t.Cleanup(revokeSecond)
	require.Len(t, firstCap, k8s.ManagerTokenCallbackCapabilitySize)
	require.NotEqual(t, firstCap, secondCap)
	token, err := svc.ManagerToken(t.Context(), firstCap, "", "")
	require.NoError(t, err)
	require.Equal(t, "first-credential", token)
	_, err = svc.ManagerToken(t.Context(), firstCap, "test-pod", "test-audience")
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	token, err = svc.ManagerToken(t.Context(), secondCap, "", "")
	require.NoError(t, err)
	require.Equal(t, "second-credential", token)
	for _, unknown := range [][]byte{nil, []byte(first), bytes.Repeat([]byte{'x'}, k8s.ManagerTokenCallbackCapabilitySize)} {
		_, err = svc.ManagerToken(t.Context(), unknown, "", "")
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	require.NoError(t, os.WriteFile(first, []byte("private invalid\ncontent"), 0o600))
	_, err = svc.ManagerToken(t.Context(), firstCap, "", "")
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.NotContains(t, err.Error(), "private")
	require.NotContains(t, err.Error(), first)
	require.NoError(t, os.WriteFile(first, []byte("first-rotated"), 0o600))
	token, err = svc.ManagerToken(t.Context(), firstCap, "", "")
	require.NoError(t, err)
	require.Equal(t, "first-rotated", token)
	revoke()
	revoke()
	_, err = svc.ManagerToken(t.Context(), firstCap, "", "")
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	token, err = svc.ManagerToken(t.Context(), secondCap, "", "")
	require.NoError(t, err)
	require.Equal(t, "second-credential", token)
	_, _, err = svc.RegisterManagerTokenCallback(t.Context(), filepath.Join(dir, "missing"))
	require.Error(t, err)
	require.NotContains(t, err.Error(), dir)
}

func TestManagerTokenCallbackNegotiatedScopeAndRevocation(t *testing.T) {
	svc := &service{}
	var calls int
	var receivedPod, receivedAudience string
	credential := "test-first"
	var providerErr error
	capability, revoke, err := svc.RegisterNegotiatedManagerTokenCallback(func(_ context.Context, pod, audience string) (string, error) {
		calls++
		receivedPod, receivedAudience = pod, audience
		return credential, providerErr
	})
	require.NoError(t, err)
	t.Cleanup(revoke)
	require.Zero(t, calls, "registration cannot request credentials before a manager has advertised")
	for _, scope := range [][2]string{{"", ""}, {"test-pod", ""}, {"", "test-audience"}} {
		_, err = svc.ManagerToken(t.Context(), capability, scope[0], scope[1])
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	require.Zero(t, calls)
	value, err := svc.ManagerToken(t.Context(), capability, "test-first-pod", "test-staging-audience")
	require.NoError(t, err)
	require.Equal(t, credential, value)
	require.Equal(t, "test-first-pod", receivedPod)
	require.Equal(t, "test-staging-audience", receivedAudience)
	credential = "test-rotated"
	value, err = svc.ManagerToken(t.Context(), capability, "test-second-pod", "test-production-audience")
	require.NoError(t, err)
	require.Equal(t, credential, value)
	require.Equal(t, "test-second-pod", receivedPod)
	require.Equal(t, "test-production-audience", receivedAudience)
	providerErr = errors.New("private provider credential or diagnostic")
	value, err = svc.ManagerToken(t.Context(), capability, receivedPod, receivedAudience)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Empty(t, value)
	require.NotContains(t, err.Error(), "private")
	require.Equal(t, 3, calls)
	revoke()
	_, err = svc.ManagerToken(t.Context(), capability, receivedPod, receivedAudience)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Equal(t, 3, calls)
	_, _, err = svc.RegisterNegotiatedManagerTokenCallback(nil)
	require.Error(t, err)
}
