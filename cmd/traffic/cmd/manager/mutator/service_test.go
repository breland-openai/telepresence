package mutator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"
	admission "k8s.io/api/admission/v1"

	"github.com/telepresenceio/telepresence/v2/pkg/errcat"
)

func TestWebhookShutdownBeforeReady(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		listener := bufconn.Listen(1024)
		done := make(chan error, 1)
		go func() {
			done <- serveWebhook(ctx, &http.Server{}, listener, func() {
				t.Error("watchers started after cancellation")
			})
		}()
		synctest.Wait()
		cancel()
		require.NoError(t, <-done)
		_, err := listener.DialContext(t.Context())
		require.Error(t, err)
	})
}

func TestWebhookReturnsServeError(t *testing.T) {
	for _, tc := range []struct {
		name        string
		beforeReady bool
	}{
		{"before readiness", true},
		{"after readiness", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
				defer cancel()
				listener := bufconn.Listen(1024)
				if tc.beforeReady {
					require.NoError(t, listener.Close())
				}
				started := false
				err := serveWebhook(ctx, &http.Server{}, listener, func() {
					started = true
					_ = listener.Close()
				})
				_, listenErr := listener.Accept()
				require.Error(t, listenErr)
				require.ErrorIs(t, err, listenErr)
				require.NoError(t, ctx.Err())
				require.Equal(t, !tc.beforeReady, started)
			})
		})
	}
}

func TestWebhookShutdownAfterReady(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wantErr error
	}{
		{"request finishes", nil},
		{"drain deadline", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				type contextKey struct{}
				ctx, cancel := context.WithCancel(context.WithValue(t.Context(), contextKey{}, "manager"))
				defer cancel()
				handling := make(chan struct{})
				handled := make(chan struct{})
				finish := make(chan struct{})
				server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, "manager", r.Context().Value(contextKey{}))
					close(handling)
					defer close(handled)
					select {
					case <-finish:
						w.WriteHeader(http.StatusNoContent)
					case <-r.Context().Done():
					}
				})}
				listener := bufconn.Listen(1024)
				ready := make(chan struct{})
				done := make(chan error, 1)
				go func() {
					done <- serveWebhook(ctx, server, listener, func() { close(ready) })
				}()
				<-ready
				conn, err := listener.DialContext(ctx)
				require.NoError(t, err)
				defer conn.Close()
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				require.NoError(t, req.Write(conn))
				<-handling

				cancel()
				synctest.Wait()
				_, err = listener.DialContext(t.Context())
				require.Error(t, err, "the listener must be closed while the request drains")
				select {
				case <-handled:
					t.Fatal("request context canceled before the drain deadline")
				case err := <-done:
					t.Fatalf("returned with an active request: %v", err)
				default:
				}
				if tc.wantErr == nil {
					close(finish)
				}
				require.ErrorIs(t, <-done, tc.wantErr)
				<-handled
			})
		})
	}
}

type contextRecordingInjector struct {
	uninstallContext context.Context
}

func (*contextRecordingInjector) Inject(context.Context, *admission.AdmissionRequest) (PatchOps, error) {
	return nil, nil
}

func (i *contextRecordingInjector) Uninstall(ctx context.Context) {
	i.uninstallContext = ctx
}

func TestApplyMutatorError(t *testing.T) {
	t.Run("user error is admitted with a warning", func(t *testing.T) {
		// A User error must not deny the pod, otherwise the ReplicaSet controller retries the
		// creation indefinitely and wedges the workload's rollout.
		resp := &admission.AdmissionResponse{Allowed: true}
		err := errcat.User.Newf("invalid value %q for annotation %s", "bogus", "telepresence.getambassador.io/inject-traffic-agent")
		applyMutatorError(resp, err)
		assert.True(t, resp.Allowed, "pod creation must still be allowed for a user error")
		assert.Nil(t, resp.Result)
		assert.Equal(t, []string{err.Error()}, resp.Warnings)
	})

	t.Run("wrapped user error is still recognized", func(t *testing.T) {
		resp := &admission.AdmissionResponse{Allowed: true}
		err := fmt.Errorf("context: %w", errcat.User.New("nested user error"))
		applyMutatorError(resp, err)
		assert.True(t, resp.Allowed)
		assert.Len(t, resp.Warnings, 1)
	})

	t.Run("transient error denies so the create is retried", func(t *testing.T) {
		resp := &admission.AdmissionResponse{Allowed: true}
		err := errors.New("unable to get or generate agent config for workload echo.demo")
		applyMutatorError(resp, err)
		assert.False(t, resp.Allowed, "a transient error must deny so the pod creation is retried")
		assert.Nil(t, resp.Warnings)
		if assert.NotNil(t, resp.Result) {
			assert.Equal(t, err.Error(), resp.Result.Message)
		}
	})
}

func TestUninstallHandlerUsesManagerContext(t *testing.T) {
	type contextKey struct{}
	managerCtx := context.WithValue(context.Background(), contextKey{}, "manager")
	requestCtx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodDelete, "/uninstall", nil).WithContext(requestCtx)
	injector := &contextRecordingInjector{}

	recorder := httptest.NewRecorder()
	uninstallHandlerWithContext(managerCtx, injector, nil)(recorder, req)
	require.Equal(t, http.StatusOK, recorder.Code)
	require.NotNil(t, injector.uninstallContext)
	assert.NoError(t, injector.uninstallContext.Err())
	assert.Equal(t, "manager", injector.uninstallContext.Value(contextKey{}))
}
