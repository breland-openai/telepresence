package fwd

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

type httpGRPCDisconnectStream struct {
	fakeTapStream
	id      tunnel.ConnID
	code    codes.Code
	ack     atomic.Bool
	once    sync.Once
	written chan struct{}
	notify  chan<- struct{}
	release <-chan struct{}
}

func (s *httpGRPCDisconnectStream) ID() tunnel.ConnID { return s.id }

func (s *httpGRPCDisconnectStream) Receive(ctx context.Context) (tunnel.Message, error) {
	if !s.ack.Swap(true) {
		return tunnel.NewMessage(tunnel.DialOK, nil), nil
	}
	select {
	case <-ctx.Done():
	case <-s.written:
		if s.release != nil {
			select {
			case <-s.release:
			case <-ctx.Done():
			}
		}
	}
	return nil, status.Error(s.code, "inner tunnel transport terminated")
}

func (s *httpGRPCDisconnectStream) Send(_ context.Context, message tunnel.Message) error {
	if message.Code() == tunnel.Normal && len(message.Payload()) > 0 {
		s.once.Do(func() {
			close(s.written)
			if s.notify != nil {
				select {
				case s.notify <- struct{}{}:
				default:
				}
			}
		})
	}
	return nil
}

func installHTTPGRPCFailure(f *tcp, backend *httptest.Server, code codes.Code, durable bool, notify chan<- struct{}, release <-chan struct{}) {
	guard := RouteGuard{RoutingKey: "developer", InterceptID: "client:web"}
	if durable {
		guard.Incarnation = "current"
		f.SetRouteGuards([]RouteGuard{guard}, nil)
	}
	f.SetIntercepting([]*manager.InterceptInfo{mediatedRuntime(backend, guard, false)})
	f.SetStreamProvider(&fakeStreamProvider{createFor: func(_ context.Context, id tunnel.ConnID) (tunnel.Stream, error) {
		return &httpGRPCDisconnectStream{id: id, code: code, written: make(chan struct{}), notify: notify, release: release}, nil
	}})
}

func TestPermanentMediationInnerGRPCFailureBeforeHTTPHeaders(t *testing.T) {
	for _, test := range []struct {
		code    codes.Code
		durable bool
		want    int
	}{
		{codes.Canceled, true, http.StatusServiceUnavailable},
		{codes.Unavailable, true, http.StatusServiceUnavailable},
		{codes.Canceled, false, http.StatusBadGateway},
		{codes.Unavailable, false, http.StatusBadGateway},
		{codes.DeadlineExceeded, true, http.StatusBadGateway},
		{codes.PermissionDenied, true, http.StatusBadGateway},
		{codes.Internal, true, http.StatusBadGateway},
	} {
		for _, upgrade := range []bool{false, true} {
			name := test.code.String()
			if test.durable {
				name += "-protected"
			}
			if upgrade {
				name += "-websocket"
			}
			t.Run(name, func(t *testing.T) {
				staging, stagingCalls := mediatedBackend(t, "staging", false)
				f, address := startMediatedForwarder(t, staging, false)
				notify := make(chan struct{}, 1)
				installHTTPGRPCFailure(f, staging, test.code, test.durable, notify, nil)
				conn, err := net.Dial("tcp", address.String())
				require.NoError(t, err)
				defer conn.Close()
				require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
				reader := bufio.NewReader(conn)
				mediatedResponse(t, mediatedH1Request(t, conn, reader, "unrelated", false), http.StatusOK, "staging:unrelated")
				response := mediatedH1Request(t, conn, reader, "developer", upgrade)
				select {
				case <-notify:
				default:
					t.Fatal("inner tunnel failed before the agent sent the selected HTTP request after DialOK")
				}
				if test.want == http.StatusServiceUnavailable {
					mediatedResponse(t, response, test.want, "temporarily unavailable")
				} else {
					mediatedResponse(t, response, test.want, test.code.String())
					require.Empty(t, response.Header.Get("Retry-After"))
					require.Empty(t, response.Header.Get("Cache-Control"))
				}
				mediatedResponse(t, mediatedH1Request(t, conn, reader, "unrelated", false), http.StatusOK, "staging:unrelated")
				require.EqualValues(t, 2, stagingCalls.Load())
			})
		}
	}
}

func TestPermanentMediationCallerCancellationRemainsCallerCancellation(t *testing.T) {
	staging, _ := mediatedBackend(t, "staging", false)
	f, address := startMediatedForwarder(t, staging, false)
	notify, release := make(chan struct{}, 1), make(chan struct{})
	defer close(release)
	installHTTPGRPCFailure(f, staging, codes.Canceled, true, notify, release)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address.String()+"/hmr", nil)
	require.NoError(t, err)
	req.Header.Set(localRoutingKeyHeader, "developer")
	done := make(chan error, 1)
	go func() {
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if response != nil {
			_ = response.Body.Close()
		}
		done <- err
	}()
	select {
	case <-notify:
	case <-time.After(5 * time.Second):
		t.Fatal("selected request did not reach the acquired developer tunnel")
	}
	cancel()
	select {
	case err = <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("frontend cancellation did not complete")
	}
}

func TestHTTPAcquisitionDoesNotReclassifyCanceledCallerOrApplicationStatus(t *testing.T) {
	for _, code := range []codes.Code{codes.Canceled, codes.Unavailable} {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		req := httptest.NewRequestWithContext(ctx, http.MethodGet, "http://web.example/", nil)
		transport := httpConnectionAcquisitionTransport{
			timeout: time.Second, protected: true,
			transport: httpAcquisitionTestRoundTripper(func(*http.Request) (*http.Response, error) {
				cancel()
				return nil, status.Error(code, "inner tunnel transport terminated")
			}),
		}
		response, err := transport.RoundTrip(req)
		if response != nil {
			_ = response.Body.Close()
		}
		require.Error(t, err)
		require.False(t, errors.Is(err, errClientStream))
		require.Equal(t, code, status.Code(err))
	}
	transport := httpConnectionAcquisitionTransport{
		timeout: time.Second, protected: true,
		transport: httpAcquisitionTestRoundTripper(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Grpc-Status": {"14"}}, Body: io.NopCloser(strings.NewReader("application"))}, nil
		}),
	}
	response, err := transport.RoundTrip(httptest.NewRequest(http.MethodGet, "http://web.example/", nil))
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "14", response.Header.Get("Grpc-Status"))
}
