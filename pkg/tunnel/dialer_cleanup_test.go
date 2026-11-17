package tunnel

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type cleanupTestStream struct {
	Stream
	receive func(context.Context) (Message, error)
	send    func(context.Context, Message) error
}

func (*cleanupTestStream) ID() ConnID                      { return NewZeroID() }
func (*cleanupTestStream) Tag() Tag                        { return ClientToManager }
func (*cleanupTestStream) CloseSend(context.Context) error { return nil }

func (s *cleanupTestStream) Receive(ctx context.Context) (Message, error) {
	return s.receive(ctx)
}

func (s *cleanupTestStream) Send(ctx context.Context, msg Message) error {
	return s.send(ctx, msg)
}

func openCleanupTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer listener.Close()
	peer, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })
	conn, err := listener.AcceptTCP()
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, peer.SetDeadline(time.Now().Add(5*time.Second)))
	return conn, peer
}

func requireCleanupDone(t *testing.T, endpoint Endpoint, conn *net.TCPConn) {
	t.Helper()
	select {
	case <-endpoint.Done():
	case <-time.After(time.Second):
		t.Fatal("tunnel kept its TCP socket after it should have closed")
	}
	require.ErrorIs(t, conn.SetDeadline(time.Time{}), net.ErrClosed)
}

func TestDialerClosesAfterBothHalfCloses(t *testing.T) {
	conn, peer := openCleanupTCPPair(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var sent bytes.Buffer
	stream := &cleanupTestStream{
		receive: func(context.Context) (Message, error) { return nil, io.EOF },
		send: func(_ context.Context, msg Message) error {
			_, err := sent.Write(msg.Payload())
			return err
		},
	}
	endpoint := NewConnEndpointTTL(stream, conn, cancel, time.Minute, nil, nil)
	endpoint.Start(ctx)

	// The peer sees EOF on the request and can still send the response.
	_, err := peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	_, err = peer.Write([]byte("response"))
	require.NoError(t, err)
	require.NoError(t, peer.CloseWrite())
	requireCleanupDone(t, endpoint, conn)
	require.Equal(t, "response", sent.String())
}

func TestDialerExpiresAfterStreamHalfClose(t *testing.T) {
	conn, peer := openCleanupTCPPair(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &cleanupTestStream{
		receive: func(context.Context) (Message, error) { return nil, io.EOF },
		send:    func(context.Context, Message) error { return nil },
	}
	endpoint := NewConnEndpointTTL(stream, conn, cancel, 50*time.Millisecond, nil, nil)
	endpoint.Start(ctx)

	// The peer never sends its FIN after seeing ours.
	_, err := peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	requireCleanupDone(t, endpoint, conn)
}

func TestDialerClosesWhenStreamSendFails(t *testing.T) {
	conn, peer := openCleanupTCPPair(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &cleanupTestStream{
		receive: func(ctx context.Context) (Message, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		send: func(context.Context, Message) error { return io.ErrClosedPipe },
	}
	endpoint := NewConnEndpointTTL(stream, conn, cancel, time.Minute, nil, nil)
	endpoint.Start(ctx)

	_, err := peer.Write([]byte("response"))
	require.NoError(t, err)
	requireCleanupDone(t, endpoint, conn)
}

func TestDialerExpiresWhileStreamSendIsBlocked(t *testing.T) {
	conn, peer := openCleanupTCPPair(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sendStarted := make(chan struct{})
	stream := &cleanupTestStream{
		receive: func(context.Context) (Message, error) { return nil, io.EOF },
		send: func(ctx context.Context, _ Message) error {
			close(sendStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	endpoint := NewConnEndpointTTL(stream, conn, cancel, 250*time.Millisecond, nil, nil)
	endpoint.Start(ctx)

	_, err := peer.Write([]byte("response"))
	require.NoError(t, err)
	select {
	case <-sendStarted:
	case <-time.After(time.Second):
		t.Fatal("tunnel never tried to send the response")
	}
	requireCleanupDone(t, endpoint, conn)
}
