package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

type controlledConnStream struct {
	Stream
	send      func(context.Context, Message) error
	receive   func(context.Context) (Message, error)
	closeSend func(context.Context) error
}

func (s *controlledConnStream) Send(ctx context.Context, m Message) error { return s.send(ctx, m) }

func (s *controlledConnStream) Receive(ctx context.Context) (Message, error) { return s.receive(ctx) }

func (s *controlledConnStream) CloseSend(ctx context.Context) error { return s.closeSend(ctx) }

func awaitConnResult[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream event")
		var zero T
		return zero
	}
}

type connReadResult struct {
	data string
	n    int
	err  error
}

func readConnOnce(conn net.Conn, size int) <-chan connReadResult {
	result := make(chan connReadResult, 1)
	go func() {
		buf := make([]byte, size)
		n, err := conn.Read(buf)
		result <- connReadResult{data: string(buf[:n]), n: n, err: err}
	}()
	return result
}

func TestStreamConnCanceledWriteNeverRetainsPublicBuffer(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sent := make(chan string, 1)
	stream := &controlledConnStream{
		send: func(_ context.Context, m Message) error {
			sent <- string(m.Payload())
			return nil
		},
		receive:   func(ctx context.Context) (Message, error) { <-ctx.Done(); return nil, ctx.Err() },
		closeSend: func(context.Context) error { return nil },
	}
	conn := NewStreamConn(ctx, stream, nil, nil)
	buf := []byte("original")
	n, err := conn.Write(buf)
	assert.Zero(t, n)
	assert.ErrorIs(t, err, context.Canceled)
	copy(buf, "mutated!")
	runtime.Gosched()
	select {
	case payload := <-sent:
		t.Errorf("an already canceled write reached the native stream after returning: %q", payload)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestStreamConnAcceptedWriteUsesPrivatePayload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	sent := make(chan string, 1)
	stream := &controlledConnStream{
		send: func(_ context.Context, m Message) error {
			close(entered)
			<-release
			sent <- string(m.Payload())
			return nil
		},
		receive:   func(ctx context.Context) (Message, error) { <-ctx.Done(); return nil, ctx.Err() },
		closeSend: func(context.Context) error { return nil },
	}
	conn := NewStreamConn(ctx, stream, nil, nil)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetWriteDeadline(time.Now().Add(120*time.Millisecond)))
	buf := []byte("original")
	written := make(chan connReadResult, 1)
	go func() { n, err := conn.Write(buf); written <- connReadResult{n: n, err: err} }()
	awaitConnResult(t, entered)
	result := awaitConnResult(t, written)
	assert.Zero(t, result.n)
	assert.ErrorIs(t, result.err, os.ErrDeadlineExceeded)
	copy(buf, "mutated!")
	unblock()
	assert.Equal(t, "original", awaitConnResult(t, sent))
}

func TestStreamConnSerializesNativeSendAndClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	entered, closed := make(chan string, 8), make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var active, calls, closeCalls, overlaps atomic.Int32
	stream := &controlledConnStream{
		send: func(_ context.Context, m Message) error {
			if active.Add(1) != 1 {
				overlaps.Add(1)
			}
			defer active.Add(-1)
			calls.Add(1)
			entered <- string(m.Payload())
			<-release
			return nil
		},
		receive: func(ctx context.Context) (Message, error) { <-ctx.Done(); return nil, ctx.Err() },
		closeSend: func(context.Context) error {
			if active.Load() != 0 {
				overlaps.Add(1)
			}
			closeCalls.Add(1)
			closed <- struct{}{}
			return nil
		},
	}
	conn := NewStreamConn(ctx, stream, nil, nil)
	writes := make(chan connReadResult, 2)
	go func() { n, err := conn.Write([]byte("first")); writes <- connReadResult{n: n, err: err} }()
	assert.Equal(t, "first", awaitConnResult(t, entered))
	go func() { n, err := conn.Write([]byte("second")); writes <- connReadResult{n: n, err: err} }()
	read := readConnOnce(conn, 2)
	select {
	case msg := <-entered:
		t.Errorf("second native Send overlapped an existing native Send: %q", msg)
	case <-time.After(50 * time.Millisecond):
	}
	publicClose := make(chan error, 1)
	go func() { publicClose <- conn.Close() }()
	assert.NoError(t, awaitConnResult(t, publicClose))
	for range 2 {
		result := awaitConnResult(t, writes)
		assert.Zero(t, result.n)
		assert.ErrorIs(t, result.err, net.ErrClosed)
	}
	assert.ErrorIs(t, awaitConnResult(t, read).err, net.ErrClosed)
	select {
	case <-closed:
		t.Error("native CloseSend overlapped a native Send that ignores per-call contexts")
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	if closeCalls.Load() == 0 {
		awaitConnResult(t, closed)
	}
	assert.Zero(t, overlaps.Load())
	assert.EqualValues(t, 1, calls.Load())
	assert.EqualValues(t, 1, closeCalls.Load())
	assert.NoError(t, conn.Close())
}

func TestStreamConnReadDeadlineRetainsLatePayloadAndSingleReceiver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	entered := make(chan int32, 8)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var calls, active, overlaps atomic.Int32
	stream := &controlledConnStream{
		send: func(context.Context, Message) error { return nil },
		receive: func(ctx context.Context) (Message, error) {
			if active.Add(1) != 1 {
				overlaps.Add(1)
			}
			defer active.Add(-1)
			call := calls.Add(1)
			entered <- call
			if call == 1 {
				<-release
				return NewMessage(Normal, []byte("retained")), nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		closeSend: func(context.Context) error { return nil },
	}
	conn := NewStreamConn(ctx, stream, nil, nil)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(120*time.Millisecond)))
	first := readConnOnce(conn, 3)
	assert.EqualValues(t, 1, awaitConnResult(t, entered))
	firstResult := awaitConnResult(t, first)
	assert.Zero(t, firstResult.n)
	assert.ErrorIs(t, firstResult.err, os.ErrDeadlineExceeded)
	require.NoError(t, conn.SetReadDeadline(time.Time{}))
	second := readConnOnce(conn, 3)
	duplicate := false
	select {
	case call := <-entered:
		duplicate = true
		t.Errorf("native Receive #%d started while timed-out Receive was still in flight", call)
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	if duplicate {
		cancel()
		awaitConnResult(t, second)
		return
	}
	secondResult := awaitConnResult(t, second)
	assert.NoError(t, secondResult.err)
	assert.Equal(t, "ret", secondResult.data)
	assert.NoError(t, conn.SetReadDeadline(time.Now().Add(-time.Second)))
	thirdResult := awaitConnResult(t, readConnOnce(conn, 5))
	assert.Zero(t, thirdResult.n)
	assert.ErrorIs(t, thirdResult.err, os.ErrDeadlineExceeded)
	assert.NoError(t, conn.SetReadDeadline(time.Time{}))
	fourthResult := awaitConnResult(t, readConnOnce(conn, 5))
	assert.NoError(t, fourthResult.err)
	assert.Equal(t, "ained", fourthResult.data)
	assert.Zero(t, overlaps.Load())
}

func TestStreamConnFutureDeadlineCanBeExtendedWhileReadWaits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var calls atomic.Int32
	stream := &controlledConnStream{
		send: func(context.Context, Message) error { return nil },
		receive: func(ctx context.Context) (Message, error) {
			if calls.Add(1) != 1 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			close(entered)
			<-release
			return NewMessage(Normal, []byte("extended")), nil
		},
		closeSend: func(context.Context) error { return nil },
	}
	conn := NewStreamConn(ctx, stream, nil, nil)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(250*time.Millisecond)))
	read := readConnOnce(conn, 8)
	awaitConnResult(t, entered)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	select {
	case result := <-read:
		t.Errorf("future deadline extension prematurely interrupted public Read: %v", result.err)
		unblock()
		return
	case <-time.After(300 * time.Millisecond):
	}
	unblock()
	result := awaitConnResult(t, read)
	assert.NoError(t, result.err)
	assert.Equal(t, "extended", result.data)
}

func TestStreamConnCloseAndDeadlineSettersAreSafe(t *testing.T) {
	var closes atomic.Int32
	stream := &controlledConnStream{
		send:      func(context.Context, Message) error { return nil },
		receive:   func(ctx context.Context) (Message, error) { <-ctx.Done(); return nil, io.EOF },
		closeSend: func(context.Context) error { closes.Add(1); return nil },
	}
	conn := NewStreamConn(context.Background(), stream, nil, nil)
	require.NoError(t, conn.Close())
	type setterResult struct {
		err       error
		recovered any
		setter    bool
	}
	results := make(chan setterResult, 60)
	for i := range 60 {
		go func() {
			r := setterResult{setter: i%3 != 0}
			defer func() { r.recovered = recover(); results <- r }()
			switch i % 3 {
			case 0:
				r.err = conn.Close()
			case 1:
				r.err = conn.SetDeadline(time.Now())
			case 2:
				r.err = conn.SetReadDeadline(time.Now())
			}
		}()
	}
	for range 60 {
		r := awaitConnResult(t, results)
		assert.Nil(t, r.recovered, "public operation panicked after close")
		if r.setter {
			assert.True(t, errors.Is(r.err, net.ErrClosed), "setter after close returned %v", r.err)
		} else {
			assert.NoError(t, r.err)
		}
	}
	assert.Eventually(t, func() bool { return closes.Load() == 1 }, time.Second, 5*time.Millisecond)
}

type delayedConnGRPCServer struct {
	manager.UnimplementedManagerServer
	started    chan struct{}
	release    chan struct{}
	fromClient chan string
}

func (s *delayedConnGRPCServer) Tunnel(raw manager.Manager_TunnelServer) error {
	ctx := raw.Context()
	stream, err := NewServerStream(ctx, ManagerToClient, raw)
	if err != nil {
		return err
	}
	close(s.started)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
	}
	if err = stream.Send(ctx, NewMessage(Normal, []byte("retained"))); err != nil {
		return err
	}
	m, err := stream.Receive(ctx)
	if err != nil {
		return err
	}
	s.fromClient <- string(m.Payload())
	_, err = stream.Receive(ctx)
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func TestStreamConnNativeGRPCReadSurvivesDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	peer := &delayedConnGRPCServer{
		started: make(chan struct{}), release: make(chan struct{}), fromClient: make(chan string, 1),
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	manager.RegisterManagerServer(server, peer)
	t.Cleanup(server.Stop)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	client, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	raw, err := manager.NewManagerClient(client).Tunnel(ctx)
	require.NoError(t, err)
	id := NewConnID(types.ProtoTCP, netip.MustParseAddrPort("127.0.0.1:3000"), netip.MustParseAddrPort("127.0.0.1:4000"))
	stream, err := NewClientStream(ctx, ClientToManager, raw, id, SessionID("test-session"), 0, 0)
	require.NoError(t, err)
	awaitConnResult(t, peer.started)
	readProbe, writeProbe := NewCounterProbe("read"), NewCounterProbe("write")
	conn := NewStreamConn(ctx, stream, readProbe, writeProbe)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(120*time.Millisecond)))
	first := awaitConnResult(t, readConnOnce(conn, 3))
	assert.Zero(t, first.n)
	var timeout net.Error
	require.ErrorAs(t, first.err, &timeout)
	assert.True(t, timeout.Timeout())
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(500*time.Millisecond)))
	second := readConnOnce(conn, 3)
	close(peer.release)
	resumed := awaitConnResult(t, second)
	if !assert.NoError(t, resumed.err, "native gRPC late message was lost after resetting a public deadline") {
		return
	}
	assert.Equal(t, "ret", resumed.data)
	remaining := awaitConnResult(t, readConnOnce(conn, 5))
	assert.NoError(t, remaining.err)
	assert.Equal(t, "ained", remaining.data)
	assert.EqualValues(t, 8, readProbe.GetValue())
	n, err := conn.Write([]byte("client"))
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.EqualValues(t, 6, writeProbe.GetValue())
	assert.Equal(t, "client", awaitConnResult(t, peer.fromClient))
	assert.NoError(t, conn.Close())
	server.Stop()
	assert.NoError(t, awaitConnResult(t, serveErr))
}
