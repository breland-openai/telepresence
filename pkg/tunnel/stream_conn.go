package tunnel

import (
	"context"
	"io"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/telepresenceio/clog"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

// NewStreamConn returns a net.Conn that reads and writes messages from the given stream.
// The read and write probes are optional.
func NewStreamConn(ctx context.Context, s Stream, readProbe, writeProbe *CounterProbe) net.Conn {
	ioCtx, cancelIO := context.WithCancel(ctx)
	return &streamConn{
		ctx:           ctx,
		ioCtx:         ioCtx,
		cancelIO:      cancelIO,
		stream:        s,
		readProbe:     readProbe,
		writeProbe:    writeProbe,
		closed:        make(chan struct{}),
		incoming:      make(chan connIncoming),
		outgoing:      make(chan connOutgoing),
		readDeadline:  newConnDeadline(),
		writeDeadline: newConnDeadline(),
	}
}

type streamConn struct {
	ctx        context.Context
	ioCtx      context.Context
	cancelIO   context.CancelFunc
	stream     Stream
	readProbe  *CounterProbe
	writeProbe *CounterProbe

	stateLock     sync.Mutex
	closed        chan struct{}
	readDeadline  connDeadline
	writeDeadline connDeadline

	receiveOnce sync.Once
	sendOnce    sync.Once
	incoming    chan connIncoming
	outgoing    chan connOutgoing

	// The lastIncoming message and the offset into it are protected by readLock.
	readLock     sync.Mutex
	offset       int
	lastIncoming Message
}

type connIncoming struct {
	message Message
	err     error
}

type connOutgoing struct {
	ctx      context.Context
	deadline <-chan struct{}
	message  Message
	result   chan error
}

func (c *streamConn) receive() {
	defer close(c.incoming)
	for {
		if connSignalClosed(c.closed) || c.ioCtx.Err() != nil {
			return
		}
		m, err := c.stream.Receive(c.ioCtx)
		if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
			err = io.EOF
		}
		// Keep the result until a public Read takes it or the connection closes.
		select {
		case <-c.ioCtx.Done():
			return
		case c.incoming <- connIncoming{message: m, err: err}:
		}
		if err != nil {
			return
		}
	}
}

func (c *streamConn) Read(data []byte) (n int, err error) {
	c.readLock.Lock()
	defer c.readLock.Unlock()

	deadline := c.readDeadline.wait()
	if err = c.operationError(deadline); err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return 0, nil
	}
	c.receiveOnce.Do(func() { go c.receive() })
	for c.lastIncoming == nil {
		select {
		case <-c.closed:
			return 0, net.ErrClosed
		case <-c.ctx.Done():
			return 0, c.ctx.Err()
		case <-deadline:
			return 0, os.ErrDeadlineExceeded
		case incoming, ok := <-c.incoming:
			if !ok {
				if err = c.operationError(deadline); err != nil {
					return 0, err
				}
				return 0, io.EOF
			}
			if incoming.err != nil {
				if err = c.operationError(deadline); err != nil {
					return 0, err
				}
				return 0, incoming.err
			}
			if incoming.message != nil && len(incoming.message.Payload()) != 0 {
				c.lastIncoming = incoming.message
				c.offset = 0
			}
		}
	}
	payload := c.lastIncoming.Payload()
	n = copy(data, payload[c.offset:])
	c.offset += n
	if c.offset == len(payload) {
		c.lastIncoming = nil
	}
	if c.readProbe != nil {
		c.readProbe.Increment(uint64(n))
	}
	return n, nil
}

func (c *streamConn) send() {
	// This goroutine owns both native payload sends and native close.
	defer func() {
		if err := c.stream.CloseSend(c.ctx); err != nil {
			clog.Debugf(c.ctx, "Close stream send: %v", err)
		}
	}()
	for {
		if connSignalClosed(c.closed) || c.ioCtx.Err() != nil {
			return
		}
		select {
		case <-c.ioCtx.Done():
			return
		case outgoing := <-c.outgoing:
			err := c.operationError(outgoing.deadline)
			if err == nil {
				err = outgoing.ctx.Err()
			}
			if err == nil {
				err = c.stream.Send(outgoing.ctx, outgoing.message)
			}
			if err != nil {
				if interrupted := c.operationError(outgoing.deadline); interrupted != nil {
					err = interrupted
				}
			}
			outgoing.result <- err
		}
	}
}

func (c *streamConn) Write(b []byte) (int, error) {
	deadline := c.writeDeadline.wait()
	if err := c.operationError(deadline); err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, nil
	}
	ctx, cancel := context.WithCancel(c.ioCtx)
	defer cancel()
	outgoing := connOutgoing{
		ctx: ctx, deadline: deadline, message: NewMessage(Normal, b), result: make(chan error, 1),
	}
	c.sendOnce.Do(func() { go c.send() })
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	case <-c.ctx.Done():
		return 0, c.ctx.Err()
	case <-deadline:
		return 0, os.ErrDeadlineExceeded
	case c.outgoing <- outgoing:
	}

	finish := func(err error) (int, error) {
		if err != nil {
			return 0, err
		}
		n := len(b)
		if c.writeProbe != nil {
			c.writeProbe.Increment(uint64(n))
		}
		clog.Debugf(c.ctx, "Write %d bytes", n)
		return n, nil
	}
	interrupted := func(err error) (int, error) {
		select {
		case completed := <-outgoing.result:
			return finish(completed)
		default:
			return 0, err
		}
	}
	select {
	case err := <-outgoing.result:
		return finish(err)
	case <-c.closed:
		return interrupted(net.ErrClosed)
	case <-c.ctx.Done():
		return interrupted(c.ctx.Err())
	case <-deadline:
		return interrupted(os.ErrDeadlineExceeded)
	}
}

func (c *streamConn) Close() error {
	c.stateLock.Lock()
	if connSignalClosed(c.closed) {
		c.stateLock.Unlock()
		return nil
	}
	close(c.closed)
	c.readDeadline.set(time.Time{})
	c.writeDeadline.set(time.Time{})
	c.stateLock.Unlock()
	c.cancelIO()
	c.sendOnce.Do(func() { go c.send() })
	return nil
}

func (c *streamConn) operationError(deadline <-chan struct{}) error {
	switch {
	case connSignalClosed(c.closed):
		return net.ErrClosed
	case c.ctx.Err() != nil:
		return c.ctx.Err()
	case connSignalClosed(deadline):
		return os.ErrDeadlineExceeded
	default:
		return nil
	}
}

func addFromAP(ap netip.AddrPort, proto types.Proto) net.Addr {
	if proto == types.ProtoUDP {
		return net.UDPAddrFromAddrPort(ap)
	}
	return net.TCPAddrFromAddrPort(ap)
}

func (c *streamConn) LocalAddr() net.Addr {
	id := c.stream.ID()
	return addFromAP(id.Source(), id.Protocol())
}

func (c *streamConn) RemoteAddr() net.Addr {
	id := c.stream.ID()
	return addFromAP(id.Destination(), id.Protocol())
}

func (c *streamConn) SetDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	if connSignalClosed(c.closed) {
		return net.ErrClosed
	}
	c.readDeadline.set(t)
	c.writeDeadline.set(t)
	return nil
}

func (c *streamConn) SetReadDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	if connSignalClosed(c.closed) {
		return net.ErrClosed
	}
	c.readDeadline.set(t)
	return nil
}

func (c *streamConn) SetWriteDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	if connSignalClosed(c.closed) {
		return net.ErrClosed
	}
	c.writeDeadline.set(t)
	return nil
}

type connDeadline struct {
	lock   sync.Mutex
	signal chan struct{}
	timer  *time.Timer
}

func newConnDeadline() connDeadline {
	return connDeadline{signal: make(chan struct{})}
}

func (d *connDeadline) wait() <-chan struct{} {
	d.lock.Lock()
	defer d.lock.Unlock()
	return d.signal
}

func (d *connDeadline) set(t time.Time) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if d.timer != nil {
		if !d.timer.Stop() {
			<-d.signal
		}
		d.timer = nil
	}
	if t.IsZero() || time.Until(t) > 0 {
		if connSignalClosed(d.signal) {
			d.signal = make(chan struct{})
		}
		if !t.IsZero() {
			signal := d.signal
			d.timer = time.AfterFunc(time.Until(t), func() { close(signal) })
		}
	} else if !connSignalClosed(d.signal) {
		close(d.signal)
	}
}

func connSignalClosed(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return true
	default:
		return false
	}
}
