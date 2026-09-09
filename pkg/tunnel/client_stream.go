package tunnel

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type GRPCClientStream interface {
	GRPCStream
	CloseSend() error
}

func NewClientStream(ctx context.Context, tag Tag, grpcStream GRPCClientStream, id ConnID, sessionID SessionID, callDelay, dialTimeout time.Duration) (Stream, error) {
	return newClientStream(ctx, tag, grpcStream, id, sessionID, callDelay, dialTimeout, false)
}

func newDialResponseStream(ctx context.Context, tag Tag, grpcStream GRPCClientStream, id ConnID, sessionID SessionID, callDelay, dialTimeout time.Duration) (Stream, error) {
	return newClientStream(ctx, tag, grpcStream, id, sessionID, callDelay, dialTimeout, true)
}

func newClientStream(ctx context.Context, tag Tag, grpcStream GRPCClientStream, id ConnID, sessionID SessionID,
	callDelay, dialTimeout time.Duration, dialResponse bool,
) (Stream, error) {
	s := &clientStream{stream: newStream(tag, grpcStream)}
	s.id = id
	s.roundtripLatency = callDelay
	s.dialTimeout = dialTimeout
	s.sessionID = sessionID
	s.dialResponse = dialResponse

	if err := s.Send(ctx, streamInfoMessage(id, sessionID, callDelay, dialTimeout, dialResponse)); err != nil {
		_ = s.CloseSend(ctx)
		return nil, err
	}
	m, err := s.Receive(ctx)
	if err != nil {
		_ = s.CloseSend(ctx)
		return nil, fmt.Errorf("failed to read initial StreamOK message: %w", err)
	}
	if m.Code() != streamOK {
		_ = s.CloseSend(ctx)
		return nil, errors.New("initial message was not StreamOK")
	}
	s.peerVersion = getVersion(m)
	return s, nil
}

type clientStream struct {
	stream
}

func (s *clientStream) CloseSend(_ context.Context) error {
	return s.grpcStream.(GRPCClientStream).CloseSend()
}
