package vif

import (
	"context"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
	"github.com/telepresenceio/telepresence/v2/pkg/types"
)

func TestNewConnEndpointTTL(t *testing.T) {
	tests := []struct {
		name          string
		protocol      types.Proto
		tag           tunnel.Tag
		lookupTimeout time.Duration
		want          time.Duration
	}{
		{
			name:          "DNS uses default lookup timeout",
			protocol:      types.ProtoUDP,
			tag:           tunnel.DnsToTun,
			lookupTimeout: 4 * time.Second,
			want:          5 * time.Second,
		},
		{
			name:          "DNS uses custom lookup timeout",
			protocol:      types.ProtoUDP,
			tag:           tunnel.DnsToTun,
			lookupTimeout: 9 * time.Second,
			want:          10 * time.Second,
		},
		{
			name:          "ordinary UDP retains its shorter timeout",
			protocol:      types.ProtoUDP,
			tag:           tunnel.TunToClient,
			lookupTimeout: 9 * time.Second,
			want:          2 * time.Second,
		},
		{
			name:          "ordinary TCP retains its existing timeout",
			protocol:      types.ProtoTCP,
			tag:           tunnel.TunToClient,
			lookupTimeout: 9 * time.Second,
			want:          2 * time.Hour,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := client.GetDefaultConfig()
			config.DNS().LookupTimeout = tt.lookupTimeout
			ctx, cancel := context.WithCancel(client.WithConfig(context.Background(), config))
			defer cancel()

			id := tunnel.NewConnID(
				tt.protocol,
				netip.MustParseAddrPort("192.0.2.2:43210"),
				netip.MustParseAddrPort("198.51.100.53:53"),
			)
			stream, _ := tunnel.NewPipe(id, "test-session", tt.tag, tunnel.TunToDNS)
			conn, peer := net.Pipe()
			defer conn.Close()
			defer peer.Close()

			endpoint := newConnEndpoint(ctx, stream, conn, cancel)
			timedEndpoint, ok := endpoint.(interface{ GetTTL() time.Duration })
			require.True(t, ok)
			require.Equal(t, tt.want, timedEndpoint.GetTTL())
		})
	}
}

func TestDispatchToStreamPreservesDelayedDNSResponse(t *testing.T) {
	config := client.GetDefaultConfig()
	config.DNS().LookupTimeout = 4 * time.Second
	ctx, cancel := context.WithCancel(client.WithConfig(context.Background(), config))
	defer cancel()

	id := tunnel.NewConnID(
		types.ProtoUDP,
		netip.MustParseAddrPort("192.0.2.2:43210"),
		netip.MustParseAddrPort("198.51.100.53:53"),
	)
	stream, peer := tunnel.NewPipe(id, "test-session", tunnel.DnsToTun, tunnel.TunToDNS)
	clientConn, vifConn := net.Pipe()
	defer clientConn.Close()
	defer vifConn.Close()

	responseSent := make(chan error, 1)
	go func() {
		request, err := peer.Receive(ctx)
		if err != nil {
			responseSent <- err
			return
		}
		if string(request.Payload()) != "query" {
			responseSent <- &net.OpError{Op: "read", Err: io.ErrUnexpectedEOF}
			return
		}
		select {
		case <-ctx.Done():
			responseSent <- ctx.Err()
		case <-time.After(2500 * time.Millisecond):
			responseSent <- peer.Send(ctx, tunnel.NewMessage(tunnel.Normal, []byte("response")))
		}
	}()

	ok := dispatchToStream(ctx, id, vifConn, func(context.Context, tunnel.ConnID) (tunnel.Stream, error) {
		return stream, nil
	})
	require.True(t, ok)
	require.NoError(t, clientConn.SetDeadline(time.Now().Add(4*time.Second)))
	_, err := clientConn.Write([]byte("query"))
	require.NoError(t, err)

	response := make([]byte, len("response"))
	_, err = io.ReadFull(clientConn, response)
	require.NoError(t, err)
	require.Equal(t, "response", string(response))
	require.NoError(t, <-responseSent)
}
