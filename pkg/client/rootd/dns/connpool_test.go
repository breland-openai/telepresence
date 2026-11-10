//go:build linux

package dns

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
)

func TestConnPoolConcurrency(t *testing.T) {
	const (
		TOTAL_THREADS       = 15
		REQUESTS_PER_THREAD = 5
		TIMEOUT_S           = 8
	)
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	started := make(chan struct{})
	server := &dns.Server{
		PacketConn:        listener,
		NotifyStartedFunc: func() { close(started) },
		Handler: dns.HandlerFunc(func(writer dns.ResponseWriter, request *dns.Msg) {
			response := new(dns.Msg)
			response.SetReply(request)
			response.Authoritative = true
			question := request.Question[0]
			response.Answer = []dns.RR{&dns.MX{
				Hdr: dns.RR_Header{Name: question.Name, Rrtype: dns.TypeMX, Class: dns.ClassINET},
				Mx:  "mail.example.",
			}}
			if err := writer.WriteMsg(response); err != nil {
				t.Errorf("write DNS response: %v", err)
			}
		}),
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.ActivateAndServe() }()
	select {
	case <-started:
	case err := <-serverDone:
		require.NoError(t, err)
		t.Fatal("DNS test server stopped before it became ready")
	case <-time.After(TIMEOUT_S * time.Second):
		t.Fatal("DNS test server did not start")
	}
	t.Cleanup(func() {
		require.NoError(t, server.Shutdown())
		require.NoError(t, <-serverDone)
	})
	addr, err := netip.ParseAddrPort(listener.LocalAddr().String())
	require.NoError(t, err)
	ctx := context.Background()
	dc := &dns.Client{
		Net:     "udp",
		Timeout: TIMEOUT_S * time.Second,
	}
	pool, err := NewConnPool(addr, 5)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	defer pool.Close()
	errors := make(chan error, TOTAL_THREADS*REQUESTS_PER_THREAD)
	wg := &sync.WaitGroup{}
	wg.Add(TOTAL_THREADS)
	for i := 0; i < TOTAL_THREADS; i++ {
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < REQUESTS_PER_THREAD; j++ {
				msg := new(dns.Msg)
				domain := fmt.Sprintf("dns-test-%d-%d.example.", idx, j)
				msg.SetQuestion(domain, dns.TypeMX)
				ctx, cancel := context.WithTimeout(ctx, TIMEOUT_S*time.Second)
				response, _, err := pool.Exchange(ctx, dc, msg)
				cancel()
				if err != nil {
					errors <- err
					continue
				}
				if response.Rcode != dns.RcodeSuccess || len(response.Answer) != 1 {
					errors <- fmt.Errorf("unexpected DNS response for %s: %v", domain, response)
					continue
				}
				mx, ok := response.Answer[0].(*dns.MX)
				if !ok || mx.Hdr.Name != domain || mx.Mx != "mail.example." {
					errors <- fmt.Errorf("unexpected DNS answer for %s: %v", domain, response.Answer[0])
				}
			}
		}(i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Error(err)
		}
	}
}
