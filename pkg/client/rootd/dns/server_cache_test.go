package dns

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/telepresenceio/clog/testutil"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/dnsproxy"
)

func cacheTestAnswer(question *dns.Question) dnsproxy.RRs {
	return dnsproxy.RRs{&dns.A{
		Hdr: dnsproxy.NewHeader(question.Name, dns.TypeA),
		A:   net.ParseIP("192.0.2.10"),
	}}
}

func cacheOppositeFamilySuccess(server *Server, name string) {
	ready := make(chan struct{})
	close(ready)
	server.cache.Store(cacheKey{name: name, qType: dns.TypeAAAA}, &cacheEntry{
		created: time.Now(),
		rCode:   dns.RcodeSuccess,
		wait:    ready,
	})
}

func TestTemporaryDNSFailuresAreNotCached(t *testing.T) {
	for _, tt := range []struct {
		name  string
		rCode int
		err   error
	}{
		{name: "SERVFAIL response", rCode: dns.RcodeServerFailure},
		{name: "canceled RPC", rCode: dns.RcodeNameError, err: status.Error(codes.Canceled, "remote cancellation")},
		{name: "deadline RPC", rCode: dns.RcodeNameError, err: status.Error(codes.DeadlineExceeded, "remote deadline")},
		{name: "unavailable RPC", rCode: dns.RcodeServerFailure, err: status.Error(codes.Unavailable, "remote failure")},
		{name: "canceled resolver", rCode: dns.RcodeNameError, err: context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, otherFamily := range []bool{false, true} {
				name := "without other family"
				if otherFamily {
					name = "with other family"
				}
				t.Run(name, func(t *testing.T) {
					calls := 0
					server := NewServer(&client.DNS{IncludeSuffixes: []string{".example"}}, "application",
						func(_ context.Context, question *dns.Question) (dnsproxy.RRs, int, error) {
							calls++
							if calls == 1 {
								return nil, tt.rCode, tt.err
							}
							return cacheTestAnswer(question), dns.RcodeSuccess, nil
						})
					server.ctx = testutil.NewContext(t, false)
					question := &dns.Question{Name: "service.example.", Qtype: dns.TypeA, Qclass: dns.ClassINET}
					if otherFamily {
						cacheOppositeFamilySuccess(server, question.Name)
					}

					answer, rCode, err := server.resolveThruCache(question)
					require.Empty(t, answer)
					require.Equal(t, dns.RcodeServerFailure, rCode)
					if tt.err == nil {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, tt.err)
					}
					_, cached := server.cache.Load(cacheKey{name: question.Name, qType: question.Qtype})
					require.False(t, cached)

					for range 2 {
						answer, rCode, err = server.resolveThruCache(question)
						require.NoError(t, err)
						require.Equal(t, dns.RcodeSuccess, rCode)
						require.Len(t, answer, 1)
						require.Equal(t, 2, calls)
					}
				})
			}
		})
	}
}

func TestDNSLookupDeadlineIsNotCached(t *testing.T) {
	calls := 0
	server := NewServer(&client.DNS{IncludeSuffixes: []string{".example"}, LookupTimeout: time.Millisecond}, "application",
		func(ctx context.Context, question *dns.Question) (dnsproxy.RRs, int, error) {
			calls++
			if calls == 1 {
				<-ctx.Done()
				return nil, dns.RcodeNameError, ctx.Err()
			}
			return cacheTestAnswer(question), dns.RcodeSuccess, nil
		})
	server.ctx = testutil.NewContext(t, false)
	question := &dns.Question{Name: "service.example.", Qtype: dns.TypeA, Qclass: dns.ClassINET}

	answer, rCode, err := server.resolveThruCache(question)
	require.Empty(t, answer)
	require.Equal(t, dns.RcodeServerFailure, rCode)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, cached := server.cache.Load(cacheKey{name: question.Name, qType: question.Qtype})
	require.False(t, cached)

	answer, rCode, err = server.resolveThruCache(question)
	require.NoError(t, err)
	require.Equal(t, dns.RcodeSuccess, rCode)
	require.Len(t, answer, 1)
	require.Equal(t, 2, calls)
}

func TestDNSAbsenceRemainsCached(t *testing.T) {
	for _, otherFamily := range []bool{false, true} {
		name := "absent name"
		expectedCode := dns.RcodeNameError
		if otherFamily {
			name = "absent family"
			expectedCode = dns.RcodeSuccess
		}
		t.Run(name, func(t *testing.T) {
			calls := 0
			server := NewServer(&client.DNS{IncludeSuffixes: []string{".example"}}, "application",
				func(context.Context, *dns.Question) (dnsproxy.RRs, int, error) {
					calls++
					return nil, dns.RcodeNameError, nil
				})
			server.ctx = testutil.NewContext(t, false)
			question := &dns.Question{Name: "missing.example.", Qtype: dns.TypeA, Qclass: dns.ClassINET}
			if otherFamily {
				cacheOppositeFamilySuccess(server, question.Name)
			}

			for range 2 {
				answer, rCode, err := server.resolveThruCache(question)
				require.NoError(t, err)
				require.Empty(t, answer)
				require.Equal(t, expectedCode, rCode)
				require.Equal(t, 1, calls)
			}

			entry, cached := server.cache.Load(cacheKey{name: question.Name, qType: question.Qtype})
			require.True(t, cached)
			entry.created = time.Now().Add(-cacheTTL - time.Second)
			_, _, err := server.resolveThruCache(question)
			require.NoError(t, err)
			require.Equal(t, 2, calls)
		})
	}
}
