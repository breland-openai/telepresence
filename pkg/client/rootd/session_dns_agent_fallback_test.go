package rootd

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	dns2 "github.com/miekg/dns"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/telepresenceio/telepresence/rpc/v2/agent"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/dnsproxy"
)

type fallbackDNSCall struct {
	name     string
	session  string
	deadline time.Time
}

type fallbackDNSLookup func(context.Context, *manager.LookupRequest) (*manager.LookupResponse, error)

type fallbackDNSCallLog struct {
	sync.Mutex
	calls []fallbackDNSCall
}

func (l *fallbackDNSCallLog) record(ctx context.Context, name, session string) {
	deadline, _ := ctx.Deadline()
	l.Lock()
	l.calls = append(l.calls, fallbackDNSCall{name: name, session: session, deadline: deadline})
	l.Unlock()
}

func (l *fallbackDNSCallLog) snapshot() []fallbackDNSCall {
	l.Lock()
	defer l.Unlock()
	return append([]fallbackDNSCall(nil), l.calls...)
}

type fallbackDNSAgent struct {
	agent.UnimplementedAgentServer
	lookup fallbackDNSLookup
	calls  fallbackDNSCallLog
}

func (a *fallbackDNSAgent) Lookup(ctx context.Context, req *manager.LookupRequest) (*manager.LookupResponse, error) {
	a.calls.record(ctx, req.Name, req.GetSession().GetSessionId())
	return a.lookup(ctx, req)
}

type fallbackDNSManager struct {
	manager.UnimplementedManagerServer
	lookup   fallbackDNSLookup
	complex  *manager.DNSResponse
	calls    fallbackDNSCallLog
	dnsCalls fallbackDNSCallLog
}

func (m *fallbackDNSManager) Lookup(ctx context.Context, req *manager.LookupRequest) (*manager.LookupResponse, error) {
	m.calls.record(ctx, req.Name, req.GetSession().GetSessionId())
	return m.lookup(ctx, req)
}

func (m *fallbackDNSManager) LookupDNS(ctx context.Context, req *manager.DNSRequest) (*manager.DNSResponse, error) {
	m.dnsCalls.record(ctx, req.Name, req.GetSession().GetSessionId())
	return m.complex, nil
}

func fallbackDNSResponse(ips ...string) fallbackDNSLookup {
	return func(context.Context, *manager.LookupRequest) (*manager.LookupResponse, error) {
		resp := &manager.LookupResponse{}
		for _, ip := range ips {
			resp.Ips = append(resp.Ips, netip.MustParseAddr(ip).AsSlice())
		}
		return resp, nil
	}
}

func fallbackDNSError(code codes.Code) fallbackDNSLookup {
	return func(context.Context, *manager.LookupRequest) (*manager.LookupResponse, error) {
		return nil, status.Error(code, code.String())
	}
}

func newFallbackDNSAgentConnection(t *testing.T, a *fallbackDNSAgent) (*grpc.ClientConn, *grpc.Server) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	agent.RegisterAgentServer(server, a)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///retired-dns-agent", grpc.WithContextDialer(
		func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
		server.Stop()
		require.NoError(t, listener.Close())
	})
	return conn, server
}

func newFallbackDNSSession(t *testing.T, ag *fallbackDNSAgent, mgr *fallbackDNSManager) (*session, *grpc.Server) {
	t.Helper()
	ctx := client.WithConfig(t.Context(), client.GetDefaultConfig())
	s := newTestStreamSession(ctx)
	s.lookupSequencer = xsync.NewMap[string, clusterLookupResult]()
	s.managerConn = newWorkloadLookupConnection(t, func(server *grpc.Server) {
		manager.RegisterManagerServer(server, mgr)
	})
	if ag == nil {
		return s, nil
	}
	conn, server := newFallbackDNSAgentConnection(t, ag)
	s.agentClients = &workloadLookupClients{random: agent.NewAgentClient(conn)}
	return s, server
}

func TestSimpleDNSRecoversBothFamiliesAfterRetiredAgentTransportEOF(t *testing.T) {
	const name = "statefulset.tp-regression.svc.cluster.local."
	entered := make(chan struct{})
	ag := &fallbackDNSAgent{lookup: func(ctx context.Context, _ *manager.LookupRequest) (*manager.LookupResponse, error) {
		if err := grpc.SendHeader(ctx, metadata.Pairs("agent-generation", "retired")); err != nil {
			return nil, err
		}
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	mgr := &fallbackDNSManager{lookup: fallbackDNSResponse("10.96.230.196", "fd00::196")}
	s, oldServer := newFallbackDNSSession(t, ag, mgr)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type result struct {
		records dnsproxy.RRs
		code    int
		err     error
	}
	completed := make(chan result, 1)
	go func() {
		records, code, err := s.clusterLookup(ctx, lookupQuestion(name, dns2.TypeA))
		completed <- result{records: records, code: code, err: err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("lookup did not reach the retiring physical agent")
	}
	oldServer.Stop()
	var answer result
	select {
	case answer = <-completed:
	case <-ctx.Done():
		t.Fatal("lookup did not finish within its caller deadline")
	}
	require.Equal(t, 1, len(ag.calls.snapshot()))
	require.NoError(t, answer.err)
	require.Equal(t, dns2.RcodeSuccess, answer.code)
	requireLookupAnswer(t, answer.records, "10.96.230.196")
	requireLookupAnswer(t, answer.records, "fd00::196")
	managerCalls := mgr.calls.snapshot()
	require.Len(t, managerCalls, 1)
	require.Equal(t, name, managerCalls[0].name)
	require.Equal(t, "test-session", managerCalls[0].session)
	require.Empty(t, mgr.dnsCalls.snapshot())
	records, code, err := s.clusterLookup(ctx, lookupQuestion(name, dns2.TypeAAAA))
	require.NoError(t, err)
	require.Equal(t, dns2.RcodeSuccess, code)
	requireLookupAnswer(t, records, "fd00::196")
	require.Len(t, mgr.calls.snapshot(), 1)
	require.Zero(t, s.dnsFailures)
}

func TestSimpleDNSKeepsAnswersErrorsAndLegacyRouting(t *testing.T) {
	const name = "test.ns.svc.cluster.local."
	complexResponse, err := dnsproxy.ToRPC(dnsproxy.RRs{&dns2.A{Hdr: rrHeader(name, dns2.TypeA), A: net.ParseIP("192.0.2.55").To4()}}, dns2.RcodeSuccess)
	require.NoError(t, err)
	tests := []struct {
		name         string
		agent        fallbackDNSLookup
		manager      fallbackDNSLookup
		wantAgent    int
		wantManager  int
		wantComplex  int
		wantRCode    int
		wantError    codes.Code
		wantAddress  string
		wantFailures int
	}{
		{name: "no agent uses simple manager", manager: fallbackDNSResponse("10.96.0.10"), wantManager: 1, wantRCode: dns2.RcodeSuccess, wantAddress: "10.96.0.10"},
		{name: "agent answer", agent: fallbackDNSResponse("10.96.0.11"), manager: fallbackDNSResponse("10.96.0.10"), wantAgent: 1, wantRCode: dns2.RcodeSuccess, wantAddress: "10.96.0.11"},
		{name: "agent authoritative no addresses", agent: fallbackDNSResponse(), manager: fallbackDNSResponse("10.96.0.10"), wantAgent: 1, wantRCode: dns2.RcodeNameError},
		{name: "agent permission denied", agent: fallbackDNSError(codes.PermissionDenied), manager: fallbackDNSResponse("10.96.0.10"), wantAgent: 1, wantRCode: dns2.RcodeServerFailure, wantError: codes.PermissionDenied, wantFailures: 1},
		{name: "agent invalid input", agent: fallbackDNSError(codes.InvalidArgument), manager: fallbackDNSResponse("10.96.0.10"), wantAgent: 1, wantRCode: dns2.RcodeServerFailure, wantError: codes.InvalidArgument, wantFailures: 1},
		{name: "agent resolver deadline", agent: fallbackDNSError(codes.DeadlineExceeded), manager: fallbackDNSResponse("10.96.0.10"), wantAgent: 1, wantRCode: dns2.RcodeServerFailure, wantError: codes.DeadlineExceeded, wantFailures: 1},
		{name: "agent resolver internal error", agent: fallbackDNSError(codes.Internal), manager: fallbackDNSResponse("10.96.0.10"), wantAgent: 1, wantRCode: dns2.RcodeServerFailure, wantError: codes.Internal, wantFailures: 1},
		{name: "agent legacy uses complex manager directly", agent: fallbackDNSError(codes.Unimplemented), manager: fallbackDNSResponse("10.96.0.10"), wantAgent: 1, wantComplex: 1, wantRCode: dns2.RcodeSuccess, wantAddress: "192.0.2.55"},
		{name: "manager authoritative no addresses after unavailable agent", agent: fallbackDNSError(codes.Unavailable), manager: fallbackDNSResponse(), wantAgent: 1, wantManager: 1, wantRCode: dns2.RcodeNameError},
		{name: "manager failure after unavailable agent is reported once", agent: fallbackDNSError(codes.Unavailable), manager: fallbackDNSError(codes.Unauthenticated), wantAgent: 1, wantManager: 1, wantRCode: dns2.RcodeServerFailure, wantError: codes.Unauthenticated, wantFailures: 1},
		{name: "manager legacy after unavailable agent", agent: fallbackDNSError(codes.Unavailable), manager: fallbackDNSError(codes.Unimplemented), wantAgent: 1, wantManager: 1, wantComplex: 1, wantRCode: dns2.RcodeSuccess, wantAddress: "192.0.2.55"},
		{name: "unavailable manager without agent is not retried", manager: fallbackDNSError(codes.Unavailable), wantManager: 1, wantRCode: dns2.RcodeServerFailure, wantError: codes.Unavailable, wantFailures: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ag *fallbackDNSAgent
			if tt.agent != nil {
				ag = &fallbackDNSAgent{lookup: tt.agent}
			}
			mgr := &fallbackDNSManager{lookup: tt.manager, complex: complexResponse}
			s, _ := newFallbackDNSSession(t, ag, mgr)
			records, code, lookupErr := s.simpleLookup(t.Context(), lookupQuestion(name, dns2.TypeA))
			require.Equal(t, tt.wantError, status.Code(lookupErr))
			require.Equal(t, tt.wantRCode, code)
			if tt.wantAddress == "" {
				require.Empty(t, records)
			} else {
				requireLookupAnswer(t, records, tt.wantAddress)
			}
			if ag != nil {
				require.Len(t, ag.calls.snapshot(), tt.wantAgent)
			}
			require.Len(t, mgr.calls.snapshot(), tt.wantManager)
			require.Len(t, mgr.dnsCalls.snapshot(), tt.wantComplex)
			require.Equal(t, tt.wantFailures, s.dnsFailures)
		})
	}
}

func TestSimpleDNSAgentTransportFallbackKeepsOriginalCallerDeadline(t *testing.T) {
	const name = "deadline.ns.svc.cluster.local."
	managerEntered := make(chan struct{})
	managerCanceled := make(chan struct{})
	ag := &fallbackDNSAgent{lookup: fallbackDNSError(codes.Unavailable)}
	mgr := &fallbackDNSManager{lookup: func(ctx context.Context, _ *manager.LookupRequest) (*manager.LookupResponse, error) {
		close(managerEntered)
		<-ctx.Done()
		close(managerCanceled)
		return nil, ctx.Err()
	}}
	s, _ := newFallbackDNSSession(t, ag, mgr)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	type result struct {
		code int
		err  error
	}
	completed := make(chan result, 1)
	go func() {
		_, code, err := s.simpleLookup(ctx, lookupQuestion(name, dns2.TypeAAAA))
		completed <- result{code: code, err: err}
	}()
	select {
	case <-managerEntered:
	case answer := <-completed:
		t.Fatalf("manager fallback was not attempted: %v", answer.err)
	case <-ctx.Done():
		t.Fatal("manager fallback did not start before the original caller deadline")
	}
	for _, calls := range [][]fallbackDNSCall{ag.calls.snapshot(), mgr.calls.snapshot()} {
		require.Len(t, calls, 1)
		require.Equal(t, name, calls[0].name)
		require.Equal(t, "test-session", calls[0].session)
		require.WithinDuration(t, deadline, calls[0].deadline, 250*time.Millisecond)
	}
	answer := <-completed
	require.Equal(t, codes.DeadlineExceeded, status.Code(answer.err))
	require.Equal(t, dns2.RcodeServerFailure, answer.code)
	select {
	case <-managerCanceled:
	case <-time.After(time.Second):
		t.Fatal("manager lookup was not canceled by the original caller deadline")
	}
	require.Equal(t, 1, s.dnsFailures)
}

func TestSimpleDNSCallerCancellationDoesNotStartManagerFallback(t *testing.T) {
	entered := make(chan struct{})
	ag := &fallbackDNSAgent{lookup: func(ctx context.Context, _ *manager.LookupRequest) (*manager.LookupResponse, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	mgr := &fallbackDNSManager{lookup: fallbackDNSResponse("10.96.0.10")}
	s, _ := newFallbackDNSSession(t, ag, mgr)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	completed := make(chan error, 1)
	go func() {
		_, code, err := s.simpleLookup(ctx, lookupQuestion("canceled.ns.svc.cluster.local.", dns2.TypeA))
		if code != dns2.RcodeServerFailure {
			completed <- status.Errorf(codes.Internal, "unexpected DNS result %d", code)
			return
		}
		completed <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("lookup did not reach the optional agent")
	}
	cancel()
	select {
	case err := <-completed:
		require.Equal(t, codes.Canceled, status.Code(err))
	case <-time.After(time.Second):
		t.Fatal("lookup did not stop after caller cancellation")
	}
	require.Len(t, ag.calls.snapshot(), 1)
	require.Empty(t, mgr.calls.snapshot())
	require.Empty(t, mgr.dnsCalls.snapshot())
	require.Equal(t, 1, s.dnsFailures)
}
