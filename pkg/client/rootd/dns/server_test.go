package dns

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/telepresenceio/clog/testutil"
	rpc "github.com/telepresenceio/telepresence/rpc/v2/daemon"
	"github.com/telepresenceio/telepresence/v2/pkg/client"
	"github.com/telepresenceio/telepresence/v2/pkg/dnsproxy"
)

type recordingDNSResponseWriter struct {
	dns.ResponseWriter
	message *dns.Msg
}

func (w *recordingDNSResponseWriter) WriteMsg(message *dns.Msg) error {
	w.message = message.Copy()
	return nil
}

type suiteServer struct {
	suite.Suite

	server *Server
}

func (s *suiteServer) SetupSuite() {
	s.server = &Server{
		cache: xsync.NewMap[cacheKey, *cacheEntry](),
	}
}

func (s *suiteServer) TestSetMappings() {
	// given
	entry := &cacheEntry{wait: make(chan struct{}), created: time.Now()}
	aliasKeyA := cacheKey{name: "echo-easy-alias.", qType: dns.TypeA}
	aliasKeyAAAA := cacheKey{name: "echo-easy-alias.", qType: dns.TypeAAAA}
	aliasedToKeyA := cacheKey{name: "echo-easy.blue.svc.cluster.local.", qType: dns.TypeA}
	aliasedToKeyAAAA := cacheKey{name: "echo-easy.blue.svc.cluster.local.", qType: dns.TypeA}

	s.server.cache.Store(aliasKeyA, entry)
	s.server.cache.Store(aliasKeyAAAA, entry)
	s.server.cache.Store(aliasedToKeyA, entry)
	s.server.cache.Store(aliasedToKeyAAAA, entry)

	s.server.mappingsMap = map[string]string{}

	// when
	s.server.SetMappings([]*rpc.DNSMapping{
		{
			Name:     "echo-easy-alias",
			AliasFor: "echo-easy.blue.svc.cluster.local",
		},
	})

	// then
	_, exists := s.server.cache.Load(aliasKeyA)
	s.False(exists, "Mapping's A record wasn't purged")
	_, exists = s.server.cache.Load(aliasKeyAAAA)
	s.False(exists, "Mapping's AAAA record wasn't purged")
	_, exists = s.server.cache.Load(aliasedToKeyA)
	s.True(exists, "Service's A record was purged")
	_, exists = s.server.cache.Load(aliasedToKeyAAAA)
	s.True(exists, "Service's AAAA record was purged")

	s.Equal(s.server.mappingsMap, map[string]string{
		"echo-easy-alias.": "echo-easy.blue.svc.cluster.local.",
	})

	// given
	s.server.cache.Store(aliasKeyA, entry)
	s.server.cache.Store(aliasKeyAAAA, entry)

	// when
	s.server.SetMappings([]*rpc.DNSMapping{})

	// then
	// mappings are empty
	s.Empty(s.server.mappingsMap)

	// nothing is purged when clearing the mappings because mappings never make it to the cache
	_, exists = s.server.cache.Load(aliasKeyA)
	s.True(exists, "Mapping's A record was purged")
	_, exists = s.server.cache.Load(aliasKeyAAAA)
	s.True(exists, "Mapping's AAAA record was purged")
	_, exists = s.server.cache.Load(aliasedToKeyA)
	s.True(exists, "Service's A record was purged")
	_, exists = s.server.cache.Load(aliasedToKeyAAAA)
	s.True(exists, "Service's AAAA record was purged")
}

func (s *suiteServer) TestSetExcludes() {
	// given
	entry := &cacheEntry{wait: make(chan struct{}), created: time.Now()}
	toDeleteARecordKey := cacheKey{name: "echo-easy.", qType: dns.TypeA}
	toDelete4ARecordKey := cacheKey{name: "echo-easy.", qType: dns.TypeAAAA}
	toDeleteNewARecordKey := cacheKey{name: "new-excluded.", qType: dns.TypeAAAA}

	s.server.cache.Store(toDeleteARecordKey, entry)
	s.server.cache.Store(toDelete4ARecordKey, entry)
	s.server.cache.Store(toDeleteNewARecordKey, entry)

	s.server.Excludes = []string{"echo-easy"}

	// when
	newExcluded := []string{"new-excluded"}
	s.server.SetExcludes(newExcluded)

	// then
	_, exists := s.server.cache.Load(toDeleteARecordKey)
	assert.False(s.T(), exists, "Excluded A record was purged")
	_, exists = s.server.cache.Load(toDelete4ARecordKey)
	assert.False(s.T(), exists, "Excluded AAAA record was purged")
	_, exists = s.server.cache.Load(toDeleteNewARecordKey)
	assert.False(s.T(), exists, "New excluded record was purged")
	assert.Equal(s.T(), newExcluded, s.server.Excludes)
}

func (s *suiteServer) TestIsExcluded() {
	// given
	s.server.Excludes = []string{
		"echo-easy",
	}
	s.server.search = []string{
		tel2SubDomainDot + "cluster.local",
		"blue.svc.cluster.local",
	}

	// when & then
	assert.True(s.T(), s.server.isExcluded("echo-easy"))
	assert.True(s.T(), s.server.isExcluded("echo-easy.tel2-search.cluster.local"))
	assert.True(s.T(), s.server.isExcluded("echo-easy.blue.svc.cluster.local"))
	assert.False(s.T(), s.server.isExcluded("something-else"))
}

func TestServerTestSuite(t *testing.T) {
	suite.Run(t, new(suiteServer))
}

func TestMappingsMapCanonicalizesFullyQualifiedNames(t *testing.T) {
	mappings := mappingsMap(client.DNSMappings{
		{Name: "KUBERNETES.DEFAULT.SVC.", AliasFor: "10.0.0.1"},
		{Name: "database.APPLICATION.", AliasFor: "Postgres.APPLICATION.SVC.CLUSTER.LOCAL."},
	})

	require.Equal(t, map[string]string{
		"kubernetes.default.svc.": "10.0.0.1",
		"database.application.":   "postgres.application.svc.cluster.local.",
	}, mappings)
}

func TestPreservedLocalClusterDNS(t *testing.T) {
	const (
		localAPIIP         = "10.0.0.1"
		localServiceName   = "artifact-gateway.platform.svc.cluster.local."
		localServiceIP     = "192.0.2.21"
		remoteAPIIP        = "246.246.0.3"
		remoteServiceIP    = "198.51.100.21"
		remotePlatformName = "frontend.platform.svc.cluster.local."
		remotePlatformIP   = "198.51.100.42"
		remoteWebIP        = "246.246.0.13"
	)

	var remoteQueries []string
	remoteLookup := func(_ context.Context, question *dns.Question) (dnsproxy.RRs, int, error) {
		remoteQueries = append(remoteQueries, question.Name)
		answerIP := remoteAPIIP
		switch question.Name {
		case localServiceName:
			answerIP = remoteServiceIP
		case remotePlatformName:
			answerIP = remotePlatformIP
		case "web.application.svc.cluster.local.":
			answerIP = remoteWebIP
		}
		if question.Qtype != dns.TypeA {
			return nil, dns.RcodeSuccess, nil
		}
		return dnsproxy.RRs{&dns.A{
			Hdr: dns.RR_Header{Name: question.Name, Rrtype: dns.TypeA, Class: dns.ClassINET},
			A:   net.ParseIP(answerIP),
		}}, dns.RcodeSuccess, nil
	}

	server := NewServer(&client.DNS{
		Mappings: client.DNSMappings{
			{Name: "kubernetes.default.svc", AliasFor: localAPIIP},
			{Name: "kubernetes.default.svc.cluster.local", AliasFor: localAPIIP},
			{Name: localServiceName, AliasFor: localServiceIP},
		},
	}, "application", remoteLookup)
	server.ctx = testutil.NewContext(t, true)
	server.clientLookup = server.resolveThruCache

	// Stale remote answers must never take precedence over authoritative local
	// mappings, even when previous remote lookups have not expired.
	wait := make(chan struct{})
	close(wait)
	for _, stale := range []struct {
		name string
		ip   string
	}{
		{name: "kubernetes.default.svc.", ip: remoteAPIIP},
		{name: localServiceName, ip: remoteServiceIP},
	} {
		server.cache.Store(cacheKey{name: stale.name, qType: dns.TypeA}, &cacheEntry{
			created: time.Now(),
			answer: dnsproxy.RRs{&dns.A{
				Hdr: dns.RR_Header{Name: stale.name, Rrtype: dns.TypeA, Class: dns.ClassINET},
				A:   net.ParseIP(stale.ip),
			}},
			rCode: dns.RcodeSuccess,
			wait:  wait,
		})
	}

	tests := []struct {
		name     string
		query    string
		qtype    uint16
		answerIP string
	}{
		{name: "short API name", query: "kubernetes.default.svc.", qtype: dns.TypeA, answerIP: localAPIIP},
		{name: "fully qualified API name", query: "kubernetes.default.svc.cluster.local.", qtype: dns.TypeA, answerIP: localAPIIP},
		{name: "case-insensitive API name", query: "KUBERNETES.default.svc.", qtype: dns.TypeA, answerIP: localAPIIP},
		{name: "Telepresence search suffix", query: "kubernetes.default.svc.tel2-search.", qtype: dns.TypeA, answerIP: localAPIIP},
		{name: "opposite address family does not leak remotely", query: "kubernetes.default.svc.", qtype: dns.TypeAAAA},
		{name: "additional local service", query: localServiceName, qtype: dns.TypeA, answerIP: localServiceIP},
		{name: "additional local service opposite address family", query: localServiceName, qtype: dns.TypeAAAA},
		{name: "ordinary remote service in preserved namespace", query: remotePlatformName, qtype: dns.TypeA, answerIP: remotePlatformIP},
		{name: "ordinary remote service", query: "web.application.svc.cluster.local.", qtype: dns.TypeA, answerIP: remoteWebIP},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := new(dns.Msg)
			request.SetQuestion(tt.query, tt.qtype)
			response := new(recordingDNSResponseWriter)

			server.ServeDNS(response, request)

			require.NotNil(t, response.message)
			require.Equal(t, dns.RcodeSuccess, response.message.Rcode)
			if tt.answerIP == "" {
				require.Empty(t, response.message.Answer)
				return
			}
			require.Len(t, response.message.Answer, 1)
			answer, ok := response.message.Answer[0].(*dns.A)
			require.True(t, ok)
			require.Equal(t, tt.answerIP, answer.A.String())
		})
	}
	require.Equal(t, []string{remotePlatformName, "web.application.svc.cluster.local."}, remoteQueries)
}

func TestExcludedLocalClusterAPIDoesNotFallBack(t *testing.T) {
	var remoteLookupCalled bool
	server := NewServer(&client.DNS{Excludes: []string{"kubernetes.default.svc"}}, "application",
		func(context.Context, *dns.Question) (dnsproxy.RRs, int, error) {
			remoteLookupCalled = true
			return nil, dns.RcodeSuccess, nil
		})
	server.ctx = testutil.NewContext(t, true)
	server.clientLookup = server.resolveThruCache

	request := new(dns.Msg)
	request.SetQuestion("kubernetes.default.svc.", dns.TypeA)
	response := new(recordingDNSResponseWriter)
	server.ServeDNS(response, request)

	require.NotNil(t, response.message)
	require.Equal(t, dns.RcodeNameError, response.message.Rcode)
	require.False(t, remoteLookupCalled)
}
