package intercept

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"
	core "k8s.io/api/core/v1"

	"github.com/telepresenceio/telepresence/v2/pkg/agentconfig"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/cli"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/managers"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/rt"
	"github.com/telepresenceio/telepresence/v2/regression_test/framework/workloads"
)

const (
	recoveryReplicas      = 4
	recoveryHeader        = "x-local-routing-key"
	recoveryKey           = "chav-1-known"
	recoveryOtherKey      = "chav-1-other"
	recoveryTarget        = 60 * time.Second
	recoveryProbeTimeout  = 2 * time.Second
	recoveryProbeInterval = 500 * time.Millisecond
	recoveryStablePeriod  = 5 * time.Second
)

// ManagerRecovery checks each traffic-agent through its own Kubernetes API
// port-forward, which remains independent of the traffic-manager and VPN.
type ManagerRecovery struct{ rt.Suite }

func init() {
	rt.Register(&ManagerRecovery{}, rt.InArea("intercept"), rt.NeedsManager(managers.Default), rt.WithLabels(rt.Slow))
}

func (s *ManagerRecovery) Test_KnownHTTPAndWebSocketAcrossFourReplicas() {
	t := s.T()
	ctx := s.Ctx()
	conn := s.Connect()
	wl := s.Workload(workloads.EchoReplicas("manager-recovery", recoveryReplicas))
	local, marker := newRecoveryLocal(t)
	localPort := local.Listener.Addr().(*net.TCPAddr).Port //nolint:forcetypeassert // httptest listens on TCP
	conn.Intercept(t, wl, cli.Port(localPort, "http"), cli.MountFalse(), cli.HTTPHeader(recoveryHeader, recoveryKey))
	defer func() {
		_, _, err := s.CLI().Run(ctx, "detach", wl.Name, "-n", wl.Namespace)
		if err != nil {
			t.Logf("detach %s: %v", wl.Name, err)
		}
	}()

	replicas := recoveryAgentPods(t, ctx, s.R(), wl)
	for _, replica := range replicas {
		replica.address = recoveryForward(t, ctx, s.R(), wl.Namespace, replica.pod, replica.port)
		t.Cleanup(replica.closeWebSocket)
	}
	client := &http.Client{Timeout: recoveryProbeTimeout, Transport: &http.Transport{DisableKeepAlives: true}}
	defer client.CloseIdleConnections()

	var baseline []recoverySample
	for deadline := time.Now().Add(recoveryTarget); ; {
		baseline = sampleRecovery(ctx, client, replicas, marker)
		if recoveryAllHealthy(baseline, marker) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("four-replica baseline never routed the matching key and WebSocket locally and both controls to their own pods: %+v", baseline)
		}
		time.Sleep(recoveryProbeInterval)
	}

	rt.Mutate(t, rt.ManagerFixture(managers.Default))
	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- rt.RestartManager(rt.Env{Ctx: ctx, T: t, R: s.R()}) }()
	issues := map[string]*recoveryIssue{}
	websocketFailures := make(map[string]int, len(replicas))
	serviceUnavailable := make(map[string]int, len(replicas))
	var managerReady bool
	var managerErr error
	var stableSince, recoveredAt time.Time

	for time.Since(started) < recoveryTarget+recoveryStablePeriod {
		select {
		case managerErr = <-done:
			managerReady = true
			done = nil
		default:
		}
		samples := sampleRecovery(ctx, client, replicas, marker)
		for _, sample := range samples {
			if sample.keyed.status == http.StatusServiceUnavailable {
				serviceUnavailable[sample.pod]++
			}
			if sample.websocket != nil {
				websocketFailures[sample.pod]++
			}
			recordRecoveryIssues(issues, started, sample, marker)
		}
		if managerReady && managerErr == nil && recoveryAllHealthy(samples, marker) {
			if stableSince.IsZero() {
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= recoveryStablePeriod {
				recoveredAt = stableSince
				break
			}
		} else {
			stableSince = time.Time{}
		}
		time.Sleep(recoveryProbeInterval)
	}
	if !managerReady {
		managerErr = <-done
	}
	if managerErr != nil {
		t.Errorf("manager rollout: %v", managerErr)
	}
	if recoveredAt.IsZero() || recoveredAt.Sub(started) > recoveryTarget {
		t.Errorf("all four agents did not stably recover matching-key HTTP, WebSocket, and ordinary traffic within %s", recoveryTarget)
	} else {
		t.Logf("all four agents were stably recovered after %s", recoveredAt.Sub(started).Round(time.Millisecond))
	}
	for _, replica := range replicas {
		t.Logf("%s: explicit HTTP 503 samples=%d, WebSocket heartbeat/reconnect failures=%d",
			replica.pod, serviceUnavailable[replica.pod], websocketFailures[replica.pod])
	}
	for _, key := range slices.Sorted(maps.Keys(issues)) {
		issue := issues[key]
		t.Errorf("%s: %d bad samples; first at %s: %s", key, issue.count, issue.at, issue.first)
	}
}

type recoveryReplica struct {
	pod, address string
	port         int32
	websocket    *websocket.Conn
	sequence     int
}

type recoveryHTTP struct {
	status               int
	body, retry, caching string
	err                  error
}

type recoverySample struct {
	pod                    string
	keyed, ordinary, other recoveryHTTP
	websocket              error
}

type recoveryIssue struct {
	count int
	at    time.Duration
	first string
}

func newRecoveryLocal(t testing.TB) (*httptest.Server, string) {
	t.Helper()
	marker := fmt.Sprintf("chav-1-local:%d", time.Now().UnixNano())
	mux := http.NewServeMux()
	mux.Handle("/hmr", websocket.Handler(func(conn *websocket.Conn) {
		defer conn.Close()
		for {
			var payload string
			if websocket.Message.Receive(conn, &payload) != nil || websocket.Message.Send(conn, marker+":"+payload) != nil {
				return
			}
		}
	}))
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, marker) })
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, marker
}

func recoveryAgentPods(t testing.TB, ctx context.Context, r *rt.Runtime, wl *rt.Workload) []*recoveryReplica {
	t.Helper()
	var last error
	for deadline := time.Now().Add(recoveryTarget); ; {
		var pods core.PodList
		last = r.KubectlJSON(ctx, wl.Namespace, &pods, "get", "pods", "-l", "app="+wl.Name)
		var replicas []*recoveryReplica
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp != nil || !slices.ContainsFunc(pod.Status.Conditions, func(c core.PodCondition) bool {
				return c.Type == core.PodReady && c.Status == core.ConditionTrue
			}) {
				continue
			}
			for _, c := range pod.Spec.Containers {
				if c.Name != agentconfig.ContainerName {
					continue
				}
				for _, port := range c.Ports {
					if port.Name == "http" {
						replicas = append(replicas, &recoveryReplica{pod: pod.Name, port: port.ContainerPort})
					}
				}
			}
		}
		if last == nil && len(replicas) == recoveryReplicas {
			slices.SortFunc(replicas, func(a, b *recoveryReplica) int { return strings.Compare(a.pod, b.pod) })
			return replicas
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d ready traffic-agent pods, found %d: %v", recoveryReplicas, len(replicas), last)
		}
		time.Sleep(time.Second)
	}
}

func recoveryForward(t testing.TB, parent context.Context, r *rt.Runtime, ns, pod string, remotePort int32) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port for %s: %v", pod, err)
	}
	address := listener.Addr().String()
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split port for %s: %v", pod, err)
	}
	_ = listener.Close()
	ctx, cancel := context.WithCancel(parent)
	done := make(chan error, 1)
	go func() {
		_, runErr := r.Kubectl(ctx, ns, "port-forward", "--address=127.0.0.1", "pod/"+pod,
			port+":"+strconv.Itoa(int(remotePort)))
		done <- runErr
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Logf("port-forward %s did not stop after cancellation", pod)
		}
	})
	for deadline := time.Now().Add(20 * time.Second); ; {
		select {
		case err = <-done:
			t.Fatalf("port-forward %s exited: %v", pod, err)
		default:
		}
		if c, dialErr := net.DialTimeout("tcp", address, 250*time.Millisecond); dialErr == nil {
			_ = c.Close()
			return address
		}
		if time.Now().After(deadline) {
			t.Fatalf("port-forward %s did not start", pod)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func sampleRecovery(ctx context.Context, client *http.Client, replicas []*recoveryReplica, marker string) []recoverySample {
	done := make(chan recoverySample, len(replicas))
	for _, replica := range replicas {
		go func() {
			done <- recoverySample{
				pod:       replica.pod,
				keyed:     probeRecoveryHTTP(ctx, client, replica.address, recoveryKey),
				ordinary:  probeRecoveryHTTP(ctx, client, replica.address, ""),
				other:     probeRecoveryHTTP(ctx, client, replica.address, recoveryOtherKey),
				websocket: replica.probeWebSocket(ctx, marker),
			}
		}()
	}
	results := make([]recoverySample, 0, len(replicas))
	for range replicas {
		results = append(results, <-done)
	}
	return results
}

func probeRecoveryHTTP(ctx context.Context, client *http.Client, address, key string) recoveryHTTP {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/", nil)
	if err != nil {
		return recoveryHTTP{err: err}
	}
	if key != "" {
		req.Header.Set(recoveryHeader, key)
	}
	response, err := client.Do(req)
	if err != nil {
		return recoveryHTTP{err: err}
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8192))
	return recoveryHTTP{
		status: response.StatusCode, body: string(body), retry: response.Header.Get("Retry-After"),
		caching: response.Header.Get("Cache-Control"), err: err,
	}
}

func (r *recoveryReplica) probeWebSocket(ctx context.Context, marker string) (err error) {
	defer func() {
		if err != nil {
			r.closeWebSocket()
		}
	}()
	if r.websocket == nil {
		config, configErr := websocket.NewConfig("ws://"+r.address+"/hmr", "http://rtest.invalid/")
		if configErr != nil {
			return configErr
		}
		config.Header.Set(recoveryHeader, recoveryKey)
		dialCtx, cancel := context.WithTimeout(ctx, recoveryProbeTimeout)
		defer cancel()
		r.websocket, err = config.DialContext(dialCtx)
		if err != nil {
			return err
		}
	}
	r.sequence++
	sequence := strconv.Itoa(r.sequence)
	if err = r.websocket.SetDeadline(time.Now().Add(recoveryProbeTimeout)); err != nil {
		return err
	}
	if err = websocket.Message.Send(r.websocket, sequence); err != nil {
		return err
	}
	var answer string
	if err = websocket.Message.Receive(r.websocket, &answer); err != nil {
		return err
	}
	if answer != marker+":"+sequence {
		return fmt.Errorf("WebSocket heartbeat response %q is not from local handler", answer)
	}
	return nil
}

func (r *recoveryReplica) closeWebSocket() {
	if r.websocket != nil {
		_ = r.websocket.Close()
		r.websocket = nil
	}
}

func recoveryAllHealthy(samples []recoverySample, marker string) bool {
	return len(samples) == recoveryReplicas && !slices.ContainsFunc(samples, func(sample recoverySample) bool {
		return !recoveryLocal(sample.keyed, marker) || !recoveryCluster(sample.ordinary, sample.pod) ||
			!recoveryCluster(sample.other, sample.pod) || sample.websocket != nil
	})
}

func recoveryLocal(sample recoveryHTTP, marker string) bool {
	return sample.err == nil && sample.status == http.StatusOK && sample.body == marker
}

func recoveryCluster(sample recoveryHTTP, pod string) bool {
	return sample.err == nil && sample.status == http.StatusOK && strings.Contains(sample.body, "Request served by "+pod)
}

func recordRecoveryIssues(issues map[string]*recoveryIssue, started time.Time, sample recoverySample, marker string) {
	record := func(kind, detail string) {
		key := sample.pod + "/" + kind
		issue := issues[key]
		if issue == nil {
			issue = &recoveryIssue{at: time.Since(started).Round(time.Millisecond), first: detail}
			issues[key] = issue
		}
		issue.count++
	}
	if !recoveryLocal(sample.keyed, marker) && (sample.keyed.err != nil ||
		sample.keyed.status != http.StatusServiceUnavailable || sample.keyed.retry != "1" || sample.keyed.caching != "no-store") {
		record("matching-key", fmt.Sprintf("status=%d body=%q retry=%q cache=%q err=%v", sample.keyed.status,
			sample.keyed.body, sample.keyed.retry, sample.keyed.caching, sample.keyed.err))
	}
	for _, control := range []struct {
		name string
		http recoveryHTTP
	}{{"ordinary", sample.ordinary}, {"other-key", sample.other}} {
		if !recoveryCluster(control.http, sample.pod) {
			record(control.name, fmt.Sprintf("status=%d body=%q err=%v", control.http.status, control.http.body, control.http.err))
		}
	}
}
