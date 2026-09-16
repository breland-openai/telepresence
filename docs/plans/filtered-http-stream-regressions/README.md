# Real Kubernetes filtered HTTP stream regressions

The StatefulSet attachment matrix tests raw unfiltered intercept/replacement or
the wiretap's separate direct tunnel. The existing public header-filter suite
tests fresh HTTP/1 connections, and the h2c case stops at the first matched
response. Neither proves the selected HTTP transport's real connection reuse or
cancellation isolation. Add release regressions targeting the traffic-agent's
selected HTTP reverse proxy (`tunnel.NewStreamConn`). No product change is needed.

## Implementation

- Add a dedicated `HeaderFilterStreams` suite in the existing intercept area,
  under `managers.Default`, with public selectors
  `TestIntercept/HeaderFilterStreams/Test_HTTP1ConnectionReuseAndCancellation`
  and `TestIntercept/HeaderFilterStreams/Test_H2CConnectionReuseAndCancellation`.
  The required source Kubernetes shard 2 runs this area automatically. Do not
  apply the suite-level CompatCore label: the exact existing cached selected
  transport was fork-specific commit `6590cf012`, whereas the existing basic
  HeaderFilter and H2C methods already remain in the optional oldest-counterpart
  compat suite. Mixed fleet tests are a separate goal responsibility.
- Use an actual Kubernetes Echo Deployment and Service, default host connection
  with both shortcuts disabled, and actual CLI `--http-header` filtered intercept
  directing matching traffic to a bound per-test local target. H2 explicitly sets
  service `appProtocol: kubernetes.io/h2c`; both the originating client and local
  target speak true HTTP/2 prior knowledge. No mount is needed.
- Implement one suite-local server with native Go `http.Server.ConnContext` that
  allocates a unique physical accepted TCP connection ID. Every locally matched
  response returns its target identity, requested path, received protocol and
  this accepted TCP ID. Keep a synchronized per-path request record and use
  per-request channels for held-request arrival and actual request cancellation.
  Independent direct-local control must confirm the server is serving. Positive
  cluster controls should assert the actual echo response says `Request served by`
  and includes the unique path, and has no local target marker or connection ID.
- Use dedicated reused client transports, drain every healthy body, and send
  unique matched/wrong/missing paths. Assert two selected HTTP/1 requests have the
  same server-side physical connection ID before cancellation and two after it
  share an ID. HTTP/1 cancellation may correctly discard its current TCP
  connection; the next selected request must be usable, without cross-routing.
- Assert two genuine selected H2 streams share the physical target ID; hold a
  third selected stream until the server signals its arrival, prove a concurrent
  matching stream can respond on that same TCP connection, then cancel the held
  originating request and wait for the actual target request context to cancel.
  A subsequent selected request must still use the same physical H2 connection.
  Missing and explicitly wrong values for the same filter header must both reach
  the physical cluster echo and must never appear in the local target's record.
- Avoid sleep as synchronization: channel readiness and context cancellation
  control the held request, with generous explicit bounded waits only as failure
  backstops. Use normal exact attachment detach with bounded deferred cleanup on
  failures, and close target servers/transports without affecting shared fixtures.
- Keep implementation confined to regression suite source plus focused
  clusterless suite helper tests. No shared-host, cluster or external writes.

## Validation

- Native local-only helper tests should prove HTTP1 physical reuse, true H2
  multiplexing/physical reuse, server-held request cancellation and subsequent
  usability. These are labeled helper proof, not Kubernetes E2E.
- Compile the whole public regression binary (`go test -c`) with Go 1.27.1 and
  run the local fixture tests. Do not execute regression `TestMain`/actual cluster
  in this source task; technical Kind has a separate exclusive owner. Report the
  exact two selectors for the later cluster owner and distinguish them from the
  adjacent matrix wiretap selectors.
- Run local normal `make lint` if feasible; source-only trusted lint/public
  workflow and genuine Kubernetes are separate normal gates. Remove this review
  scaffolding in the final implementation commit.
