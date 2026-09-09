---
title: Protected local intercepts
description: Eligible routing keys, agent and gateway behavior, Helm settings, and limits for durable local HTTP intercepts.
---

# Protected local intercepts

A protected local intercept persists the intended destination of an exact
`X-Local-Routing-Key`. When a traffic-agent knows the key belongs to a local
intercept, it forwards matching HTTP requests only to that intercept. If the local
route has no live intercept or its local stream cannot open, the traffic-agent returns
HTTP `503 Service Unavailable` with `Retry-After: 1` and `Cache-Control: no-store`;
it does not retry the request against the cluster application. The feature is disabled
by default. See the
[how-to](../../howtos/protected-intercepts.md) for a command and checks.

## Eligible requests and targets

A protected intercept must use:

- Exactly one `X-Local-Routing-Key` header filter with a nonempty literal value.
  The HTTP header name is case-insensitive; the value is an exact match.
- One HTTP target port carried over TCP, with no additional header or path filters,
  extra intercepted ports, wiretap, or container replacement.
- An [authenticated client session](../authentication.md), including in permissive
  authentication mode. The key selects traffic; it is not an authentication credential.
  It is stored in a Kubernetes ConfigMap, so do not use it to carry a secret.
- A pod selector when targeting a Kubernetes Service. Multiple selected workloads can
  participate in the same intercept; the manager requires route protection from the
  selected pods instead of assuming that one healthy replica is enough.

In a namespace with durable routing enabled, an intercept using this header in an
unsupported filter combination is rejected. Intercepts using other headers retain
their existing behavior. Namespaces outside an explicit Helm namespace list also
retain their existing behavior.

## Activation, startup, and removal

The traffic-manager persists the desired route before acknowledging creation. It does
not mark the runtime intercept `ACTIVE` until the eligible traffic-agents have
acknowledged installing the current route and, by default, an authorized gateway
controller has acknowledged its configuration. An unavailable participant, missing
acknowledgment, or unverifiable Service state prevents activation. `ACTIVE` is not a
health check for the application running on the workstation.

The first activation requires acknowledgments from all selected pods that have not
been deleted or started terminating, as well as any endpoints still serving traffic.
After activation, a new pod that cannot yet serve traffic does not interrupt the
existing route. Any pod that is Ready or still serving must acknowledge the route.
A change to the gateway's Service configuration requires a new acknowledgment.

A new traffic-agent configured for protected routes does not become Ready until it
has loaded its initial authoritative route state. If a connection nevertheless reaches
one of its protected ports before that load, the agent returns `503` for **any**
nonempty local routing key; requests without the header can still use the normal
route. After the initial load, keys that do not select a protected intercept on that
target follow normal routing. An established agent retains the routes it already
knows while reconnecting to the manager.

`telepresence detach <intercept-name>` records an explicit removal. After the removal
reaches the traffic-agents, the former key follows normal routing. Losing the client
connection, or an agent or manager restarting, is not evidence of removal. Requests
may fail until the route is restored or explicitly removed; there is no guaranteed
recovery time. Reusing an intercept name does not allow a delayed removal for its
previous instance to remove the replacement.

## Protocol and gateway limits

| Traffic path or protocol | Protection available |
|---|---|
| Declared clear HTTP/1.x and clear WebSocket upgrades (`http`, `http1`, `ws` and their supported aliases) | The traffic-agent can mediate requests continuously. With strict sidecar routing, the init container also redirects the original application port for these protocols, including named Service target ports. |
| Declared clear HTTP/2 (`h2c` or `kubernetes.io/h2c`) | The traffic-agent can mediate traffic that reaches its listener. The original named application port does not receive the same strict redirect, and the coordinated gateway currently does not acknowledge this protocol. |
| Declared `https`, `http2`, `wss`, `kubernetes.io/wss`, or `grpc` | The traffic-agent can mediate only if it has loaded the application's downstream TLS certificate. The coordinated gateway currently does not acknowledge these protocols. For clear-text gRPC use an explicit `h2c` declaration. See [Protocol selection](protocols.md) and [Intercept TLS/mTLS applications](../../howtos/mtls.md). |
| Undeclared or opaque TCP, custom protocols, and UDP | The traffic-agent does not acknowledge protected HTTP routing for these ports. Ordinary traffic keeps its normal behavior. |

External gateway reconciliation is a separate integration and is not installed by
this Helm setting. The coordinated gateway currently supports declared clear HTTP/1.x
and clear WebSocket upgrades. Its acknowledgment means Kubernetes accepted the
controller's configuration; it does not prove that every proxy has received it or
that an application endpoint is healthy. When there are no usable endpoints, the
gateway can return its own error before reaching an agent.

The decision applies to each new HTTP request and to a fresh WebSocket upgrade.
A WebSocket that is already upgraded is not reclassified or moved to a different
destination; it may close during a disruption and must be reconnected by its client.

## Helm configuration

| Helm value | Default | Meaning |
|---|---|---|
| `routeIntent.enabled` | `false` | Stores and distributes desired routes in the traffic-manager namespace. Requires permissive or enforcing [authentication](../authentication.md). |
| `routeIntent.namespaces` | `[]` | Limits the feature to the listed managed workload namespaces. Empty means all namespaces already managed by the traffic-manager. |
| `routeIntent.requireAuthoritativeAgents` | `false` | Configures injected traffic-agents to require authoritative route state and apply strict routing. Enable only after the serving managers support the protocol; refresh workload pods to install the configured agent. |
| `routeIntent.controllerServiceAccounts` | `[]` | Full authenticated service account identities allowed to watch routes globally and acknowledge gateway configuration. With an empty list no gateway controller can do so. |
| `routeIntent.skipGatewayAck` | `false` | Allows activation without a gateway acknowledgment only when explicitly enabled. Intended for installations with no local-routing gateway; it provides no external gateway protection. |
| `routeIntent.agentImage` | `""` | Optional traffic-agent image used only in the selected namespaces; an empty value keeps the normal agent image. |
| `agent.initContainer.enabled` | `true` | Required when `routeIntent.requireAuthoritativeAgents` is enabled. The init container also needs its network administration capability; see [Cluster configuration](../cluster-config.md). |

For a namespace with a compatible external gateway controller, an example is:

```yaml
security:
  authentication:
    mode: permissive
routeIntent:
  enabled: true
  namespaces: [development]
  requireAuthoritativeAgents: true
  controllerServiceAccounts:
    - system:serviceaccount:gateway-system:local-route-controller
```

Upgrade clients, traffic-managers, traffic-agents, and the gateway controller before
enabling the feature for developer traffic. The controller identity in the example
must be replaced with the identity actually used by the gateway.
