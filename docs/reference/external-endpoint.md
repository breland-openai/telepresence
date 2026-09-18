---
title: External control endpoint
description: The traffic-manager's optional TLS gRPC listener for clients that must not contact the Kubernetes API server.
---

The traffic-manager can publish an optional TLS gRPC listener for clients,
configured with the Helm value `externalEndpoint`. A client configured with
`cluster.managerAddress` dials it directly instead of establishing a
port-forward through the Kubernetes API server, which removes the last
mechanical Kubernetes permission a connection needs: such a client makes no
Kubernetes API requests at all — from either of its daemons.

```yaml
# client config (config.yml or the kubeconfig's telepresence.io extension)
cluster:
  managerAddress: tls://tm.example.com:443
  managerServerCA: /path/to/ca.pem   # PEM, base64 PEM, or file path; omit for public trust
```

## Requirements

- **The external listener always enforces both authentication and authorization.**
  `security.authentication.mode` separately controls the internal listener, so it
  can remain `permissive` while clients migrate. Opening the external listener does
  not weaken its checks or revoke existing port-forward access by default.
- **Server trust must survive manager restarts.** The listener terminates TLS
  with a persisted certificate: either an existing `kubernetes.io/tls` Secret
  (`externalEndpoint.tls.secretName`) or a cert-manager Certificate
  (`externalEndpoint.tls.certManager`). The in-memory QUIC CA is ephemeral by
  design and is not used here.
- The client authenticates with its kubeconfig bearer token, exactly as over
  a port-forward. Reading the kubeconfig and running its exec plugin are
  local operations, not API-server requests.

## An external bearer-token webhook

By default the external listener authenticates bearer tokens using the target
cluster's Kubernetes TokenReview API. If a client's token is instead recognized
by another identity service, configure a standard `authentication.k8s.io/v1`
TokenReview webhook for this listener:

```yaml
externalEndpoint:
  enabled: true
  tls:
    secretName: traffic-manager-external-tls
  authenticationWebhook:
    url: https://identity.example.com/tokenreview
    audiences: [https://identity.example.com/dev-clients]
    credentials:
      serviceAccountTokenAudience: https://identity.example.com/tokenreview
    # For private TLS trust; omit this section to use the system trust store.
    # ca:
    #   secretName: identity-root-ca
    #   secretKey: ca.crt
```

The `audiences` list describes the end-user tokens the webhook can accept. It is
sent explicitly in `spec.audiences`, and an authenticated webhook response must
include at least one of those audiences in `status.audiences`. Kubernetes RBAC
authorization still runs in the target cluster using the returned username,
UID, groups, and extra claims. The webhook is therefore trusted to supply those
claims and should allow only identities entitled to use this cluster.
When a presented JWT contains a readable, nonempty audience that matches none
of those configured audiences, the manager rejects it locally without disclosing
the credential to the webhook. This is only a rejection check: the webhook must
still validate every accepted identity, including the signature and audience.

The `credentials` setting authenticates the **manager itself** in a separate
HTTP `Authorization` header; it is not the end-user token in `spec.token`.
Prefer a projected, one-hour Kubernetes service-account token when the webhook
can verify the cluster's issuer, audience, and manager service account. As an
alternative set `credentials.secretName` and optionally `credentials.secretKey`
(default `token`) to mount an existing Secret containing the caller bearer.
Both token sources are re-read and may rotate without restarting the manager.
A private CA bundle (`ca.secretName` and optional `ca.secretKey`, default
`ca.crt`) is loaded at startup; restart the manager after rotating this bundle.
HTTPS is mandatory and redirects are rejected.

Webhook errors, malformed or wrong-audience responses, and unsuccessful reviews
never fall back to Kubernetes or admit an unauthenticated external client.
Previously verified JWT decisions can be cached for up to two minutes, never
reused past the JWT's declared expiry. Successful opaque-token decisions are
not cached because a standard TokenReview response carries no token expiry.
The existing internal client, agent, and routing-observer authentication remains
unchanged and never uses this webhook. Do not enable an external webhook until
its network access, caller authentication, and target-cluster RBAC are ready.

## The data plane rides QUIC

The external listener carries the control plane. Outbound cluster traffic
can fall back to tunnel streams on the TLS gRPC connection itself, but
agent-bound streams — the delivery path for intercepted traffic and volume
mounts — need a client-to-agent channel, which in external mode is the QUIC
tunnel (`quicTunnel.enabled`): there is no Kubernetes port-forward to fall
back to. Publish the QUIC endpoint alongside the external endpoint.
Without it, an external-only client can connect, browse, and gather logs,
but creating an intercept or ingest fails early with an error naming the
missing channel — the client never creates an attachment whose traffic
has no way to reach the local workstation.

## What the client loses without cluster access

In external-only mode, features that inherently require client-side
Kubernetes access are disabled with explicit errors rather than degraded
silently: the ConfigMap-backed admin commands for revoking intercepts, and
the legacy direct log-gathering path (the manager serves the logs instead).
Agent uninstallation and global log-level changes are limited to the internal
path because the current administrative handlers affect other workloads without
a dedicated admin check. The legacy full-agent watch RPCs are also internal only;
modern direct clients use a namespace-authorized watch that omits container
environment variables. Full details for a particular workload are available only
through the workload-authorized agent operations.
Symbolic service ports are resolved by the manager on the client's behalf,
so they work the same as over a port-forward; only against a
traffic-manager too old to serve that resolution does the client report an
error asking for a numeric port. Namespace discovery always comes from the
manager.

## A restricted surface

The external listener does not serve the manager's internal gRPC surface.
It exposes only the client-facing functionality, restricted to the request
forms an external caller is permitted to use. Everything that only
in-cluster peers need — the traffic-agent's calls and the quic-forwarder
feed — is absent and remains reachable only on the in-cluster listener.

Pre-session, the deliberately public surface is the version handshake and
health checking — nothing else. Every other call requires an authenticated
principal, and every call that names a session verifies that the session
belongs to the caller's identity; possession of a session ID is never
sufficient. Request forms that only in-cluster peers use — an intercept
watch naming an agent session, for example — are rejected.

## Admission controls

The external listener is a `TokenReview` amplification surface: each
previously unseen invalid token can cost the manager up to two reviews.
A cap on concurrent reviews and a rate limiter therefore gate the
`TokenReview` itself, so a cached token passes freely while only an
actual review pays the budget; a maximum credential length, per-connection
stream and message-size limits, and TLS-handshake and
unauthenticated-idle deadlines bound the work before that. The
`telepresence_auth_*` counters (labeled `listener="external"`) make this path observable; see
[External Endpoint Authentication Metrics](../howtos/monitoring.md#external-endpoint-authentication-metrics).
Network-level restrictions (LoadBalancer source ranges, NetworkPolicy)
remain recommended defense in depth.

## Disabling the port-forward path entirely

Setting `externalEndpoint.disablePortForwardRbac: true` stops the chart from granting `pods/portforward` in
any client Role — the connect Role's bootstrap grant and the per-namespace
intercept Roles' direct-agent-dial grant alike — since an external client
never port-forwards. Leave this value false during a mixed-fleet migration. The exception is `security.authorization.requiredGrant:
portforward`, where possession of `pods/portforward` is itself the
authorization policy, so the named grants remain. With any other required
grant, what remains is RBAC granted elsewhere: set
`security.authorization.requiredGrant: telepresence` so an identity holding
`pods/portforward` for unrelated reasons still cannot turn the tunnel into
a session. Wildcard identities such as `cluster-admin` pass every review
and cannot be constrained this way.
