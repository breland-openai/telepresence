# External manager TokenReview webhook

## Goal

Let a deployment authenticate direct client bearer tokens through a standard
Kubernetes v1 TokenReview webhook, without requiring provider-specific code or
changing authentication on the existing internal listener.

## Design

1. Add a manager and Helm configuration for an HTTPS webhook, accepted user
   audiences, and a separate rotating service-account token or Secret credential
   for the manager; allow optional custom CA trust. Bound requests and reject
   redirects, invalid users and failed or mismatched reviews without fallback.
2. Protect credentials before disclosure where a bearer JWT clearly identifies a
   disjoint recipient, while still requiring the webhook for authentication.
   Bound cache reuse by a recognizable token's expiry; keep opaque tokens fresh.
3. Enforce authentication, resource authorization, and session ownership on the
   direct listener independently of the existing listener. Preserve native agent
   authentication and normal port-forward RBAC for mixed client fleets.
4. Audit the externally exposed RPCs under mixed-mode assumptions: isolate
   unbound sessions; do not accept client-provided agent identities or stream
   container environments; protect namespace/workload access and restrict
   administrative operations without a suitable per-target authorization policy.
   Validate malformed requests and prevent handler panics from stopping service.
5. Document operator configuration and retained limitations. Exercise auth and
   manager tests, chart and CLI schema tests, Terraform consumer contract, and
   repository lint before publication. Cluster deployment and provider setup are
   separate from this change.
