# Preserve selected host-cluster DNS names

## Problem

A workstation can run inside one Kubernetes network while connecting to a
different cluster. The current DNS preservation feature protects the owning
cluster's Kubernetes API, but another exact service name can exist in both
clusters and must sometimes continue resolving through the host network.

Static DNS mappings cannot discover the correct address independently on each
host, and they do not receive the physical-route protection applied to
automatically discovered host endpoints. Broad DNS suffix exclusions would
interfere with ordinary remote-cluster service discovery.

## Public configuration

Add an optional exact-hostname allowlist:

```yaml
client:
  dns:
    preserveLocalClusterDNSNames:
      - artifact-gateway.platform.svc.cluster.local
```

The list defaults to empty. Existing Kubernetes API preservation remains
unchanged, and the existing `preserveLocalClusterDNS` setting disables all
automatic host-cluster DNS preservation when false.

## Implementation

1. Add the allowlist to client DNS configuration, equality, merge-compatible
   serialization, and snake-case status.
2. Resolve each valid, canonicalized exact hostname independently against
   filtered physical DNS resolvers before tunnel DNS takes over.
3. Retain bounded discovery, IPv4 preference with IPv6 fallback, explicit
   mapping precedence, runtime mapping updates, and existing API aliases.
4. Keep discovered addresses in the existing automatically preserved mapping
   collection so overlapping destinations receive exact physical host routes.
5. Document the configuration and expose it in the traffic-manager Helm
   schema without adding new RPCs or protocol-buffer fields.
6. Verify manager configuration rewrites, Helm rendering, host-name discovery,
   ordinary remote service resolution, IPv4/IPv6 behavior, route preservation,
   and reconnect-safe mapping updates.

## Constraints

- Preserve only explicitly configured exact names; reject suffixes and
  wildcards.
- Leave remote service discovery, mesh lookups, and existing routing behavior
  unchanged.
- Use only generic names and documentation-safe addresses in public source,
  tests, documentation, commits, and pull-request text.
- Remove this temporary design plan in the final implementation commit.
