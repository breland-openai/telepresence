# Preserve the owning cluster's Kubernetes API DNS

## Problem

A Linux client hosted inside one Kubernetes cluster can connect Telepresence to
another cluster. Both clusters expose their API under
`kubernetes.default.svc.cluster.local`, so the connected cluster can shadow the
owning cluster's API when Telepresence claims `.svc` DNS routing. Exact DNS
exclusions cannot recover the owning API because systemd-resolved routes these
queries to Telepresence without a fallback resolver.

## Design

- Add `dns.preserveLocalClusterDNS`, enabled by default with an explicit
  `false` opt-out.
- Before installing the Telepresence DNS server, query physical Linux resolvers
  directly for `kubernetes.default.svc.cluster.local` within a bounded deadline.
- Ignore loopback, configured virtual DNS addresses, and Telepresence IPv4/IPv6
  virtual subnets. Preserve both the abbreviated and fully qualified API names
  as exact literal-IP mappings when physical DNS answers.
- Preserve explicit user mappings, runtime mapping updates, ordinary remote
  service discovery, opposite-address-family behavior, and DNS cache precedence.
- Preserve the owning API's physical `/32` or `/128` route when its address
  overlaps a proxied remote subnet.
- Limit automatic discovery to Linux and the conventional `cluster.local`
  domain; document the behavior and opt-out.

## Verification

- Hermetic physical-DNS tests for IPv4, IPv6, timeout/no-answer, disabled
  preservation, resolver failover, and virtual resolver exclusion.
- DNS-server tests for colliding local/remote API answers, ordinary remote
  services, explicit mapping precedence, search suffixes, stale cache entries,
  and exclusion-only NXDOMAIN behavior.
- Root-daemon tests for runtime mapping updates, status snapshots, and
  overlapping IPv4/IPv6 route preservation.
- A connected-client regression using a local API listener and a real remote
  service, plus package race tests and the repository's complete lint suite.
