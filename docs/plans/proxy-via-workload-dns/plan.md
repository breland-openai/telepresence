# Resolve service-mesh DNS through the selected proxy-via workload

## Problem

A traffic-manager can require complex DNS lookups while a service-mesh workload
knows additional DNS names that the manager does not. With multiple connected
traffic agents, selecting an arbitrary agent is nondeterministic and can use a
different workload's service-mesh view. The existing owning-cluster Kubernetes
API DNS-preservation behavior must remain unchanged.

## Design

- Identify DNS names using explicitly configured included suffixes and eligible
  address-record query types; do not redirect ordinary Kubernetes cluster names.
- Resolve eligible names through the exact workload selected by `--proxy-via`,
  even when manager policy enables complex DNS lookups.
- Select active workload agents deterministically by workload and connected
  namespace, excluding node agents and unrelated attached workloads.
- Preserve existing DNS address-family behavior, virtual-address translation,
  Kubernetes API ownership mappings, and manager fallback when the selected
  agent cannot resolve the name.
- Document the existing service-mesh feature's deterministic workload selection
  without introducing organization-specific hostnames, CIDRs, or configuration.

## Verification

- Agent-selection tests with multiple simultaneously connected workloads,
  namespace collisions, node agents, and unavailable connections.
- Root-daemon DNS tests for complex lookup, eligible included suffixes, selected
  workload preference, manager fallback, missing records, IPv4/IPv6, and
  unchanged ordinary cluster lookups.
- Preserve and rerun owning-cluster API DNS and overlapping-route regressions,
  focused race tests, the full root-daemon suite, and `make lint`.
- Collect read-only live evidence without reconnecting Telepresence, modifying
  shared cluster configuration, or deploying.
