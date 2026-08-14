# Preserve the effective DNS lookup timeout

## Problem

When local-cluster DNS preservation creates an effective configuration copy,
DNS server initialization normalizes the default lookup timeout on that copy
instead of the canonical client configuration. Hostname-based root-daemon
lookups can then inherit a zero-duration timeout and cancel immediately.

## Implementation

1. Synchronize the DNS server's normalized lookup timeout back to the canonical
   client configuration immediately after server initialization.
2. Preserve explicitly configured lookup timeouts and existing local mapping
   behavior.
3. Add focused coverage for the default timeout, explicit timeout overrides,
   effective status, and hostname-based resolution when practical.
4. Remove this temporary design plan in the final implementation commit.

## Constraints

- Keep the correction narrowly scoped to root-daemon timeout initialization.
- Leave DNS ownership, host routes, mesh lookups, and protocol definitions
  unchanged.
- Use only generic service names and documentation-safe addresses.
