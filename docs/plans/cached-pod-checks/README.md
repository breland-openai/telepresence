# Pilot cached pod verification

For a sidecar intercept on a stable workload, try the existing shared pod and
controller informers before opening a warning watch or reconciling evictions.
Limit this path to verified native ownership, the complete expected replica
count, matching desired configurations, and at least one connected agent with
the physical pod's name and UID. Require a recent watch bookmark and an applied
store revision for each resource read. A disconnect, a lagging store, a missing
owner, a pending replacement, or an unavailable informer sends the request
through authoritative reconciliation. The pilot adds no informer or permission.
Enable the path explicitly for experiments.

Keep eviction decisions and replacement readiness on the authoritative path.
Only Kubernetes watch events and relists may remove pods from the informer.
Treat transient failures during shared-Service expansion separately. Preserve
the requested identity and return a retryable failure if verification cannot
finish within the request budget.

Count Kubernetes requests on the same healthy workload with the pilot disabled
and enabled. Exercise disconnects, unregistered or uninjected replicas, stale
controller and pod UIDs, config drift, and accepted evictions before testing
with a disposable shared Service. Compare routing to both workloads and restore
the manager after the experiment.
