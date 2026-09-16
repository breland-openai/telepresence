# Preserve requested agent configuration across deferred pod eviction

## Evidence and objective

Two independently run public Kubernetes shards reproduce the same three leaves:
replacing the echo container in both ordinary and headless singleton StatefulSets
times out at 60 seconds; uninstalling an injected singleton Deployment reports
success but still lists its agent after 120 seconds. In the prerequisite run,
both replacements follow a successful initial intercept and ingest immediately;
the Deployment uninstall starts in the same second as its successful first
intercept and detach. Both StatefulSet fixtures use a real StatefulSet, and all
three workload manifests have no injection annotation under the default OnDemand
policy.

The mutator waits for an accepted eviction's newly owned physical Pod to become
Kubernetes Ready before allowing another eviction. It currently returns success
without preserving a request it deferred while that replacement is pending.
There is no Pod reconciliation handler, and the workload handler ignores an
unannotated OnDemand template. Named uninstall deletes the in-memory config and
only calls the mutator once. The manager also accepts an already registered
sidecar selected by workload/name without checking its physical Pod's actual
sidecar annotation against the new requested replacement policy. As a result,
application-level agent registration before Kubernetes marks the Pod Ready can
leave later desired configuration permanently unscheduled and can prematurely
return an incompatible agent. Official artifacts contain client tails and
Kubernetes abnormal events, but do not upload the manager log at these times;
the before/after fake API and a focused real test will establish the timing
directly rather than assigning every StatefulSet timeout from an inferred trace.

## Implementation

1. Retain explicit per-workload eviction intent (configuration or uninstall
   tombstone) together with the workload UID. Initial requests retain the
   existing synchronous first attempt and return immediate API/RBAC errors to
   their caller. A workqueue owned by the watcher continues any pending intent;
   requests for the same identity coalesce and newer authoritative desired
   configuration supersedes the older intent. A nil desired map alone must not
   grant permission to evict unrelated unrequested workloads. A named and an
   all-agents uninstall record tombstones only for identified workloads; the
   pre-delete Helm bulk/drain path retains its explicit independent waiting.
2. Extend the Pod informer to retain just the PodReady condition and register
   add/update/delete handlers, including deleted-final-state tombstones.
   Handlers enqueue only workloads with active recorded intent using exact
   direct owner refs for StatefulSets/standalone ReplicaSets or injected Pod
   workload labels for indirect controllers. Reconcile performs authoritative
   owner/UID verification with the existing helpers before eviction; unknown or
   unrelated Pod events must not evict anything. A delayed queue check is also
   retained whenever an intent remains pending, so a missed transition or a
   deletion without a later event cannot strand it. After any existing accepted
   batch guard releases, completion requires an authoritative native owned Pod
   list, and an uncertain list retains the request. Explicit all-agent removal
   also discovers native agent Pods in its connected namespace when caches lag.
   Native physical Pods and their direct queue events require a Kubernetes
   controller owner; matching Telepresence labels alone must never grant an
   orphaned or bare Pod eviction. Keep intermediary controller and disabled-kind
   historical cleanup semantics, and leave non-mutation caches unchanged.
3. Continue to observe existing accepted-batch guards, exact physical Pod UID
   preconditions, full owned Ready capacity, workload rollout, zero replicas,
   manually injected Pods, and normal PDB handling. Active batches are never
   reset by a newer intent. Workloads replaced under the same name invalidate
   the old intent before any action. Normal pending state uses a bounded delay;
   transient network/server/PDB errors use rate-limited retry; authorization or
   invalid-object errors stop that attempt and emit clear logs, with subsequent
   relevant events or an explicit new request able to retry. Watcher/namespace
   shutdown discards its owned work and preserves the existing Helm drain path.
4. When ensuring an injectable sidecar, select an agent only if a Kubernetes
   GET confirms the candidate physical Pod name/namespace has the same exact
   UID, is not terminating, and carries the configuration annotation matching
   the requested immutable sidecar configuration, including replacement
   policy. First injection may still use registration without waiting for
   Kubernetes Ready; only a subsequent mutator disruption requires that guard.
   A fresh matching existing agent remains fast. An incompatible candidate never
   satisfies the wait; a bounded recheck handles transient Pod lookups without
   relying on another registration update. Permanent access failures should
   surface clearly; the node-agent and manually injected disabled-injector paths
   remain unchanged. No new manager/agent RPC field or version gate is needed.
   Timeout cleanup must atomically require the exact still-published expected
   configuration and exact live workload UID, so an older waiter cannot erase a
   newer desired configuration or the same-named recreated workload. Injectable
   sidecar waits also use a per-workload ownership cohort so the older timeout
   cannot remove a byte-identical configuration while a younger caller is still
   waiting or already succeeded. Successful protection includes the physical
   workload UID. Release cohorts on every outcome and remove retired entries;
   a cohort where every qualifying arrival waiter fails drops its own confirmed
   configuration exactly once. A younger pre-publication failure cannot veto an
   older qualifying cleanup; a younger published configuration remains protected
   if its direct eviction fails, so the normal queue can continue it. After
   detaching a replacement, dispatch the pod eviction only after the atomic map
   update publishes the restored configuration; assert that a single finalizer
   with no active batch or future Pod event schedules the correct restoration.
   Restoring a persisted node-agent intercept must attach only the node-job
   cleanup and watch, matching fresh node intercept semantics: it must not run
   a sidecar restoration finalizer or touch a concurrent sidecar for the same
   workload. Retain ordinary restored sidecar and node-job cleanup coverage.
5. Preserve immutable Sidecar snapshots across concurrent manager updates by
   correcting `Sidecar.Clone` to deep-copy Containers, Intercepts, PullSecrets,
   MeshDialSubnets, sidecar and container mount policies, mount paths, and the
   native Kubernetes resource and security structures. Preserve nil fields.
   Standalone sequential tests must reject changed original identities/data, and
   a concurrent read/clone test must be clean under the Go race detector. Root
   independently approved this essential invariant after existing shallow slice
   writes were surfaced and verified directly in production mutator Update.

## Verification

- Add a deterministic fake Kubernetes API/informer test red before this change:
  initial injection accepts Pod A, the newly registered Pod B is not Ready,
  and a single subsequent replace/uninstall call must not evict B yet; after
  making B Ready, without making a second request, assert precisely B is evicted
  with its physical UID and no unrelated Pod is evicted. Exercise both kinds
  and an explicit absent-config uninstall tombstone. Include superseding intent,
  same-name workload new UID, termination/delete events with no later event,
  missing events via delayed retry, manual Pods, capacity and PDB/API failures.
- Add manager state tests that an already registered old-policy agent is never
  returned on replace, a wrong physical Pod UID is rejected, a transient Pod
  lookup is rechecked, and a registered matching/new agent is returned; preserve
  node-agent behavior and the no-experiment compatibility of the RPC.
- Re-run existing eviction/drain and manager state tests, then race tests and
  full project `make lint` using Go 1.27.1. Before external source publication,
  run normal lint and genuine user-facing focused regression leaves against an
  exclusively task-owned Kind technical cluster as available; never mutate any
  staging or default technical cluster.
- Remove this temporary plan in the last implementation commit, as required by
  the repository guidance.
