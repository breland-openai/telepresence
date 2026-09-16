# Rolling ingest pod identity and session-event delivery

1. Preserve the manager's physical pod identity in both the combined
   agent-pod projection and the legacy full-agent projection. Prefer a known
   physical UID when matching the ingest and replacement candidates; use
   normalized name and IP when either side omits UID, and retain name alone
   only when no comparable physical identity remains.
2. Publish combined agent-pod deltas to the root relay before scheduling
   ingest lifecycle work. Keep that lifecycle serialized on its own consumer
   so a root readiness or manager refetch wait cannot prevent later deltas
   from reaching the relay or intercept consumer. Coalesce full snapshots and
   wait for processing to stop before the final cleanup.
3. Prove same-name pod churn and stale-session removal, backward fallback,
   and relay progress while ingest access is deliberately blocked. Run the
   user-daemon package with the race detector and the normal lint gate before
   any source push. This source task does not use a Kubernetes cluster.
