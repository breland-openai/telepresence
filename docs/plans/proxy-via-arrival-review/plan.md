# Proxy workload startup review

1. Preserve the current serial manager `EnsureAgent` calls and budget one effective
   agent-arrival timeout for each distinct named proxy workload. Leave ordinary,
   empty, duplicate, and local virtual translation routes unchanged.
2. Pass the startup context into synchronous embedded-root work while keeping
   successful root services on their independent session context. Bound manager
   unary calls, cluster-info readiness, and proxy port-forward readiness; clean up
   services if an embedded startup fails.
3. Preserve the RPC error chain through network readiness and add native gRPC and
   embedded-root tests for serial cold names, duplicates, local routes, deadline,
   caller cancellation, and surviving a successful startup-context cancellation.

This is review scaffolding for PR 17 and is removed before the follow-up commit.
