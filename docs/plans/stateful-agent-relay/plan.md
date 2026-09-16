# StatefulSet agent relay reconciliation

The root daemon receives physically distinct agent sessions for StatefulSet
pods with the same Kubernetes name. It must retain each physical pod, select an
exact namespace and IP for intercepted traffic, and remove only the session
that disappears. A connection, including its port, Kubernetes UID, and QUIC
identity, must never transfer to a replacement pod.

1. Add native tests that report coexisting old and replacement agents in each
   order, verify exact IP routing and waiters, and test individual removal and
   removal callbacks after a client is replaced. Exercise the legacy manager
   full-snapshot watch separately.
2. Use an internal pod identity derived from the Kubernetes pod UID. For an
   older manager that omits it, retain the pod namespace, name, and normalized
   IP as the physical identity. Change both watch snapshots and live clients to
   use that identity, including legacy full-snapshot conversion. Continue
   exposing the actual workload and pod name and using connected agents for
   workload-level selection.
3. Prevent an old client dial/removal callback from deleting a different live
   client created for the same identity. Run native race tests for agentpf and
   related user/root daemon packages, then normal project lint before any push.

This changes internal client state only; the public wire protocol is unchanged.
