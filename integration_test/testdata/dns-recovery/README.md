# Native root daemon DNS recovery reproduction

Run on a disposable Linux VM with `iproute2`, `iptables`, `util-linux`, Python 3,
and an ordinary non-root user. No Kubernetes cluster is used. The harness does
not alter the VM host's DNS, routes, or Telepresence processes: it uses two
network namespaces and a private mount namespace. Namespace configuration under
`/etc/netns/tp-dnsc-PID` is removed on exit.

```sh
sudo python3 integration_test/testdata/dns-recovery/repro.py \
  --binary /absolute/path/to/baseline-telepresence \
  --expect broken --user dev-user --output /tmp/tp-dns-baseline

sudo python3 integration_test/testdata/dns-recovery/repro.py \
  --binary /absolute/path/to/fixed-telepresence \
  --expect recovered --user dev-user --output /tmp/tp-dns-fixed
```

The supplied binary is executed natively for both `rootd` and CLI bootstrap. Its
SHA-256 is recorded. DNS answers come from a controlled UDP server on an isolated
veth peer; public, internal, and authentication fixture names have documentation
IP addresses. The native CLI's kubeconfig invokes an exec-auth DNS probe which
deliberately refuses to supply credentials after recording its result. This
proves the authentication dependency without contacting or changing a cluster.
Each isolated CLI cache is initialized by the native binary before registering
rootd, so the CLI's first-run cache migration cannot shut down the test daemon.

The harness **injects** an orphaned `TELEPRESENCE_DNS` chain with the same legacy
DNAT shape as the implementation, targeting an absent listener. This is an
explicit stale-state fixture, not evidence that a native Telepresence session
installed those rules or that a particular incident used legacy DNS mode.

It verifies:

1. The physical fixture resolver answers all names without interception.
2. An occupied listener's DNAT remains intact across native rootd startup, and an
   unrelated NAT chain is unchanged.
3. The injected stale DNS rules break all names.
4. Native rootd answers its Version RPC, verified with the native CLI's structured
   version output. Baseline leaves DNS broken; a startup cleanup fix restores DNS
   before rootd receives a session `Connect` RPC. Opening a TCP listener is not
   sufficient to pass this readiness barrier.
5. Native CLI exec-auth sees the same state and exits deliberately before any
   rootd session starts.
6. The resulting DNS state survives normal SIGTERM and forced SIGKILL shutdown
   cases. These are idle native daemon lifecycle checks; they do not claim to
   exercise active-session graceful teardown.

Artifacts include structured results, real rootd/CLI logs, the DNS server's query
log, and before/after NAT and route snapshots. A failed assumption fails the run.

For a native Linux source build, use the repository build target with explicit
unique test version, for example:

```sh
TELEPRESENCE_VERSION=v2.31.2-dns-recovery-baseline \
  TELEPRESENCE_REGISTRY=local make build
```

Preserve separate baseline and fixed binary paths. Do not overwrite an installed
Telepresence client or reuse a released version string for modified source.
