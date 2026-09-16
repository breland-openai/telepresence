# Reuse the host connector without overwriting an active daemon

## Observed failure

The regular Linux Kubernetes Docker coexistence shard first left an unnamed
host connection from another suite, then established a differently named host
and a Docker connection. The host and Docker connections both reported a
successful connect. The Docker workload list succeeded, but the host list
failed before it could run because the root daemon was reconnecting. Root logs
showed two host manager session IDs continuously taking and disconnecting the
same root session. The Docker used a separate in-process root and did not do
this.

All host connector identifiers use the same `userd/daemon.json`. An explicit
connect discovers using the requested name. If the live host entry has a
different name, discovery does not find it. The host launcher then rewrites the
same generic info file before starting another connector process. The previous
process watches for file disappearance, and an overwrite leaves it alive.

## Scope and behavior

### Discovery and selectors

- Consult the well-known host info before launching a connector after name
  discovery misses only for an explicitly requested, required host connect:
  `required && !cr.Docker && !cr.Implicit && cr.Use == nil`.
- Any explicit `--use` keeps its exact current regular-expression selection,
  whether the command's session is required or optional. In particular, `list`
  and `intercept` request a required but implicit session; a nonmatching
  selector must keep the selector error and must not launch a connector, use a
  different entry, or rename it. An optional scoped command such as `quit`
  keeps its idempotent not-connected result when there is no match and must
  not select another connection. Multiple matches keep the ambiguity error.
  A successful explicit selector also remains authoritative: with
  `connect --use old --name new`, the current service can report the selected
  connection's actual old name; do not implicitly change its ownership.
  Docker and required implicit commands without `--use` keep their ordinary
  discovery and absence behavior; a same-name host follows its existing path.
- When an ordinary explicit host connect finds an active session with a
  different name, preserve its process, cached identifier, and root session.
  Report the actual `ConnectInfo.ConnectionName` returned by the service and
  the native command for a scoped disconnect:
  `telepresence quit --use '^<regexp-quoted-actual-name>$'`. Quote the argument
  for the shell if needed. A cached name can identify the uncertain entry in
  an error, but cannot replace the actual RPC name in the active-session case.

### Physical reuse and the shared root

- A local `Connector.Version` response proves that a process is responding,
  not that its user or shared root session is inactive. A successful Status
  can positively report an active user session through `ConnectInfo.SessionInfo`
  and `ManagerVersion`. For inactive reuse, require a successful, complete
  status showing both user-session fields absent and no shared-root session at
  `DaemonStatus.OutboundConfig.Session`. An idle user connector with an active
  or indeterminate shared root must be preserved and reported; it must not
  acquire the root from an old or orphaned owner.
- A Status error, including `FailedPrecondition` while connecting or the root
  is reconnecting, `Unavailable`, `DeadlineExceeded`, cancellation, or a
  malformed/inconsistent response, cannot prove inactivity. Preserve the
  entry and process and report retry or a scoped disconnect. `CheckConnect`
  may independently confirm an active session when normal Status cannot see
  the root, but cannot alone prove inactivity because it does not check the
  service's `connecting` flag. If the existing native APIs cannot supply a
  complete atomic answer, add a narrow connector-state RPC or reservation
  under the existing service mutex instead of inferring inactivity.
- A positively inactive user connector and shared root can reuse their
  existing physical connector port and process; never launch another one.
  Commit its actual successful connection name, Kubernetes context, and
  namespace in the generic info file while preserving the verified port and
  physical owner. In-memory `userClient.SetConnectionInfo` alone is not a
  persistent cache update.

### Coordination and stale cache evidence

- Serialize host ownership across CLI processes with a stable cache-level
  lock and reservation/lease. The lock must live outside `userd`, in the
  cache root or a dedicated sibling directory: `InfoLoader.infoFiles()` treats
  every file in `userd` as a daemon-info entry. Existing individual
  `dos.WithLockedFs` reads and writes do not protect the transition.
- The same reservation must cover the initial discovery decision, the actual
  cold host launch or inactive reuse, Connect RPC dispatch, and final
  successful service identity plus persistent cache ownership (or scoped
  failure). Otherwise two cold callers can each launch a process, or two
  callers seeing the same idle port can rename it for different requests
  before either connects. Compare the exact observed cache identity/generation
  and connector port under the stable lock when reserving, updating, releasing,
  or cleaning up. Never delete or overwrite a newer reservation/owner; audit
  both the launcher's deferred cleanup and `connectSession`, which currently
  deletes the generic info on all Connect errors. An unrelated configuration,
  reconnect, or user RPC failure must not delete a reused connector's info.
  Required competing callers wait until their own caller context is canceled or
  the owner completes; do not impose a shorter lock deadline than a supported
  manager/agent connect. A controlled native cold connect took 233.778 seconds;
  the supported manager/agent budget is five minutes. Optional status remains
  independent of the lock.
- Keep the cache file open and write-locked from reading its identity through
  updating its heartbeat on that same descriptor. Replacement of the pathname
  by an older process must not redirect that update to a different owner.
- Read the generic entry non-destructively under that lock before deciding
  it is absent. Current `LoadInfo` deletes entries older than six seconds
  after a two-second heartbeat and returns a not-found-shaped result; system
  suspension or a racing heartbeat can also produce that observation. Current
  `DialUserDaemon(false)` maps any 500 ms dial deadline to `ErrNoUserDaemon`,
  so a dial timeout, `Unavailable`, or even a single startup-time connection
  refusal cannot establish a dead process. Parser/permission errors, a
  changed cache identity, a fresh or moving heartbeat, and any uncertainty
  must leave the existing entry/process untouched.
- Genuine filesystem absence under the stable lock can launch a host. An
  existing entry can be removed only with positive stale evidence for the
  same identity/generation and port, then an identity-guarded release. A
  verified recorded host process identity and a native dead-process check is
  preferred if introduced; the present `daemon.Info` contains a container PID
  only, and ordinary host launch does not return a host PID to the CLI, so a
  new host process identity must be published and verified by its owner.
  For legacy entries without that identity, fall back conservatively: observe
  an unchanged expired heartbeat and repeated hard loopback `ECONNREFUSED`
  through at least the native startup/cache grace. If that evidence is not
  available, report retry or scoped cleanup and preserve the entry. A timeout
  is never a hard refusal. An idle connector continues its heartbeat.

## Verification and release

- Use a disposable real loopback gRPC connector and isolated cache. Prove an
  active differently named connector preserves the actual name, cached bytes,
  port, and root; a fully disconnected one reuses its physical process; and
  user inactivity with an active shared root preserves both. Exercise Status
  reconnect, connecting, timeout, permission, and malformed/ambiguous state;
  their cache contents and process must remain intact.
- Start two real competing CLI calls with different names both against an
  inactive connector and against a cold empty cache. Prove only one physical
  host connector launches, only a successful reported owner is committed,
  and losing Connect/launcher cleanup cannot delete the winner. Also prove
  an unrelated Connect failure preserves a previously reused connector.
- Exercise a genuinely dead cached connector and genuine filesystem absence;
  keep entries for recent startup refusal, slow but live loopback, an advancing
  heartbeat, and parser/permission failures. Check required implicit `list` or
  `intercept` with nonmatching and ambiguous explicit `--use`, optional
  selector lookup, explicit connect selection, ordinary no-selector discovery,
  and Docker coexistence retain their selection behavior.
- Exercise the dedicated ordinary-user Kubernetes coexistence tests after
  explicitly isolating their one host plus one Docker scenario. Keep the
  existing independent Linux Kubernetes workflow as the release gate.
- Remove this plan from the final implementation commit after review.
