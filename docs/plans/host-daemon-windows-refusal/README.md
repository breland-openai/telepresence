# Windows native host daemon refusal

The host ownership preflight may remove a descriptor only after the same stale
physical descriptor and heartbeat survive repeated, positively refused loopback
connections. Go's synthetic Windows POSIX `syscall.ECONNREFUSED` does not equal
Winsock's native connection-refused error, and the normal Windows runner fails
the genuine closed-loopback test. Timeout, cancellation, access denial, network
failure, uncertain service, or an advancing heartbeat must continue to preserve
the existing host daemon.

Use a small platform-specific refusal classifier: native Unix `ECONNREFUSED`,
native Windows `WSAECONNREFUSED`, both through `errors.Is` so `net.OpError` and
syscall wrappers are supported. Give a Windows closed-loopback probe a bounded
opportunity to produce a hard answer without extending an incoming caller's
deadline or making macOS/Linux named connects slower. Add direct native/wrapped
Windows classification with explicit negative timeout, unreachable, access and
synthetic errors, and make the genuine TCP test report the unexpected refusal
class. Keep genuine timeout and stale/advancing heartbeat controls; run targeted
macOS races, Windows cross-compilation, normal project Linux `make lint`, and
then the ordinary real GitHub Windows suite before accepting the fix.
