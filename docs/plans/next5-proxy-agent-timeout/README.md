# Named proxy agent startup timeout

## Scope

The native root connection establishes management and networking, and named
`--proxy-via` routes can also require on-demand traffic-agent installation.
Allow that extra operation to use the configured intercept wait without
extending ordinary connections or routes translated locally by `--vnat`.
Apply the same policy when the user daemon reconnects to the root daemon.

Use the client configuration after the traffic manager has been consulted.
Local overrides retain their existing priority. Caller cancellation and any
earlier caller deadline must still stop the operation immediately. If this
combined named-proxy deadline expires, retain the original RPC error and
identify the two configured waits in the diagnostic.

## Verification

- Reproduce a delayed named traffic-agent request failing at the ordinary
  deadline using real local root and manager gRPC transports.
- Verify remote and local timeout precedence, ordinary and local translation
  deadlines, independent caller cancellation, and earlier caller deadlines.
- Verify root-daemon reconnect can survive a delayed named agent and identify
  only its own configured deadline in diagnostics.
- Run client race tests and the standard repository lint target.
