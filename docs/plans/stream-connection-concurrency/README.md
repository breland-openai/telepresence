# Stream connection concurrent I/O

The tunnel `NewStreamConn` implements `net.Conn` and is returned to the
traffic-agent HTTP reverse proxy. The existing whole-package Go race gate shows
that a canceled write can return before an asynchronous worker copies the public
caller’s buffer, and close can overlap an active transport send. Native gRPC
allows one sender and one receiver concurrently, but disallows two concurrent
sends, two concurrent receives, and concurrent client send/transport close. The
existing deadline and close channels also allow a second Close or concurrent
deadline setter to panic, and timing out a native receive can lose its late
payload or start a second native receive.

## Implementation

- Limit production changes to `pkg/tunnel/stream_conn.go`; leave the Stream
  protocol, gRPC wrappers, wire format, routing and API unchanged.
- Snapshot the write buffer into an immutable tunnel message before it can be
  retained by any asynchronous owner. Keep one serialized native send owner for
  both payload and transport close. Public blocked operations must observe close
  without waiting for an uncancellable native send. Send calls not accepted by
  the owner before expiry cannot send later; an already started native gRPC send
  may finish since that API ignores per-call contexts.
- Keep one native receive owner. Its buffered or pending message belongs to the
  connection, so a later public Read after deadline renewal receives the same
  inbound bytes. Continue to allow native one-sender/one-receiver parallelism.
- Signal connection closure once with a distinct close channel; synchronize
  read/write deadline updates independently and let changing a future deadline
  affect currently blocked calls without forcing an early timeout. Preserve EOF
  mapping, probes, address APIs and typed timeout/closed errors.
- Use lazy native workers if practical so creating unused connections does not
  introduce receive goroutines; worker lifetime should be bounded by connection
  close or its parent context, with no per-call accumulation.

## Native proof

- Archive exact Go 1.27.1 baseline whole package `go test -race -count=1
  ./pkg/tunnel`, and execute new gated native Stream tests against unchanged
  source first to demonstrate the real failure(s).
- Use a gated recording Stream for immutable writes after cancellation,
  serialized native writes and transport close, immediate unblock of public
  operations, concurrent Close/deadline setters, a timed-out real incoming read
  later retained, and no second native receive; test deadline extension and
  timeout error compatibility.
- Run exact candidate whole package with `-race`, then the production agent
  HTTP-forwarder whole package with `-race`. Run standard `make lint`; any local
  Docker failure should be reported independently with exact command/receipt.
- Remove this review scaffolding in the final source commit. No public write or
  cluster/host interaction is part of this correction.
