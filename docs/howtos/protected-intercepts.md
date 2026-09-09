---
title: Protect requests for a local intercept
description: Intercept one exact local routing key and check how matching and ordinary traffic behave during a disruption.
---

# Protect requests for a local intercept

Use a protected intercept when requests for your local application must not silently
fall back to the cluster application while the local route is recovering. When the
traffic-agent has no live intercept or cannot open the local stream, it returns HTTP
`503` instead. Other requests continue to use their normal route after the agent has
loaded its initial routing state.

## Before you start

- Ask your administrator whether protected local routing is enabled for your workload's
  namespace. It is off by default and requires compatible clients, traffic-managers and
  traffic-agents. If you use an external gateway, it must support the same routing mode.
- Connect as usual; the intercept uses the existing Telepresence session and
  does not need a separate workstation credential.
- Use one HTTP Service port with a pod selector. This example uses a named Service
  port `http` with `appProtocol: http`, and a local application listening on port `8080`.
  See [supported protocols and gateway limitations](../reference/attachments/protected-intercepts.md#protocol-and-gateway-limits)
  before using a different protocol or testing a direct application port.

## Create and check the intercept

1. Start your local application. Give it a response or header that distinguishes it
   from the cluster application.
2. Connect and create an intercept with exactly one literal routing key:

   ```shell
   telepresence connect --namespace development
   telepresence intercept cart-local --workload cart --service cart \
     --port 8080:http --http-header X-Local-Routing-Key=dev-alex
   ```

   Do not add a path filter, a second header filter, or another target port to this
   intercept. Wait for the CLI to report `ACTIVE`. Use `telepresence list` to inspect
   it again if it does not become active.

3. From your connected workstation, compare a matching request with two controls:

   ```shell
   curl -i -H 'X-Local-Routing-Key: dev-alex' http://cart.development.svc.cluster.local/
   curl -i http://cart.development.svc.cluster.local/
   curl -i -H 'X-Local-Routing-Key: some-other-key' http://cart.development.svc.cluster.local/
   ```

   The first request should show the local application's marker. The other requests
   should follow their ordinary route unless another intercept selects them. If you
   normally enter through a gateway, repeat the checks against that URL.

4. In a disposable environment, stop the local application and repeat the matching
   request. It should not return the cluster application's success response. A
   temporary Telepresence `503` is expected when the agent cannot open the local
   stream; a gateway with no usable backend can return its own error. Restart the
   local application and retry. Check the ordinary requests separately.

5. When finished, remove the route explicitly:

   ```shell
   telepresence detach cart-local
   ```

   After removal has reached the agents, the former routing key follows the normal
   route again. Closing a terminal or losing the network does not reliably remove a
   durable route. If detach fails, reconnect and retry rather than assuming the route
   was removed.

For configuration, startup behavior, and the scope of protection, see the
[Protected local intercepts reference](../reference/attachments/protected-intercepts.md).
