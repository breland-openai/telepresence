---
title: Resolving Istio ServiceEntry Hosts
description: How to resolve and reach hosts that only the service mesh knows about
hide_table_of_contents: true
---

# Resolving Istio ServiceEntry Hosts

## Overview

Istio can act as a DNS proxy for the workloads in the mesh. When
[DNS proxying](https://istio.io/latest/docs/ops/configuration/traffic-management/dns-proxy/)
is enabled (`ISTIO_META_DNS_CAPTURE`), names declared by Istio resources such as
`ServiceEntry` or `VirtualDestination` are resolved by the Envoy sidecar inside
each pod. Those names do not exist in the cluster's DNS, so they are not
resolvable through an ordinary Telepresence connection.

Telepresence can still resolve and reach them, because both its DNS lookups and
its outbound connections can be made *from inside a meshed pod*: when the client
has a connection to a traffic-agent, DNS lookups are performed by that agent, and
connections routed via `--proxy-via` are dialed by it. With the agent running
next to an Istio sidecar, both are subjected to the mesh.

Three things are required:

1. The traffic-agent must dial the ServiceEntry address range through the mesh.
2. The client must route the ServiceEntry's DNS suffix to cluster lookup.
3. The client must attach to the meshed workload (or route via it with
   `--proxy-via`) so that an agent connection exists.

## Configuring the Traffic Manager

Set the following values when installing or upgrading the traffic-manager:

```yaml
agent:
  serviceMesh:
    # Subnets for which outbound connections made by the traffic-agent must
    # pass through the service-mesh proxy instead of bypassing it. This is
    # Istio's default auto-allocation range for ServiceEntry virtual IPs.
    dialSubnets:
      - 240.240.0.0/16
client:
  dns:
    # The DNS suffixes of your ServiceEntry hosts.
    includeSuffixes:
      - .internal.example
```

The `agent.serviceMesh.dialSubnets` value lists address ranges that only the
mesh knows how to route. Without it, the traffic-agent's connections bypass the
Envoy sidecar, and the auto-allocated virtual IPs are unreachable. The value
will typically mirror what you pass to `--proxy-via` when connecting.

The `client.dns.includeSuffixes` value can also be set per-client in
`config.yml` instead of globally in the chart. The suffix applies to every
matching ServiceEntry hostname; individual hostnames do not need separate
configuration.

## Connecting

Connect with a `--proxy-via` that routes the ServiceEntry range through a meshed
workload that has (or will get) a traffic-agent:

```console
$ telepresence connect --namespace <namespace> --proxy-via 240.240.0.0/16=<workload>
```

After this, hosts that resolve through Istio's DNS proxy are resolvable and
reachable from the workstation:

```console
$ curl http://my-service-entry-host.internal.example/
```

For an address lookup matching a configured `includeSuffixes` entry,
Telepresence selects the traffic-agent belonging to the `--proxy-via` workload,
even when other workloads are attached or the traffic-manager requires complex
DNS lookups. The selected agent's answer is used only when its address belongs
to that workload's configured `--proxy-via` range. The virtual IP is translated
to a locally routed address, and the connection is dialed from the same pod
through the Envoy sidecar.

If multiple workloads have `--proxy-via` ranges, Telepresence tries them in the
order configured. If no selected workload can resolve the name within its own
range, the lookup falls back to the normal traffic-manager resolution path.
Ordinary Kubernetes service names and DNS names outside the explicitly included
suffixes retain their existing resolution behavior.

## Limitations

- An agent connection is required. With a plain `telepresence connect` and no
  attachment or `--proxy-via`, DNS lookups are answered by the traffic-manager,
  which runs without a sidecar and cannot resolve mesh-only names.
- The traffic-agent's own communication with the traffic-manager always bypasses
  the mesh; only DNS and the subnets listed in `agent.serviceMesh.dialSubnets`
  are subjected to it.
