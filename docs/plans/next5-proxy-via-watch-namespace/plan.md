# Require the connected agent watch for a named proxy

1. Extend the existing real embedded-root gRPC fixture so the connected
   namespace and effective agent-watch namespaces can differ, including
   external-manager transport. Add failing controls for a B-only session
   targeting the default connected namespace: no manager EnsureAgent call,
   an immediate actionable error, and no native Kubernetes guidance for an
   external manager. Show that explicit default+B keeps the direct manager
   watch and reaches EnsureAgent. Keep ordinary/local-only B sessions valid.
2. Reject named proxy startup when the effective root agent-watch namespace
   set omits the connected namespace, before spawning a direct watch or
   requesting agent injection. Direct connections give mapping and actual
   namespace-wide Kubernetes permission guidance; external connections give
   mapping guidance only. Preserve ordinary and local translation startup,
   old manager fallback, named workload de-duplication, and real sidecar
   readiness.
3. Run the authentic failing tests before code, the focused cases and full
   relevant native race packages afterward, and the entire ordinary Linux
   make lint. Remove this review plan in the final implementing commit.
