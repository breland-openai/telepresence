# Next5 fork refresh and staging validation

## Objective

Start the next5 release fork from the current upstream release branch,
retain every upstream change, and integrate the still-relevant next4 fork
patches. Build matching staging server and client artifacts and prove the
combined implementation in an isolated cluster and on a fresh host.

## Implementation

1. Preserve the upstream and next4 histories. Resolve overlapping changes
   using both behavioral contracts, especially session ownership and
   shared intercepts, TokenReview decisions, root DNS recovery and error
   handling, connected agent fallback, client compatibility, and agent
   readiness, drain, and recovery. Regenerate protocol and documentation.
2. Run focused Go tests, broad unit coverage, protocol and chart golden
   checks, and repository lint. Build the staged multi-platform server
   image, Linux client, and Helm chart from the reviewed source. Verify
   versions, revisions, hashes, runtime identities, and image provenance.
3. Verify the existing cluster resources and upstream chart migration.
   Pin the new chart and image only for the selected staging cluster and
   align integration that refers to the manager workload. Dry-run the
   cluster scope before the rollout.
4. Create a fresh host, install the matching client, and validate DNS,
   transports, manager/agent health, intercepts, reconnect/recovery, and
   cluster-to-local routing. Start a representative application locally,
   then prove selected traffic reaches it while ordinary staging traffic
   still reaches staging. Record application readiness and controller
   state alongside transport checks.

## Migration and fallback

The upstream chart migrates the manager Deployment to a one-replica
StatefulSet and cannot surge an update. Confirm the intended manager gap
is bounded and clients recover. The old chart and image remain pinned on
other clusters; preserve the prior coordinates and follow the
upstream-supported uninstall/reinstall fallback only if necessary to
recover the selected staging target.
