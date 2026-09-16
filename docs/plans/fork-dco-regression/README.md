# Fork DCO regression gate

## Goal

The project fork must automatically verify contributor signoffs before its
Kubernetes regression shards run. Upstream continues to use the external DCO
application under its existing check name and result policy.

## Changes

1. Add a native GitHub Actions job named `DCO (fork)` that runs only in
   `breland-openai/telepresence`. Checkout the exact pull request head with full
   history and no persisted checkout credential.
2. Verify the exact event base and head objects, check the full native Git
   contribution range, and require every non-merge commit to have an actual Git
   `Signed-off-by` trailer with the contributor's author name and valid email.
   Track genuine merge commits as the official DCO application does.
3. Make only this fork's existing regression preflight wait for the native
   check, and accept only its successful completion. Preserve the upstream
   external DCO check and all existing regression shard prerequisites.
4. Exercise the embedded verifier against real temporary Git repositories,
   including valid and invalid contributors, unsigned side-branch ancestors,
   Unicode author identity, exact event objects, genuine merge commits, and
   histories larger than the pull-request API's 250-commit limit.

## Verification

Run the native Git fixture suite and parse the changed workflow locally; run
`make lint` before pushing. The pull request itself must produce a truthful
native DCO verdict and run the existing Kubernetes regression shards through
their normal workflow before merge.
