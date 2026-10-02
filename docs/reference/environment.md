---
title: Environment variables
description: "How Telepresence can import environment variables from your Kubernetes cluster to use with code running on your laptop."
hide_table_of_contents: true
---

# Environment variables

Telepresence will import environment variables from the cluster container when attaching to it.
You can use these variables with the code running on your laptop.

There are several options available to do this:

1. `telepresence replace <workload> --container <container> --env-file <filename>`

   This will write the environment variables to a file. This file can be used when starting containers locally. The option `--env-syntax`
   will allow control over the syntax of the file. Valid syntaxes are "docker", "compose", "sh", "csh", "cmd", and "ps" where "sh", "csh",
   and "ps" can be suffixed with ":export".

2. `telepresence replace <workload> --container <container> --env-file <filename> --env-syntax=json`

   This will write the environment variables to a JSON file. This file can be injected into other build processes.

3. `telepresence replace <workload> --container <container> -- <command>`

   This will run a command locally with the pod's environment variables set on your laptop.  Once the command quits the `replace` is stopped (as if `telepresence detach <workload>` was run).  This can be used in conjunction with a local server command, such as `python [FILENAME]` or `node [FILENAME]` to run a service locally while using the environment variables that were set on the pod via a ConfigMap or other means.

   Another use would be running a subshell, Bash for example:

4. `telepresence replace <workload> -- /bin/bash`

   This would start the `replace` and then launch the subshell on your laptop with all the same variables set as on the pod.

5. `telepresence replace <workload> --docker-run -- <container>`

   This will ensure that the environment is propagated to the container. Will also work for `--docker-build` and `--docker-debug`.

## Environment values in output

Remote environment values are hidden by default, regardless of the variable name.
Structured output from `list`, `ingest`, and detailed `intercept`, `replace`, and
`wiretap` output retains variable names and replaces each value with `[REDACTED]`.
This applies to JSON, YAML, JSON streams, and the deprecated `--output` formats.
Other metadata and the output structure remain unchanged. Plain text output does
not list environment values; `--debug` and debug logging do not enable them.

Use `--show-env` to include values in structured output. For example:

```sh
telepresence list --format json --show-env
telepresence intercept example --detailed-output --format json --show-env
```

**Warning:** Values can contain credentials. Only enable this flag when the output
destination is suitable for sensitive data.

The following exports are also explicit opt-ins, limited to their destination:

- `--env-file FILE` and `--env-json FILE` write actual values without needing
  `--show-env`. Set `FILE` to `-` to write them to stdout. These flags do not enable
  values in the command's other output. `--env-syntax` alone is not an opt-in.
- `gather-logs --get-pod-yaml` includes pod manifests, including literal environment
  values and other sensitive configuration. `--anonymize` only anonymizes names
  and namespaces; it does not remove values.
- Compose configuration exports require `--show-env`: `compose config`,
  `compose bridge`, `compose publish`, and `compose build --print`. This prevents exporting the
  generated runtime configuration by accident.

Rejected environment values are excluded from error messages even after opt-in.
Runtime environments passed to local commands and Docker remain unchanged.
Commands you launch and application logs can still print their own environment;
Telepresence does not filter their output or scrub previously written log files.

## Telepresence Environment Variables

Telepresence adds some useful environment variables in addition to the ones imported from the attached container:

### TELEPRESENCE_ROOT
Directory where all remote volumes mounts are rooted. See [Volume Mounts](volume.md) for more info.

### TELEPRESENCE_MOUNTS
Colon separated list of remotely mounted directories.

### TELEPRESENCE_CONTAINER
The name of the targeted container. Useful when a pod has several containers, and you want to know which one was attached to by Telepresence.
