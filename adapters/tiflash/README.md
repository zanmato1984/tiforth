# TiFlash Adapter

This directory defines the TiFlash-facing adapter boundary.

The adapter should eventually connect TiFlash behavior to shared specs and contracts while keeping engine-specific execution details from leaking into the shared semantic layer.

Current checkpoint:

- `adapters/first-expression-slice.md` defines the minimal request and response surface for the first TiDB-versus-TiFlash differential slice
- `docs/design/adapter-milestone-breakdown.md` fixes the next TiFlash checkpoint as a single-engine adapter issue before pairwise drift aggregation lands
- `docs/design/adapter-runtime-orchestration-boundary.md` fixes which TiFlash session, timeout, retry, cancellation, and diagnostic concerns stay adapter-local for milestone 1

Next checkpoint:

- execute every documented `first-expression-slice` case through a TiFlash bridge and return one normalized `case result` or explicit `adapter_unavailable`

## TODOs

- extend the request and response surface beyond the first differential expression slice
- document TiFlash-specific semantic mismatches found during inventory
