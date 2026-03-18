# TiDB Adapter

This directory defines the TiDB-facing adapter boundary.

The adapter should eventually translate TiDB concepts into shared specs, data contracts, and runtime expectations without becoming the owner of semantics.

Current checkpoint:

- `adapters/first-expression-slice.md` defines the minimal request and response surface for the first TiDB-versus-TiFlash differential slice
- `docs/design/adapter-milestone-breakdown.md` fixes the next TiDB checkpoint as a single-engine adapter issue before pairwise drift aggregation lands
- `docs/design/adapter-runtime-orchestration-boundary.md` fixes which TiDB session, timeout, retry, cancellation, and diagnostic concerns stay adapter-local for milestone 1

Next checkpoint:

- execute every documented `first-expression-slice` case through a TiDB bridge and return one normalized `case result` or explicit `adapter_unavailable`

## TODOs

- extend the request and response surface beyond the first differential expression slice
- document TiDB-specific semantic mismatches found during inventory
