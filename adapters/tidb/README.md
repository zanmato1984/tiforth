# TiDB Adapter

This directory defines the TiDB-facing adapter boundary.

The adapter should eventually translate TiDB concepts into shared specs, data contracts, and runtime expectations without becoming the owner of semantics.

Current checkpoint:

- `adapters/first-expression-slice.md` defines the minimal request and response surface for the first TiDB-versus-TiFlash differential slice
- `adapters/first-filter-is-not-null-slice.md` defines the shared request and response surface for the docs-first differential filter checkpoint
- `docs/design/adapter-milestone-breakdown.md` fixes the next TiDB checkpoint as a single-engine adapter issue before pairwise drift aggregation lands
- `docs/design/adapter-runtime-orchestration-boundary.md` fixes which TiDB session, timeout, retry, cancellation, and diagnostic concerns stay adapter-local for milestone 1
- `crates/tiforth-adapter-tidb` now encodes the first-expression-slice request catalog, TiDB-oriented SQL lowering, and row / error normalization behind a runner boundary
- `crates/tiforth-harness-differential` now exercises that TiDB adapter core alongside the TiFlash adapter core and validates the first checked-in pairwise artifacts under `inventory/`

Next checkpoint:

- wire the execution core to a live TiDB runner so the checked-in pairwise artifacts can move beyond deterministic adapter-core fixture evidence

## TODOs

- extend the request and response surface beyond the first differential expression slice
- document TiDB-specific semantic mismatches found during inventory
