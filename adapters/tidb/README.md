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
- `crates/tiforth-harness-differential/src/first_filter_is_not_null_live.rs` now provides an env-configured live TiDB runner implementation for `first-filter-is-not-null-slice`

Next checkpoint:

- refresh checked-in first-filter differential artifacts from live TiDB plus TiFlash runs when shared review environments are available

## TODOs

- extend the request and response surface beyond the current first differential slices (`first-expression-slice` and `first-filter-is-not-null-slice`)
- document TiDB-specific semantic mismatches found during inventory
