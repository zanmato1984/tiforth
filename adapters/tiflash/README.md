# TiFlash Adapter

This directory defines the TiFlash-facing adapter boundary.

The adapter should eventually connect TiFlash behavior to shared specs and contracts while keeping engine-specific execution details from leaking into the shared semantic layer.

Current checkpoint:

- `adapters/first-expression-slice.md` defines the minimal request and response surface for the first TiDB-versus-TiFlash differential slice
- `adapters/first-filter-is-not-null-slice.md` defines the shared request and response surface for the docs-first differential filter checkpoint
- `docs/design/adapter-milestone-breakdown.md` fixes the next TiFlash checkpoint as a single-engine adapter issue before pairwise drift aggregation lands
- `docs/design/adapter-runtime-orchestration-boundary.md` fixes which TiFlash session, timeout, retry, cancellation, and diagnostic concerns stay adapter-local for milestone 1
- `crates/tiforth-adapter-tiflash` now encodes the first-expression-slice request catalog, TiFlash-oriented SQL lowering, and row / error normalization behind a runner boundary
- `crates/tiforth-harness-differential` now exercises that TiFlash adapter core alongside the TiDB adapter core and validates the first checked-in pairwise artifacts under `inventory/`
- `crates/tiforth-harness-differential/src/first_filter_is_not_null_live.rs` now provides an env-configured live TiFlash runner implementation for `first-filter-is-not-null-slice`

Next checkpoint:

- refresh checked-in first-filter differential artifacts from live TiFlash plus TiDB runs when shared review environments are available

## TODOs

- extend the request and response surface beyond the current first differential slices (`first-expression-slice` and `first-filter-is-not-null-slice`)
- document TiFlash-specific semantic mismatches found during inventory
