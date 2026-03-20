# TiFlash Adapter

This directory defines the TiFlash-facing adapter boundary.

The adapter should eventually connect TiFlash behavior to shared specs and contracts while keeping engine-specific execution details from leaking into the shared semantic layer.

Current checkpoint:

- `adapters/first-expression-slice.md` defines the minimal request and response surface for the first TiDB-versus-TiFlash differential slice
- `adapters/first-filter-is-not-null-slice.md` defines the shared request and response surface for the docs-first differential filter checkpoint
- `adapters/first-temporal-date32-slice.md` defines the shared request and response surface for the docs-first differential temporal `date32` checkpoint
- `adapters/first-decimal128-slice.md` defines the shared request and response surface for the docs-first differential decimal `decimal128` checkpoint
- `adapters/first-float64-ordering-slice.md` defines the shared request and response surface for the docs-first differential float64 NaN/infinity ordering checkpoint
- `adapters/first-json-slice.md` defines the shared request and response surface for the docs-first differential JSON comparability/cast checkpoint
- `adapters/first-collation-string-slice.md` defines the shared request and response surface for the docs-first differential collation-sensitive string checkpoint
- `adapters/first-struct-slice.md` defines the shared request and response surface for the docs-first differential nested struct passthrough checkpoint
- `adapters/first-map-slice.md` defines the shared request and response surface for the docs-first differential nested map passthrough checkpoint
- `docs/design/adapter-milestone-breakdown.md` fixes the next TiFlash checkpoint as a single-engine adapter issue before pairwise drift aggregation lands
- `docs/design/adapter-runtime-orchestration-boundary.md` fixes which TiFlash session, timeout, retry, cancellation, and diagnostic concerns stay adapter-local for milestone 1
- `crates/tiforth-adapter-tiflash` now encodes first-expression-slice, first-filter-is-not-null-slice, first-temporal-date32-slice, first-decimal128-slice, and first-float64-ordering-slice request catalogs with TiFlash-oriented SQL lowering and row / error normalization behind runner boundaries
- `crates/tiforth-harness-differential` now exercises that TiFlash adapter core alongside the TiDB adapter core and validates checked-in pairwise first-expression, first-filter, first-temporal, first-decimal, and first-float64-ordering artifacts under `inventory/`
- `inventory/first-expression-slice-tiflash-compat-notes.md`, `inventory/first-filter-is-not-null-slice-tiflash-compat-notes.md`, `inventory/first-temporal-date32-slice-tiflash-compat-notes.md`, `inventory/first-decimal128-slice-tiflash-compat-notes.md`, and `inventory/first-float64-ordering-slice-tiflash-compat-notes.md` now capture TiFlash-side compatibility notes for the first-expression, first-filter, first-temporal, first-decimal, and first-float64 executable differential slices
- `crates/tiforth-harness-differential/src/first_filter_is_not_null_live.rs` now provides an env-configured live TiFlash runner implementation for `first-filter-is-not-null-slice`

Next checkpoint:

- refresh checked-in first-filter differential artifacts from live TiFlash plus TiDB runs when shared review environments are available

## TODOs

- extend the request and response surface beyond the current first differential slices (`first-expression-slice`, `first-filter-is-not-null-slice`, `first-temporal-date32-slice`, `first-decimal128-slice`, `first-float64-ordering-slice`, `first-json-slice`, `first-collation-string-slice`, `first-struct-slice`, and `first-map-slice`)
- extend TiFlash compatibility notes beyond the current first-expression, first-filter, first-temporal-date32, first-decimal128, and first-float64-ordering slice boundaries as new semantic families land
