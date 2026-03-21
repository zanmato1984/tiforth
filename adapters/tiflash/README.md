# TiFlash Adapter

This directory defines the TiFlash-facing adapter boundary.

The adapter should eventually connect TiFlash behavior to shared specs and contracts while keeping engine-specific execution details from leaking into the shared semantic layer.

Current checkpoint:

- `adapters/first-expression-slice.md` defines the minimal request and response surface for the first TiDB-versus-TiFlash differential slice
- `adapters/first-filter-is-not-null-slice.md` defines the shared request and response surface for the docs-first differential filter checkpoint
- `adapters/first-temporal-date32-slice.md` defines the shared request and response surface for the docs-first differential temporal `date32` checkpoint
- `adapters/first-temporal-timestamp-tz-slice.md` defines the shared request and response surface for the docs-first differential temporal timezone-aware `timestamp_tz(us)` checkpoint
- `adapters/first-decimal128-slice.md` defines the shared request and response surface for the docs-first differential decimal `decimal128` checkpoint
- `adapters/first-float64-ordering-slice.md` defines the shared request and response surface for the docs-first differential float64 NaN/infinity ordering checkpoint
- `adapters/first-unsigned-arithmetic-slice.md` defines the shared request and response surface for the docs-first differential unsigned `uint64` arithmetic checkpoint
- `adapters/first-json-slice.md` defines the shared request and response surface for the first executable differential JSON comparability/cast checkpoint
- `adapters/first-collation-string-slice.md` defines the shared request and response surface for the first executable differential collation-sensitive string checkpoint
- `adapters/first-struct-slice.md` defines the shared request and response surface for the first executable differential nested struct passthrough checkpoint
- `adapters/first-map-slice.md` defines the shared request and response surface for the docs-first differential nested map passthrough checkpoint
- `adapters/first-union-slice.md` defines the shared request and response surface for the docs-first differential nested union passthrough checkpoint
- `docs/design/adapter-milestone-breakdown.md` fixes the next TiFlash checkpoint as a single-engine adapter issue before pairwise drift aggregation lands
- `docs/design/adapter-runtime-orchestration-boundary.md` fixes which TiFlash session, timeout, retry, cancellation, and diagnostic concerns stay adapter-local for milestone 1
- `crates/tiforth-adapter-tiflash` now encodes first-expression-slice, first-filter-is-not-null-slice, first-temporal-date32-slice, first-temporal-timestamp-tz-slice, first-decimal128-slice, first-float64-ordering-slice, first-unsigned-arithmetic-slice, first-json-slice, first-collation-string-slice, and first-struct-slice request catalogs with TiFlash-oriented SQL lowering and row / error normalization behind runner boundaries
- `crates/tiforth-harness-differential` now exercises that TiFlash adapter core alongside the TiDB adapter core and validates checked-in pairwise first-expression, first-filter, first-temporal-date32, first-temporal-timestamp-tz, first-decimal, first-float64-ordering, first-unsigned-arithmetic, first-json, first-collation, and first-struct artifacts under `inventory/`
- `inventory/first-expression-slice-tiflash-compat-notes.md`, `inventory/first-filter-is-not-null-slice-tiflash-compat-notes.md`, `inventory/first-temporal-date32-slice-tiflash-compat-notes.md`, `inventory/first-temporal-timestamp-tz-slice-tiflash-compat-notes.md`, `inventory/first-decimal128-slice-tiflash-compat-notes.md`, `inventory/first-float64-ordering-slice-tiflash-compat-notes.md`, and `inventory/first-unsigned-arithmetic-slice-tiflash-compat-notes.md` now capture TiFlash-side compatibility notes for the first-expression, first-filter, first-temporal-date32, first-temporal-timestamp-tz, first-decimal, first-float64, and first-unsigned-arithmetic executable differential slices
- `crates/tiforth-harness-differential/src/first_filter_is_not_null_live.rs` now provides an env-configured live TiFlash runner implementation for `first-filter-is-not-null-slice`

Next checkpoint:

- refresh checked-in first-filter differential artifacts from live TiFlash plus TiDB runs when shared review environments are available

## TODOs

- extend the request and response surface beyond the current first differential slices (`first-expression-slice`, `first-filter-is-not-null-slice`, `first-temporal-date32-slice`, `first-temporal-timestamp-tz-slice`, `first-decimal128-slice`, `first-float64-ordering-slice`, `first-unsigned-arithmetic-slice`, `first-json-slice`, `first-collation-string-slice`, `first-struct-slice`, `first-map-slice`, and `first-union-slice`)
- extend TiFlash compatibility notes beyond the current first-expression, first-filter, first-temporal-date32, first-temporal-timestamp-tz, first-decimal128, first-float64-ordering, and first-unsigned-arithmetic slice boundaries as new semantic families land
