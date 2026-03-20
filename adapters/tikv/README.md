# TiKV Adapter

This directory defines the TiKV-facing adapter boundary.

The adapter should eventually translate TiKV expression and operator behavior
into shared specs and contracts without turning adapter code into the semantic
source of truth.

Current checkpoint:

- the first shared adapter boundary in `adapters/first-expression-slice.md` remains intentionally limited to TiDB and TiFlash
- `adapters/first-expression-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-expression-slice`
- `adapters/first-filter-is-not-null-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-filter-is-not-null-slice`
- `adapters/first-temporal-date32-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-temporal-date32-slice`
- `adapters/first-decimal128-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-decimal128-slice`
- `adapters/first-float64-ordering-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-float64-ordering-slice`
- `adapters/first-temporal-timestamp-tz-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-temporal-timestamp-tz-slice`
- `docs/design/adapter-milestone-breakdown.md` records why TiKV follows the initial TiDB/TiFlash pairwise checkpoint sequence
- `docs/design/adapter-runtime-orchestration-boundary.md` fixes which TiKV environment, timeout, retry, cancellation, and diagnostic concerns should stay adapter-local
- `crates/tiforth-adapter-tikv` now encodes `first-expression-slice`, `first-filter-is-not-null-slice`, `first-temporal-date32-slice`, `first-decimal128-slice`, and `first-float64-ordering-slice` request catalogs with TiKV-oriented SQL lowering and row/error normalization behind runner boundaries
- `crates/tiforth-harness-differential/src/first_filter_is_not_null_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first-filter case set
- `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first-temporal `date32` case set
- `crates/tiforth-harness-differential/src/first_decimal128_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first-decimal `decimal128` case set
- `crates/tiforth-harness-differential/src/first_float64_ordering_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first-float64-ordering case set
- `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv_pairwise.rs` now provides deterministic TiKV pairwise drift rendering for the documented first-temporal `date32` case set
- `crates/tiforth-harness-differential/src/first_decimal128_slice_tikv_pairwise.rs` now provides deterministic TiKV pairwise drift rendering for the documented first-decimal `decimal128` case set
- `crates/tiforth-harness-differential/src/first_float64_ordering_slice_tikv_pairwise.rs` now provides deterministic TiKV pairwise drift rendering for the documented first-float64-ordering case set
- `inventory/first-expression-slice-tikv-compat-notes.md` now captures the first TiKV-side compatibility notes checkpoint for `first-expression-slice`
- `inventory/first-expression-slice-tikv-case-results.json` now captures the first checked-in TiKV single-engine normalized `case-results` evidence for `first-expression-slice`
- `inventory/first-filter-is-not-null-slice-tikv-compat-notes.md` now captures the first TiKV-side compatibility notes checkpoint for `first-filter-is-not-null-slice`
- `inventory/first-filter-is-not-null-slice-tikv-case-results.json` now captures the first checked-in TiKV single-engine normalized `case-results` evidence for `first-filter-is-not-null-slice`
- `inventory/first-temporal-date32-slice-tikv-compat-notes.md` now captures the first TiKV-side compatibility notes checkpoint for `first-temporal-date32-slice`
- `inventory/first-temporal-date32-slice-tikv-case-results.json` now captures the first checked-in TiKV single-engine normalized `case-results` evidence for `first-temporal-date32-slice`
- `inventory/first-decimal128-slice-tikv-compat-notes.md` now captures the first TiKV-side compatibility notes checkpoint for `first-decimal128-slice`
- `inventory/first-decimal128-slice-tikv-case-results.json` now captures the first checked-in TiKV single-engine normalized `case-results` evidence for `first-decimal128-slice`
- `inventory/first-float64-ordering-slice-tikv-compat-notes.md` now captures the first TiKV-side compatibility notes checkpoint for `first-float64-ordering-slice`
- `inventory/first-float64-ordering-slice-tikv-case-results.json` now captures the first checked-in TiKV single-engine normalized `case-results` evidence for `first-float64-ordering-slice`
- `inventory/first-expression-slice-tidb-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiDB-versus-TiKV drift classification and machine-readable sidecar for `first-expression-slice`
- `inventory/first-expression-slice-tiflash-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiFlash-versus-TiKV drift classification and machine-readable sidecar for `first-expression-slice`
- `inventory/first-filter-is-not-null-slice-tidb-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiDB-versus-TiKV drift classification and machine-readable sidecar for `first-filter-is-not-null-slice`
- `inventory/first-filter-is-not-null-slice-tiflash-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiFlash-versus-TiKV drift classification and machine-readable sidecar for `first-filter-is-not-null-slice`
- `inventory/first-temporal-date32-slice-tidb-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiDB-versus-TiKV drift classification and machine-readable sidecar for `first-temporal-date32-slice`
- `inventory/first-temporal-date32-slice-tiflash-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiFlash-versus-TiKV drift classification and machine-readable sidecar for `first-temporal-date32-slice`
- `inventory/first-decimal128-slice-tidb-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiDB-versus-TiKV drift classification and machine-readable sidecar for `first-decimal128-slice`
- `inventory/first-decimal128-slice-tiflash-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiFlash-versus-TiKV drift classification and machine-readable sidecar for `first-decimal128-slice`
- `inventory/first-float64-ordering-slice-tidb-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiDB-versus-TiKV drift classification and machine-readable sidecar for `first-float64-ordering-slice`
- `inventory/first-float64-ordering-slice-tiflash-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiFlash-versus-TiKV drift classification and machine-readable sidecar for `first-float64-ordering-slice`

Next checkpoint:

- refresh TiKV pairwise drift artifacts for `tidb-vs-tikv` and `tiflash-vs-tikv` whenever first-expression, first-filter, first-temporal, first-decimal, or first-float64-ordering case-results evidence changes
- define live TiKV temporal and decimal runner wiring plus refresh workflow when shared environments are available
- add timestamp-timezone single-engine and pairwise adapter/harness execution plus checked-in artifacts on top of `adapters/first-temporal-timestamp-tz-slice-tikv.md`

## TODOs

- extend TiKV executable slice coverage beyond the current first-expression, first-filter, first-temporal-date32, first-decimal128, and first-float64-ordering executable checkpoints, starting with the docs-first `first-temporal-timestamp-tz-slice` boundary
