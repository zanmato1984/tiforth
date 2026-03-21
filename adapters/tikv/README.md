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
- `adapters/first-unsigned-arithmetic-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-unsigned-arithmetic-slice`
- `adapters/first-temporal-timestamp-tz-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-temporal-timestamp-tz-slice`
- `adapters/first-union-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-union-slice`
- `docs/design/adapter-milestone-breakdown.md` records why TiKV follows the initial TiDB/TiFlash pairwise checkpoint sequence
- `docs/design/adapter-runtime-orchestration-boundary.md` fixes which TiKV environment, timeout, retry, cancellation, and diagnostic concerns should stay adapter-local
- `crates/tiforth-adapter-tikv` now encodes `first-expression-slice`, `first-filter-is-not-null-slice`, `first-temporal-date32-slice`, `first-temporal-timestamp-tz-slice`, `first-decimal128-slice`, `first-float64-ordering-slice`, `first-unsigned-arithmetic-slice`, and `first-union-slice` request catalogs with TiKV-oriented SQL lowering and row/error normalization behind runner boundaries
- `crates/tiforth-harness-differential/src/first_filter_is_not_null_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first-filter case set
- `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first-temporal `date32` case set
- `crates/tiforth-harness-differential/src/first_decimal128_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first-decimal `decimal128` case set
- `crates/tiforth-harness-differential/src/first_float64_ordering_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first-float64-ordering case set
- `crates/tiforth-harness-differential/src/first_unsigned_arithmetic_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first-unsigned case set
- `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first timestamp-timezone case set
- `crates/tiforth-harness-differential/src/first_union_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first union case set
- `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv_pairwise.rs` now provides deterministic TiKV pairwise drift rendering for the documented first-temporal `date32` case set
- `crates/tiforth-harness-differential/src/first_decimal128_slice_tikv_pairwise.rs` now provides deterministic TiKV pairwise drift rendering for the documented first-decimal `decimal128` case set
- `crates/tiforth-harness-differential/src/first_float64_ordering_slice_tikv_pairwise.rs` now provides deterministic TiKV pairwise drift rendering for the documented first-float64-ordering case set
- `crates/tiforth-harness-differential/src/first_unsigned_arithmetic_slice_tikv_pairwise.rs` now provides deterministic TiKV pairwise drift rendering for the documented first-unsigned case set
- `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv_pairwise.rs` now provides deterministic TiKV pairwise drift rendering for the documented first timestamp-timezone case set
- `crates/tiforth-harness-differential/src/first_union_slice_tikv_pairwise.rs` now provides deterministic TiKV pairwise drift rendering for the documented first union case set
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
- `inventory/first-unsigned-arithmetic-slice-tikv-compat-notes.md` now captures the first TiKV-side compatibility notes checkpoint for `first-unsigned-arithmetic-slice`
- `inventory/first-unsigned-arithmetic-slice-tikv-case-results.json` now captures the first checked-in TiKV single-engine normalized `case-results` evidence for `first-unsigned-arithmetic-slice`
- `inventory/first-temporal-timestamp-tz-slice-tikv-compat-notes.md` now captures the first TiKV-side compatibility notes checkpoint for `first-temporal-timestamp-tz-slice`
- `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json` now captures the first checked-in TiKV single-engine normalized `case-results` evidence for `first-temporal-timestamp-tz-slice`
- `inventory/first-union-slice-tikv-case-results.json` now captures the first checked-in TiKV single-engine normalized `case-results` evidence for `first-union-slice`
- `inventory/first-union-slice-tikv-compat-notes.md` now captures the first TiKV-side compatibility notes checkpoint for `first-union-slice`
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
- `inventory/first-unsigned-arithmetic-slice-tidb-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiDB-versus-TiKV drift classification and machine-readable sidecar for `first-unsigned-arithmetic-slice`
- `inventory/first-unsigned-arithmetic-slice-tiflash-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiFlash-versus-TiKV drift classification and machine-readable sidecar for `first-unsigned-arithmetic-slice`
- `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiDB-versus-TiKV drift classification and machine-readable sidecar for `first-temporal-timestamp-tz-slice`
- `inventory/first-temporal-timestamp-tz-slice-tiflash-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiFlash-versus-TiKV drift classification and machine-readable sidecar for `first-temporal-timestamp-tz-slice`
- `inventory/first-union-slice-tidb-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiDB-versus-TiKV drift classification and machine-readable sidecar for `first-union-slice`
- `inventory/first-union-slice-tiflash-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiFlash-versus-TiKV drift classification and machine-readable sidecar for `first-union-slice`
- `docs/design/first-union-slice-tikv-live-runner-boundary.md` now defines the docs-first live TiKV runner configuration, execution, and artifact-refresh boundary for `first-union-slice` while keeping existing case IDs and artifact carriers stable
- `crates/tiforth-harness-differential/src/first_union_slice_tikv_live.rs`, `crates/tiforth-harness-differential/src/bin/first_union_slice_tikv_live.rs`, and `scripts/refresh-first-union-tikv-live-artifacts.sh` now provide env-configured live TiDB/TiFlash/TiKV execution and optional first-union TiKV artifact refresh wiring
- `docs/design/first-temporal-decimal-slices-tikv-live-runner-boundary.md` now defines docs-first live TiKV temporal/decimal runner configuration, execution, and artifact-refresh boundaries for `first-temporal-date32-slice`, `first-temporal-timestamp-tz-slice`, and `first-decimal128-slice` while preserving existing identifiers and carrier schemas
- `crates/tiforth-harness-differential/src/first_decimal128_slice_tikv_live.rs`, `crates/tiforth-harness-differential/src/bin/first_decimal128_slice_tikv_live.rs`, and `scripts/refresh-first-decimal128-tikv-live-artifacts.sh` now provide env-configured live TiDB/TiFlash/TiKV execution and optional first-decimal128 TiKV artifact refresh wiring
- `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv_live.rs`, `crates/tiforth-harness-differential/src/bin/first_temporal_date32_slice_tikv_live.rs`, and `scripts/refresh-first-temporal-date32-tikv-live-artifacts.sh` now provide env-configured live TiDB/TiFlash/TiKV execution and optional first-temporal-date32 TiKV artifact refresh wiring

Next checkpoint:

- refresh checked-in first-union, first-decimal128, and first-temporal-date32 TiKV artifacts from live runner output via `scripts/refresh-first-union-tikv-live-artifacts.sh`, `scripts/refresh-first-decimal128-tikv-live-artifacts.sh`, and `scripts/refresh-first-temporal-date32-tikv-live-artifacts.sh` when shared environments are available
- refresh TiKV pairwise drift artifacts for `tidb-vs-tikv` and `tiflash-vs-tikv` whenever first-expression, first-filter, first-temporal-date32, first-temporal-timestamp-tz, first-decimal128, first-float64-ordering, first-unsigned-arithmetic, or first-union case-results evidence changes
- after first-union, first-decimal128, and first-temporal-date32 live-runner refresh cadence stabilizes, implement live TiKV temporal runner wiring and refresh workflow for `first-temporal-timestamp-tz-slice` following `docs/design/first-temporal-decimal-slices-tikv-live-runner-boundary.md` when shared environments are available
- broaden TiKV executable temporal coverage beyond the current first-temporal-date32 and first-temporal-timestamp-tz checkpoints once follow-on semantic slices are accepted

## TODOs

- extend TiKV executable slice coverage beyond the current first-expression, first-filter, first-temporal-date32, first-temporal-timestamp-tz, first-decimal128, first-float64-ordering, first-unsigned-arithmetic, and first-union executable checkpoints using the next accepted docs-defined boundary
