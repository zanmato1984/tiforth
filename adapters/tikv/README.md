# TiKV Adapter

This directory defines the TiKV-facing adapter boundary.

The adapter should eventually translate TiKV expression and operator behavior into shared specs and contracts without turning adapter code into the semantic source of truth.

Current checkpoint:

- the first shared adapter boundary in `adapters/first-expression-slice.md` remains intentionally limited to TiDB and TiFlash
- `adapters/first-expression-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-expression-slice`
- `adapters/first-filter-is-not-null-slice-tikv.md` now defines the first TiKV-specific request and response boundary for `first-filter-is-not-null-slice`
- `docs/design/adapter-milestone-breakdown.md` records why TiKV follows the initial TiDB/TiFlash pairwise checkpoint sequence
- `docs/design/adapter-runtime-orchestration-boundary.md` fixes which TiKV environment, timeout, retry, cancellation, and diagnostic concerns should stay adapter-local
- `crates/tiforth-adapter-tikv` now encodes `first-expression-slice` and `first-filter-is-not-null-slice` request catalogs with TiKV-oriented SQL lowering and row/error normalization behind runner boundaries
- `crates/tiforth-harness-differential/src/first_filter_is_not_null_slice_tikv.rs` now provides deterministic TiKV single-engine harness execution for the documented first-filter case set
- `inventory/first-expression-slice-tikv-compat-notes.md` now captures the first TiKV-side compatibility notes checkpoint for `first-expression-slice`
- `inventory/first-expression-slice-tikv-case-results.json` now captures the first checked-in TiKV single-engine normalized `case-results` evidence for `first-expression-slice`
- `inventory/first-filter-is-not-null-slice-tikv-compat-notes.md` now captures the first TiKV-side compatibility notes checkpoint for `first-filter-is-not-null-slice`
- `inventory/first-filter-is-not-null-slice-tikv-case-results.json` now captures the first checked-in TiKV single-engine normalized `case-results` evidence for `first-filter-is-not-null-slice`
- `inventory/first-expression-slice-tidb-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiDB-versus-TiKV drift classification and machine-readable sidecar for `first-expression-slice`
- `inventory/first-expression-slice-tiflash-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiFlash-versus-TiKV drift classification and machine-readable sidecar for `first-expression-slice`
- `inventory/first-filter-is-not-null-slice-tidb-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiDB-versus-TiKV drift classification and machine-readable sidecar for `first-filter-is-not-null-slice`
- `inventory/first-filter-is-not-null-slice-tiflash-vs-tikv-drift-report.md` plus `.json` now capture the first executable TiFlash-versus-TiKV drift classification and machine-readable sidecar for `first-filter-is-not-null-slice`

Next checkpoint:

- refresh TiKV pairwise drift artifacts for `tidb-vs-tikv` and `tiflash-vs-tikv` whenever first-expression or first-filter case-results evidence changes

## TODOs

- refresh TiKV pairwise drift artifacts for `tidb-vs-tikv` and `tiflash-vs-tikv` whenever first-expression case-results evidence changes
- refresh TiKV pairwise drift artifacts for `tidb-vs-tikv` and `tiflash-vs-tikv` whenever first-filter case-results evidence changes
- extend the TiKV request and response surface beyond `first-expression-slice` and `first-filter-is-not-null-slice` after current checkpoints are reviewable
