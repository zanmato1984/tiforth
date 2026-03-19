# Differential Harness

This directory is for tests that compare behavior across TiDB, TiFlash, and TiKV.

Current checkpoint:

- `tests/differential/first-expression-slice.md` defines the first differential target: TiDB versus TiFlash on the milestone-1 expression-projection semantic core
- `tests/differential/first-filter-is-not-null-slice.md` defines the first differential docs checkpoint for the post-gate `is_not_null(column(index))` filter semantic slice
- `tests/differential/drift-report-carrier.md` defines the reusable minimum carrier for differential `drift-report` artifacts across slices
- `tests/differential/first-expression-slice-artifacts.md` defines the stable `case-results` and `drift-report` carriers for that slice
- `tests/differential/first-filter-is-not-null-slice-artifacts.md` defines the stable `case-results` and `drift-report` carriers for the first differential filter slice
- `adapters/first-expression-slice.md` defines the minimal request and response surface for that TiDB-versus-TiFlash slice
- `adapters/first-filter-is-not-null-slice.md` defines the minimal request and response surface for the first differential filter slice
- `crates/tiforth-harness-differential` executes the first-expression and first-filter-is-not-null slices through the current TiDB and TiFlash adapter cores and validates the checked-in paired artifacts under `inventory/`
- `crates/tiforth-harness-differential/src/bin/first_filter_is_not_null_live.rs` wires a live-runner path for `first-filter-is-not-null-slice` that can emit normalized artifacts using TiDB and TiFlash MySQL endpoints from environment configuration
- `docs/design/next-thin-end-to-end-slice.md` defines the next thin end-to-end checkpoint as executing that documented slice through adapters and into checked-in differential evidence
- `docs/design/adapter-milestone-breakdown.md` breaks that executable path into TiDB, TiFlash, and pairwise harness checkpoints so future issues stay reviewable
- `docs/process/inventory-artifact-naming.md` defines how future checked-in inventory evidence should be named
- `docs/process/inventory-refresh.md` defines when differential evidence should be checked into git or refreshed in follow-on PRs

Likely contents later:

- cross-engine case definitions
- adapters for query or expression execution
- drift reports and mismatch triage artifacts

Current rule: keep slice definitions and artifact-carrier docs in this directory; keep executable harness code under `crates/` and checked-in evidence under `inventory/`.
