# Differential Harness

This directory is for tests that compare behavior across TiDB, TiFlash, and TiKV.

Current checkpoint:

- `tests/differential/first-expression-slice.md` defines the first differential target: TiDB versus TiFlash on the milestone-1 expression-projection semantic core
- `tests/differential/first-filter-is-not-null-slice.md` defines the first differential docs checkpoint for the post-gate `is_not_null(column(index))` filter semantic slice
- `tests/differential/first-temporal-date32-slice.md` defines the first differential docs checkpoint for the narrow temporal `date32` semantic slice (`column(index)` passthrough plus `is_not_null(column(index))`)
- `tests/differential/first-decimal128-slice.md` defines the first differential docs checkpoint for the narrow decimal `decimal128` semantic slice (`column(index)` passthrough plus `is_not_null(column(index))`)
- `tests/differential/first-float64-ordering-slice.md` defines the first differential docs checkpoint for narrow `float64` NaN/infinity and canonical-ordering semantics (`column(index)` passthrough plus `is_not_null(column(index))`)
- `tests/differential/first-json-slice.md` defines the first differential docs checkpoint for narrow JSON structural-comparison and explicit `json <-> string` cast semantics
- `tests/differential/first-collation-string-slice.md` defines the first differential docs checkpoint for narrow collation-sensitive `utf8` comparison and ordering semantics under shared `binary` and `unicode_ci` collation identifiers
- `tests/differential/first-struct-slice.md` defines the first differential docs checkpoint for narrow nested `struct<a:int32, b:int32?>` passthrough semantics
- `tests/differential/first-map-slice.md` defines the first differential docs checkpoint for narrow nested `map<int32, int32?>` passthrough semantics
- `tests/differential/first-union-slice.md` defines the first differential docs checkpoint for narrow nested `dense_union<i:int32, n:int32?>` passthrough semantics
- `tests/differential/first-exchange-slice.md` defines the first exchange parity checkpoint over existing first-expression and first-filter case IDs
- `tests/differential/drift-report-carrier.md` defines the reusable minimum carrier for differential `drift-report` artifacts across slices
- `tests/differential/first-expression-slice-artifacts.md` defines the stable `case-results` and `drift-report` carriers for that slice
- `tests/differential/first-filter-is-not-null-slice-artifacts.md` defines the stable `case-results` and `drift-report` carriers for the first differential filter slice
- `tests/differential/first-temporal-date32-slice-artifacts.md` defines the stable `case-results` and `drift-report` carriers for the first differential temporal `date32` slice
- `tests/differential/first-decimal128-slice-artifacts.md` defines the stable `case-results` and `drift-report` carriers for the first differential decimal `decimal128` slice
- `tests/differential/first-float64-ordering-slice-artifacts.md` defines the stable `case-results` and `drift-report` carriers for the first differential float64 ordering slice
- `tests/differential/first-exchange-slice-artifacts.md` defines the stable baseline-versus-exchange `drift-report` carriers for the first differential exchange parity slice
- `adapters/first-expression-slice.md` defines the minimal request and response surface for that TiDB-versus-TiFlash slice
- `adapters/first-filter-is-not-null-slice.md` defines the minimal request and response surface for the first differential filter slice
- `adapters/first-temporal-date32-slice.md` defines the minimal request and response surface for the first differential temporal `date32` slice
- `adapters/first-decimal128-slice.md` defines the minimal request and response surface for the first differential decimal `decimal128` slice
- `adapters/first-float64-ordering-slice.md` defines the minimal request and response surface for the first differential float64 NaN/infinity ordering slice
- `adapters/first-json-slice.md` defines the minimal request and response surface for the first differential JSON comparability/cast slice
- `adapters/first-collation-string-slice.md` defines the minimal request and response surface for the first differential collation-sensitive string slice
- `adapters/first-struct-slice.md` defines the minimal request and response surface for the first differential struct passthrough slice
- `adapters/first-map-slice.md` defines the minimal request and response surface for the first differential map passthrough slice
- `adapters/first-union-slice.md` defines the minimal request and response surface for the first differential union passthrough slice
- `crates/tiforth-harness-differential` executes the first-expression, first-filter-is-not-null, first-temporal-date32, first-decimal128, and first-float64-ordering slices through the current TiDB and TiFlash adapter cores, validates checked-in paired artifacts under `inventory/`, and executes first-exchange-slice parity checks through `src/first_exchange_slice.rs`
- `crates/tiforth-harness-differential/src/bin/first_filter_is_not_null_live.rs` wires a live-runner path for `first-filter-is-not-null-slice` that can emit normalized artifacts using TiDB and TiFlash MySQL endpoints from environment configuration
- `scripts/refresh-first-filter-live-artifacts.sh` provides the canonical local command for env-validated dry runs and checked-in first-filter artifact refresh
- `docs/design/next-thin-end-to-end-slice.md` defines the next thin end-to-end checkpoint as executing that documented slice through adapters and into checked-in differential evidence
- `docs/design/adapter-milestone-breakdown.md` breaks that executable path into TiDB, TiFlash, and pairwise harness checkpoints so future issues stay reviewable
- `docs/process/inventory-artifact-naming.md` defines how future checked-in inventory evidence should be named
- `docs/process/inventory-refresh.md` defines when differential evidence should be checked into git or refreshed in follow-on PRs

Likely contents later:

- cross-engine case definitions
- adapters for query or expression execution
- drift reports and mismatch triage artifacts

Current rule: keep slice definitions and artifact-carrier docs in this directory; keep executable harness code under `crates/` and checked-in evidence under `inventory/`.
