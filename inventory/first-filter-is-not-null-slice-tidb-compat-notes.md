# First Filter Slice TiDB Compatibility Notes

Status: issue #167 inventory checkpoint

Verified: 2026-03-19

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #147 `design: define first differential filter slice and adapter boundary for is_not_null`
- #153 `harness: execute first-filter-is-not-null differential artifacts for TiDB and TiFlash`
- #167 `inventory: add first-filter per-engine compatibility notes artifacts`

## Purpose

This note records TiDB-side compatibility evidence for the
`first-filter-is-not-null-slice` checkpoint.

It scopes only to the shared first-filter surface:

- filter predicate `is_not_null(column(index))`
- first differential case IDs and normalized carriers
- current normalized TiDB outcomes recorded in checked-in inventory artifacts

This artifact treats checked-in differential evidence and adapter behavior as
source evidence, not as shared design authority.

## TiDB Snapshot

- tiforth repository base commit reviewed: `52c63eb96df6a27c7704c9f90decc5bec83077e7`
- artifact baseline: deterministic adapter-core differential checkpoint from
  issue #153
- live-runner refresh path exists (issue #155 and issue #157) but is not yet
  the evidence source for this note

## Shared Slice Anchors

This note stays anchored to the stable first-filter vocabulary already defined
in `tests/differential/first-filter-is-not-null-slice.md`.

- `slice_id = first-filter-is-not-null-slice`
- `filter_ref = is-not-null-column-0`
- `filter_ref = is-not-null-column-1`
- `case_id = all-rows-kept`
- `case_id = all-rows-dropped`
- `case_id = mixed-keep-drop`
- `case_id = missing-column-error`
- `case_id = unsupported-predicate-type-error`

## Reviewed Sources

- `docs/spec/first-filter-is-not-null.md`
- `adapters/first-filter-is-not-null-slice.md`
- `tests/conformance/first-filter-is-not-null-slice.md`
- `tests/differential/first-filter-is-not-null-slice.md`
- `inventory/first-filter-is-not-null-slice-tidb-case-results.json`
- `inventory/first-filter-is-not-null-slice-tidb-vs-tiflash-drift-report.md`
- `crates/tiforth-adapter-tidb/src/first_filter_is_not_null_slice.rs`

## Compatibility Notes

### `is_not_null(column(index))` Row Selection

#### TiDB Surface

- the TiDB adapter lowers documented first-filter requests into SQL with a
  `WHERE ... IS NOT NULL` predicate over adapter-owned input aliases
- the first-filter checked-in TiDB case-results artifact includes explicit row
  outcomes for all kept, all dropped, and mixed keep/drop paths

#### Recorded TiDB Facts

- `all-rows-kept` returns 3 rows with schema `a:int32, nullable=false`
- `all-rows-dropped` returns 0 rows with schema `a:int32, nullable=true`
- `mixed-keep-drop` returns rows `[1, 10]` and `[3, 30]` with the two-column
  schema preserved as `a:int32 nullable=true`, `b:int32 nullable=false`

#### Evidence Gaps

- this note is based on deterministic adapter-core artifacts, not refreshed
  live engine captures from the first-filter live runner path

### Schema, Type, And Row-Order Preservation

#### TiDB Surface

- the first-filter shared slice requires schema preservation and retained-row
  order stability for kept rows

#### Recorded TiDB Facts

- TiDB normalized row outcomes preserve documented field order and names for
  all row-returning first-filter cases
- TiDB normalized outcomes preserve shared logical type names (`int32`) and
  nullability values expected by the first-filter slice
- retained-row order in the mixed case remains stable (`1,10` then `3,30`)

### Missing Column Error Normalization

#### TiDB Surface

- the adapter emits a deliberate missing-column SQL reference
  (`__missing_column_1`) for the documented out-of-range case
- error normalization maps TiDB missing-column signals into shared
  `error_class = missing_column`

#### Recorded TiDB Facts

- `missing-column-error` normalizes to `error_class = missing_column`
- current checked-in TiDB artifact reports `engine_code = 1054` and message
  `Unknown column '__missing_column_1' in 'where clause'`

### Unsupported Predicate Type Normalization

#### TiDB Surface

- the first-filter shared boundary restricts predicate input support to
  `int32`; utf8 input is a documented unsupported case
- adapter normalization maps this path into shared
  `error_class = unsupported_predicate_type`

#### Recorded TiDB Facts

- `unsupported-predicate-type-error` normalizes to
  `error_class = unsupported_predicate_type`
- current checked-in TiDB artifact reports `engine_code = 1105` with an
  adapter-owned message indicating utf8 predicate input is out of scope

## Differential Summary Link

- the paired first-filter drift report records `match = 5`, `drift = 0`, and
  `unsupported = 0` across TiDB and TiFlash for this slice

## Boundary For This Artifact

- this note records TiDB-side compatibility evidence only for the first-filter
  `is_not_null(column(index))` slice
- it does not redefine the shared adapter request or response contract
- it does not refresh live-runner artifacts
- broader predicate families and non-`int32` predicate support remain follow-on
  work
