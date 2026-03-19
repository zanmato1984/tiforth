# First Temporal Date32 Slice TiDB Compatibility Notes

Status: issue #204 inventory checkpoint

Verified: 2026-03-19

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #185 `docs: define first temporal date32 differential artifact carriers`
- #187 `harness: execute first-temporal-date32 differential artifacts for TiDB and TiFlash`
- #204 `inventory: add temporal date32 compatibility notes for TiDB and TiFlash`

## Purpose

This note records TiDB-side compatibility evidence for the
`first-temporal-date32-slice` checkpoint.

It scopes only to the shared first-temporal surface:

- passthrough projection `column(index)` over `date32`
- filter predicate `is_not_null(column(index))` over `date32`
- first temporal differential case IDs and normalized carriers
- current normalized TiDB outcomes recorded in checked-in inventory artifacts

This artifact treats checked-in differential evidence and adapter behavior as
source evidence, not as shared design authority.

## TiDB Snapshot

- tiforth repository base commit reviewed: `328804f818108b29744700f8a451af49105c672a`
- artifact baseline: deterministic adapter-core differential checkpoint from
  issue #187
- no temporal live-runner refresh artifacts are checked in yet for this slice

## Shared Slice Anchors

This note stays anchored to the stable first-temporal vocabulary already
defined in `tests/differential/first-temporal-date32-slice.md`.

- `slice_id = first-temporal-date32-slice`
- `projection_ref = column-0`
- `filter_ref = is-not-null-column-0`
- `filter_ref = is-not-null-column-1`
- `case_id = date32-column-passthrough`
- `case_id = date32-column-null-preserve`
- `case_id = date32-is-not-null-all-kept`
- `case_id = date32-is-not-null-all-dropped`
- `case_id = date32-is-not-null-mixed-keep-drop`
- `case_id = date32-missing-column-error`
- `case_id = unsupported-temporal-type-error`

## Reviewed Sources

- `docs/design/first-temporal-semantic-slice.md`
- `adapters/first-temporal-date32-slice.md`
- `tests/conformance/first-temporal-date32-slice.md`
- `tests/differential/first-temporal-date32-slice.md`
- `inventory/first-temporal-date32-slice-tidb-case-results.json`
- `inventory/first-temporal-date32-slice-tidb-vs-tiflash-drift-report.md`
- `crates/tiforth-adapter-tidb/src/first_temporal_date32_slice.rs`

## Compatibility Notes

### `column(index)` Date32 Passthrough

#### TiDB Surface

- the TiDB adapter lowers temporal passthrough requests into SQL projection
  over adapter-owned aliases
- checked-in TiDB temporal case-results include both non-null and nullable
  passthrough paths

#### Recorded TiDB Facts

- `date32-column-passthrough` returns 3 day-domain values (`0`, `1`, `2`) with
  schema `d:date32, nullable=false`
- `date32-column-null-preserve` returns 4 rows (`0`, `null`, `2`, `null`) with
  schema `d:date32, nullable=true`
- normalized row values remain in day-domain integer form as required by this
  slice

### `is_not_null(column(index))` Date32 Filtering

#### TiDB Surface

- the TiDB adapter lowers first-temporal filter requests into SQL with
  `WHERE ... IS NOT NULL`
- checked-in TiDB temporal case-results include all kept, all dropped, and
  mixed keep/drop paths

#### Recorded TiDB Facts

- `date32-is-not-null-all-kept` returns all 3 input rows with
  `d:date32, nullable=false`
- `date32-is-not-null-all-dropped` returns 0 rows with
  `d:date32, nullable=true`
- `date32-is-not-null-mixed-keep-drop` retains only day-domain rows `0` and
  `2`, preserving retained-row order

### Missing Column Error Normalization

#### TiDB Surface

- the adapter emits a deliberate missing-column SQL reference
  (`__missing_column_1`) for the documented out-of-range case
- error normalization maps TiDB missing-column signals into shared
  `error_class = missing_column`

#### Recorded TiDB Facts

- `date32-missing-column-error` normalizes to
  `error_class = missing_column`
- current checked-in TiDB artifact reports `engine_code = 1054` with message
  `Unknown column '__missing_column_1' in 'where clause'`

### Unsupported Temporal Type Normalization

#### TiDB Surface

- the first temporal shared boundary admits only `date32`; timestamp input is
  a documented unsupported case
- adapter normalization maps this path into shared
  `error_class = unsupported_temporal_type`

#### Recorded TiDB Facts

- `unsupported-temporal-type-error` normalizes to
  `error_class = unsupported_temporal_type`
- current checked-in TiDB artifact reports `engine_code = 1105` with an
  adapter-owned message indicating timestamp input is out of scope for this
  slice

## Differential Summary Link

- the paired first-temporal drift report records `match = 7`, `drift = 0`, and
  `unsupported = 0` across TiDB and TiFlash for this slice

## Boundary For This Artifact

- this note records TiDB-side compatibility evidence only for the first
  temporal `date32` slice
- it does not redefine the shared adapter request or response contract
- it does not add live-runner or production environment captures
- broader temporal families, timezone-sensitive behavior, and temporal
  arithmetic remain follow-on work
