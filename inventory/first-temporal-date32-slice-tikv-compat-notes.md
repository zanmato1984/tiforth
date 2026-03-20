# First Temporal Date32 Slice TiKV Compatibility Notes

Status: issue #266 adapter and inventory checkpoint, issue #270 pairwise inventory checkpoint

Verified: 2026-03-20

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #185 `docs: define first temporal date32 differential artifact carriers`
- #187 `harness: execute first-temporal-date32 differential artifacts for TiDB and TiFlash`
- #264 `design: define first TiKV temporal date32 adapter request/response surface`
- #266 `adapter: execute first-temporal-date32-slice through TiKV`
- #270 `harness: add first-temporal-date32-slice TiKV pairwise drift artifacts`

## Purpose

This note records TiKV-side compatibility evidence for the
`first-temporal-date32-slice` checkpoint.

It scopes only to the shared first-temporal surface:

- passthrough projection `column(index)` over `date32`
- filter predicate `is_not_null(column(index))` over `date32`
- first temporal differential case IDs and normalized carriers
- current normalized TiKV outcomes recorded in checked-in inventory artifacts

This artifact treats checked-in single-engine evidence and adapter behavior as
source evidence, not as shared design authority.

## TiKV Snapshot

- tiforth repository base commit reviewed: `acd06f95645dbdbd0cd59e48d3f55390d0364f21`
- artifact baseline: deterministic TiKV adapter-core single-engine plus pairwise checkpoint from issues #266 and #270
- paired temporal TiKV drift artifacts now cover `tidb-vs-tikv` and `tiflash-vs-tikv`
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
- `adapters/first-temporal-date32-slice-tikv.md`
- `tests/conformance/first-temporal-date32-slice.md`
- `tests/differential/first-temporal-date32-slice.md`
- `tests/differential/first-temporal-date32-slice-artifacts.md`
- `crates/tiforth-adapter-tikv/src/first_temporal_date32_slice.rs`
- `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv.rs`
- `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv_pairwise.rs`
- `inventory/first-temporal-date32-slice-tikv-case-results.json`
- `inventory/first-temporal-date32-slice-tidb-vs-tikv-drift-report.md`
- `inventory/first-temporal-date32-slice-tiflash-vs-tikv-drift-report.md`

## Compatibility Notes

### `column(index)` Date32 Passthrough

#### TiKV Surface

- the TiKV adapter lowers temporal passthrough requests into SQL projection
  over adapter-owned aliases
- checked-in TiKV temporal case-results include both non-null and nullable
  passthrough paths

#### Recorded TiKV Facts

- `date32-column-passthrough` returns 3 day-domain values (`0`, `1`, `2`) with
  schema `d:date32, nullable=false`
- `date32-column-null-preserve` returns 4 rows (`0`, `null`, `2`, `null`) with
  schema `d:date32, nullable=true`
- normalized row values remain in day-domain integer form as required by this
  slice

### `is_not_null(column(index))` Date32 Filtering

#### TiKV Surface

- the TiKV adapter lowers first-temporal filter requests into SQL with
  `WHERE ... IS NOT NULL`
- checked-in TiKV temporal case-results include all kept, all dropped, and
  mixed keep/drop paths

#### Recorded TiKV Facts

- `date32-is-not-null-all-kept` returns all 3 input rows with
  `d:date32, nullable=false`
- `date32-is-not-null-all-dropped` returns 0 rows with
  `d:date32, nullable=true`
- `date32-is-not-null-mixed-keep-drop` retains only day-domain rows `0` and
  `2`, preserving retained-row order

### Missing Column Error Normalization

#### TiKV Surface

- the adapter emits a deliberate missing-column SQL reference
  (`__missing_column_1`) for the documented out-of-range case
- error normalization maps TiKV missing-column signals into shared
  `error_class = missing_column`

#### Recorded TiKV Facts

- `date32-missing-column-error` normalizes to
  `error_class = missing_column`
- current checked-in TiKV artifact reports `engine_code = 1054` with message
  `Unknown column '__missing_column_1' in 'where clause'`

### Unsupported Temporal Type Normalization

#### TiKV Surface

- the first temporal shared boundary admits only `date32`; timestamp input is
  a documented unsupported case
- adapter normalization maps this path into shared
  `error_class = unsupported_temporal_type`

#### Recorded TiKV Facts

- `unsupported-temporal-type-error` normalizes to
  `error_class = unsupported_temporal_type`
- current checked-in TiKV artifact reports `engine_code = 1105` with an
  adapter-owned message indicating timestamp input is out of scope for this
  slice

## Boundary For This Artifact

- this note records TiKV-side compatibility evidence only for the first
  temporal `date32` slice
- it does not redefine the shared adapter request or response contract
- it does not add live-runner or production environment captures
- checked-in evidence for this checkpoint includes the TiKV single-engine `case-results` artifact plus paired TiDB-vs-TiKV and TiFlash-vs-TiKV `drift-report` artifacts
- broader temporal families, timezone-sensitive behavior, and temporal
  arithmetic remain follow-on work
