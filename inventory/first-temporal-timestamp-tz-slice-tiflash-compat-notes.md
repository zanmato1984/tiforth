# First Temporal Timestamp-Timezone Slice TiFlash Compatibility Notes

Status: issue #316 inventory checkpoint

Verified: 2026-03-20

Related issues:

- #280 `design: define first timezone-aware timestamp semantic slice checkpoint`
- #288 `milestone-1: implement first executable temporal timestamp_tz(us) slice in local kernel`
- #298 `docs: define first-temporal-timestamp-tz differential artifact carriers`
- #304 `harness: execute first timestamp_tz(us) differential artifacts`
- #316 `inventory: add first-temporal-timestamp-tz compatibility notes for TiDB and TiFlash`

## Purpose

This note records TiFlash-side compatibility evidence for the
`first-temporal-timestamp-tz-slice` checkpoint.

It scopes only to the shared first timestamp-timezone surface:

- passthrough projection `column(index)` over `timestamp_tz(us)`
- filter predicate `is_not_null(column(index))` over `timestamp_tz(us)`
- ascending ordering probe `order-by(column(index), asc, nulls_last)`
- boundary probes for unsupported timestamp-without-timezone and unsupported
  timestamp-unit requests
- current normalized TiFlash outcomes recorded in checked-in inventory
  artifacts

This artifact treats checked-in differential evidence and adapter behavior as
source evidence, not as shared design authority.

## TiFlash Snapshot

- tiforth repository base commit reviewed: `e5e82bf050118de2d2e72f9a028f76e6d983c6d0`
- artifact baseline: deterministic adapter-core differential checkpoint from
  issue #304
- no timestamp-timezone live-runner refresh artifacts are checked in yet for
  this slice

## Shared Slice Anchors

This note stays anchored to the stable first timestamp-timezone vocabulary
already defined in `tests/differential/first-temporal-timestamp-tz-slice.md`.

- `slice_id = first-temporal-timestamp-tz-slice`
- `projection_ref = column-0`
- `filter_ref = is-not-null-column-0`
- `filter_ref = is-not-null-column-1`
- `ordering_ref = order-by-column-0-asc-nulls-last`
- `case_id = timestamp-tz-column-passthrough`
- `case_id = timestamp-tz-equivalent-instant-normalization`
- `case_id = timestamp-tz-column-null-preserve`
- `case_id = timestamp-tz-is-not-null-all-kept`
- `case_id = timestamp-tz-is-not-null-all-dropped`
- `case_id = timestamp-tz-is-not-null-mixed-keep-drop`
- `case_id = timestamp-tz-order-asc-nulls-last`
- `case_id = timestamp-tz-missing-column-error`
- `case_id = unsupported-temporal-timestamp-without-timezone-error`
- `case_id = unsupported-temporal-timestamp-unit-error`

## Reviewed Sources

- `docs/design/first-temporal-timestamp-tz-slice.md`
- `adapters/first-temporal-timestamp-tz-slice.md`
- `tests/conformance/first-temporal-timestamp-tz-slice.md`
- `tests/differential/first-temporal-timestamp-tz-slice.md`
- `tests/differential/first-temporal-timestamp-tz-slice-artifacts.md`
- `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json`
- `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tiflash-drift-report.md`
- `crates/tiforth-adapter-tiflash/src/first_temporal_timestamp_tz_slice.rs`

## Compatibility Notes

### `column(index)` Timestamp-Timezone Passthrough

#### TiFlash Surface

- the TiFlash adapter lowers timestamp-timezone passthrough requests into SQL
  projection over adapter-owned aliases
- checked-in TiFlash case-results include non-null, nullable, and
  equivalent-instant passthrough paths

#### Recorded TiFlash Facts

- `timestamp-tz-column-passthrough` returns 3 normalized UTC
  epoch-microsecond values (`0`, `1000000`, `2000000`) with schema
  `ts:timestamp_tz(us), nullable=false`
- `timestamp-tz-equivalent-instant-normalization` returns 3 equivalent
  instants normalized to the same UTC epoch-microsecond value
  (`1704067200000000`)
- `timestamp-tz-column-null-preserve` returns 4 rows
  (`0`, `null`, `2000000`, `null`) with schema
  `ts:timestamp_tz(us), nullable=true`

### `is_not_null(column(index))` Timestamp-Timezone Filtering

#### TiFlash Surface

- the TiFlash adapter lowers first timestamp-timezone filter requests into SQL
  with `WHERE ... IS NOT NULL`
- checked-in TiFlash case-results include all kept, all dropped, and mixed
  keep/drop paths

#### Recorded TiFlash Facts

- `timestamp-tz-is-not-null-all-kept` returns all 3 input rows with
  `ts:timestamp_tz(us), nullable=false`
- `timestamp-tz-is-not-null-all-dropped` returns 0 rows with
  `ts:timestamp_tz(us), nullable=true`
- `timestamp-tz-is-not-null-mixed-keep-drop` retains only normalized UTC rows
  `0` and `2000000`, preserving retained-row order

### Timestamp-Timezone Ordering Probe

#### TiFlash Surface

- ordering probe requests lower through stable
  `ordering_ref = order-by-column-0-asc-nulls-last`
- normalized row output remains UTC epoch-microsecond integers plus trailing
  null rows

#### Recorded TiFlash Facts

- `timestamp-tz-order-asc-nulls-last` returns 4 rows with non-null values in
  ascending normalized UTC order followed by null rows

### Error Normalization

#### TiFlash Surface

- the adapter emits a deliberate missing-column SQL reference
  (`__missing_column_1`) for the documented out-of-range case
- unsupported timestamp-without-timezone and unsupported timestamp-unit probes
  map to stable normalized error classes for this slice

#### Recorded TiFlash Facts

- `timestamp-tz-missing-column-error` normalizes to
  `error_class = missing_column`
- current checked-in TiFlash artifact reports `engine_code = 1054` with
  message `Unknown column '__missing_column_1' in 'where clause'`
- `unsupported-temporal-timestamp-without-timezone-error` normalizes to
  `error_class = unsupported_temporal_type`
- current checked-in TiFlash artifact reports `engine_code = 1105` with an
  adapter-owned message indicating timestamp without timezone input is out of
  scope for this slice
- `unsupported-temporal-timestamp-unit-error` normalizes to
  `error_class = unsupported_temporal_unit`
- current checked-in TiFlash artifact reports `engine_code = 1105` with an
  adapter-owned message indicating non-`us` timestamp units are out of scope
  for this slice

## Differential Summary Link

- the paired first timestamp-timezone drift report records `match = 10`,
  `drift = 0`, and `unsupported = 0` across TiDB and TiFlash for this slice

## Boundary For This Artifact

- this note records TiFlash-side compatibility evidence only for the first
  timezone-aware temporal `timestamp_tz(us)` slice
- it does not redefine the shared adapter request or response contract
- it does not add live-runner or production environment captures
- broader temporal families, timezone negotiation policy, and temporal
  arithmetic remain follow-on work
