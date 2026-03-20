# First Temporal Timestamp-Timezone Slice TiKV Compatibility Notes

Status: issue #306 adapter and inventory checkpoint

Verified: 2026-03-20

Related issues:

- #280 `design: define first timezone-aware timestamp semantic slice checkpoint`
- #288 `kernel: execute first timestamp_tz(us) local conformance slice`
- #290 `design: define TiKV adapter boundary for first-temporal-timestamp-tz-slice`
- #298 `docs: define first-temporal-timestamp-tz differential artifact carriers`
- #304 `harness: execute first timestamp_tz(us) differential artifacts`
- #306 `checkpoint: implement TiKV first-temporal-timestamp-tz executable differential slice`

## Purpose

This note records TiKV-side compatibility evidence for the
`first-temporal-timestamp-tz-slice` checkpoint.

It scopes only to the shared first timestamp-timezone surface:

- passthrough projection `column(index)` over `timestamp_tz(us)`
- filter predicate `is_not_null(column(index))` over `timestamp_tz(us)`
- ascending ordering probe `order-by(column(index), asc, nulls_last)`
- boundary probes for unsupported timestamp-without-timezone and unsupported
  timestamp-unit requests
- current normalized TiKV outcomes recorded in checked-in inventory artifacts

This artifact treats checked-in single-engine evidence and adapter behavior as
source evidence, not as shared design authority.

## TiKV Snapshot

- tiforth repository base commit reviewed: `8f58fa69f9f422e648ddc43d9893968890e63d64`
- artifact baseline: deterministic TiKV adapter-core single-engine plus pairwise
  checkpoint from issue #306
- paired timestamp-timezone TiKV drift artifacts now cover `tidb-vs-tikv` and
  `tiflash-vs-tikv`
- no timestamp-timezone live-runner refresh artifacts are checked in yet for this
  slice

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
- `adapters/first-temporal-timestamp-tz-slice-tikv.md`
- `tests/conformance/first-temporal-timestamp-tz-slice.md`
- `tests/differential/first-temporal-timestamp-tz-slice.md`
- `tests/differential/first-temporal-timestamp-tz-slice-artifacts.md`
- `crates/tiforth-adapter-tikv/src/first_temporal_timestamp_tz_slice.rs`
- `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv.rs`
- `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv_pairwise.rs`
- `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json`
- `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tikv-drift-report.md`
- `inventory/first-temporal-timestamp-tz-slice-tiflash-vs-tikv-drift-report.md`

## Compatibility Notes

### `column(index)` Timestamp-Timezone Passthrough

#### TiKV Surface

- the TiKV adapter lowers timestamp-timezone passthrough requests into SQL
  projection over adapter-owned aliases
- checked-in TiKV case-results include non-null, nullable, and equivalent-instant
  passthrough paths

#### Recorded TiKV Facts

- `timestamp-tz-column-passthrough` returns 3 normalized UTC epoch-microsecond
  values (`0`, `1000000`, `2000000`) with schema
  `ts:timestamp_tz(us), nullable=false`
- `timestamp-tz-equivalent-instant-normalization` returns 3 equivalent-instants
  normalized to the same UTC epoch-microsecond value
  (`1704067200000000`)
- `timestamp-tz-column-null-preserve` returns 4 rows
  (`0`, `null`, `2000000`, `null`) with schema
  `ts:timestamp_tz(us), nullable=true`

### `is_not_null(column(index))` Timestamp-Timezone Filtering

#### TiKV Surface

- the TiKV adapter lowers first timestamp-timezone filter requests into SQL with
  `WHERE ... IS NOT NULL`
- checked-in TiKV case-results include all kept, all dropped, and mixed
  keep/drop paths

#### Recorded TiKV Facts

- `timestamp-tz-is-not-null-all-kept` returns all 3 input rows with
  `ts:timestamp_tz(us), nullable=false`
- `timestamp-tz-is-not-null-all-dropped` returns 0 rows with
  `ts:timestamp_tz(us), nullable=true`
- `timestamp-tz-is-not-null-mixed-keep-drop` retains only normalized UTC rows
  `0` and `2000000`, preserving retained-row order

### Timestamp-Timezone Ordering Probe

#### TiKV Surface

- ordering probe requests lower through stable
  `ordering_ref = order-by-column-0-asc-nulls-last`
- normalized row output remains UTC epoch-microsecond integers plus trailing
  null rows

#### Recorded TiKV Facts

- `timestamp-tz-order-asc-nulls-last` returns 4 rows with non-null values in
  ascending normalized UTC order followed by null rows

### Error Normalization

#### TiKV Surface

- deliberate missing-column SQL reference (`__missing_column_1`) remains the
  documented out-of-range boundary probe
- unsupported timestamp-without-timezone and unsupported timestamp-unit probes
  map to stable normalized error classes

#### Recorded TiKV Facts

- `timestamp-tz-missing-column-error` normalizes to
  `error_class = missing_column` with `engine_code = 1054`
- `unsupported-temporal-timestamp-without-timezone-error` normalizes to
  `error_class = unsupported_temporal_type` with `engine_code = 1105`
- `unsupported-temporal-timestamp-unit-error` normalizes to
  `error_class = unsupported_temporal_unit` with `engine_code = 1105`

## Boundary For This Artifact

- this note records TiKV-side compatibility evidence only for the first
  timezone-aware temporal `timestamp_tz(us)` slice
- it does not redefine the shared adapter request or response contract
- it does not add live-runner or production environment captures
- checked-in evidence for this checkpoint includes the TiKV single-engine
  `case-results` artifact plus paired TiDB-vs-TiKV and TiFlash-vs-TiKV
  `drift-report` artifacts
- broader temporal families, timezone negotiation policy, and temporal arithmetic
  remain follow-on work
