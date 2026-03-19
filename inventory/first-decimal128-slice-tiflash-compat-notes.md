# First Decimal128 Slice TiFlash Compatibility Notes

Status: issue #214 inventory checkpoint

Verified: 2026-03-19

Related issues:

- #189 `design: define first decimal semantic slice boundary`
- #206 `harness: execute first-decimal128-slice differential artifacts for TiDB and TiFlash`
- #214 `inventory: add decimal128 and float64 per-engine compatibility notes artifacts`

## Purpose

This note records TiFlash-side compatibility evidence for the
`first-decimal128-slice` checkpoint.

It scopes only to the shared first-decimal surface:

- passthrough projection `column(index)` over `decimal128`
- filter predicate `is_not_null(column(index))` over `decimal128`
- first decimal differential case IDs and normalized carriers
- current normalized TiFlash outcomes recorded in checked-in inventory artifacts

This artifact treats checked-in differential evidence and adapter behavior as
source evidence, not as shared design authority.

## TiFlash Snapshot

- tiforth repository base commit reviewed: `b70b426409a500bfe2a69c36a4c617c551575d25`
- artifact baseline: deterministic adapter-core differential checkpoint from
  issue #206
- no decimal live-runner refresh artifacts are checked in yet for this slice

## Shared Slice Anchors

This note stays anchored to the stable first-decimal vocabulary already
defined in `tests/differential/first-decimal128-slice.md`.

- `slice_id = first-decimal128-slice`
- `projection_ref = column-0`
- `filter_ref = is-not-null-column-0`
- `filter_ref = is-not-null-column-1`
- `case_id = decimal128-column-passthrough`
- `case_id = decimal128-column-null-preserve`
- `case_id = decimal128-is-not-null-all-kept`
- `case_id = decimal128-is-not-null-all-dropped`
- `case_id = decimal128-is-not-null-mixed-keep-drop`
- `case_id = decimal128-missing-column-error`
- `case_id = unsupported-decimal-type-error`
- `case_id = invalid-decimal-metadata-error`

## Reviewed Sources

- `docs/design/first-decimal-semantic-slice.md`
- `adapters/first-decimal128-slice.md`
- `tests/conformance/first-decimal128-slice.md`
- `tests/differential/first-decimal128-slice.md`
- `inventory/first-decimal128-slice-tiflash-case-results.json`
- `inventory/first-decimal128-slice-tidb-vs-tiflash-drift-report.md`
- `crates/tiforth-adapter-tiflash/src/first_decimal128_slice.rs`

## Compatibility Notes

### `column(index)` Decimal128 Passthrough

#### TiFlash Surface

- the TiFlash adapter lowers decimal passthrough requests into SQL projection
  over adapter-owned aliases
- checked-in TiFlash decimal case-results include both non-null and nullable
  passthrough paths

#### Recorded TiFlash Facts

- `decimal128-column-passthrough` returns 3 rows (`"1.00"`, `"2.50"`,
  `"-3.75"`) with schema `d:decimal128(10,2), nullable=false`
- `decimal128-column-null-preserve` returns 4 rows (`"1.00"`, `null`,
  `"-3.75"`, `null`) with schema `d:decimal128(10,2), nullable=true`
- normalized row values use canonical decimal strings, preserving scale for this
  slice

### `is_not_null(column(index))` Decimal128 Filtering

#### TiFlash Surface

- the TiFlash adapter lowers first-decimal filter requests into SQL with
  `WHERE ... IS NOT NULL`
- checked-in TiFlash decimal case-results include all kept, all dropped, and
  mixed keep/drop paths

#### Recorded TiFlash Facts

- `decimal128-is-not-null-all-kept` returns all 3 input rows with
  `d:decimal128(10,2), nullable=false`
- `decimal128-is-not-null-all-dropped` returns 0 rows with
  `d:decimal128(10,2), nullable=true`
- `decimal128-is-not-null-mixed-keep-drop` retains only `"1.00"` and
  `"-3.75"`, preserving retained-row order

### Missing Column Error Normalization

#### TiFlash Surface

- the adapter emits a deliberate missing-column SQL reference
  (`__missing_column_1`) for the documented out-of-range case
- error normalization maps TiFlash missing-column signals into shared
  `error_class = missing_column`

#### Recorded TiFlash Facts

- `decimal128-missing-column-error` normalizes to
  `error_class = missing_column`
- current checked-in TiFlash artifact reports `engine_code = 1054` with message
  `Unknown column '__missing_column_1' in 'where clause'`

### Unsupported Decimal Type Normalization

#### TiFlash Surface

- the first decimal shared boundary admits only `decimal128`; `decimal256`
  input is a documented unsupported case
- adapter normalization maps this path into shared
  `error_class = unsupported_decimal_type`

#### Recorded TiFlash Facts

- `unsupported-decimal-type-error` normalizes to
  `error_class = unsupported_decimal_type`
- current checked-in TiFlash artifact reports `engine_code = 1105` with an
  adapter-owned message indicating `decimal256` input is out of scope for this
  slice

### Invalid Decimal Metadata Normalization

#### TiFlash Surface

- the first decimal shared boundary requires valid decimal metadata
  (`scale <= precision`)
- adapter normalization maps invalid declared metadata into shared
  `error_class = invalid_decimal_metadata`

#### Recorded TiFlash Facts

- `invalid-decimal-metadata-error` normalizes to
  `error_class = invalid_decimal_metadata`
- current checked-in TiFlash artifact reports `engine_code = 1105` with message
  `invalid decimal metadata: decimal(10,12) has scale greater than precision`

## Differential Summary Link

- the paired first-decimal drift report records `match = 8`, `drift = 0`, and
  `unsupported = 0` across TiDB and TiFlash for this slice

## Boundary For This Artifact

- this note records TiFlash-side compatibility evidence only for the first
  decimal `decimal128` slice
- it does not redefine the shared adapter request or response contract
- it does not add live-runner or production environment captures
- broader decimal families, arithmetic, cast/coercion, and rounding semantics
  remain follow-on work
