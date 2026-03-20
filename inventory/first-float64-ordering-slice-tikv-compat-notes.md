# First Float64 Ordering Slice TiKV Compatibility Notes

Status: issue #292 adapter, harness, and inventory checkpoint

Verified: 2026-03-20

Related issues:

- #194 `design: define first float64 NaN, infinity, and ordering checkpoint`
- #208 `harness: execute first-float64-ordering-slice differential artifacts for TiDB and TiFlash`
- #286 `design: define TiKV adapter boundary for first-float64-ordering-slice`
- #292 `harness: execute first-float64-ordering-slice TiKV single-engine artifacts`

## Purpose

This note records TiKV-side compatibility evidence for the
`first-float64-ordering-slice` checkpoint.

It scopes only to the shared first-float64 surface:

- passthrough projection `column(index)` over `float64`
- filter predicate `is_not_null(column(index))` over `float64`
- first float64 `comparison_mode` checkpoints
- first float64 differential case IDs and normalized carriers
- current normalized TiKV outcomes recorded in checked-in inventory artifacts

This artifact treats checked-in single-engine evidence plus adapter behavior as
source evidence, not as shared design authority.

## TiKV Snapshot

- tiforth repository base commit reviewed: `e07617a4c62ca2ac738eb95ac700cfa3261ec941`
- artifact baseline: deterministic TiKV adapter-core single-engine checkpoint from issue #292
- no float64 TiKV pairwise or live-runner refresh artifacts are checked in yet for this slice

## Shared Slice Anchors

This note stays anchored to the stable first-float64 vocabulary already
defined in `tests/differential/first-float64-ordering-slice.md`.

- `slice_id = first-float64-ordering-slice`
- `projection_ref = column-0`
- `filter_ref = is-not-null-column-0`
- `filter_ref = is-not-null-column-1`
- `comparison_mode = row-order-preserved`
- `comparison_mode = float64-multiset-canonical`
- `case_id = float64-column-passthrough`
- `case_id = float64-special-values-passthrough`
- `case_id = float64-is-not-null-all-kept`
- `case_id = float64-is-not-null-mixed-keep-drop`
- `case_id = float64-canonical-ordering-normalization`
- `case_id = float64-missing-column-error`
- `case_id = unsupported-floating-type-error`

## Reviewed Sources

- `docs/design/first-float64-ordering-slice.md`
- `adapters/first-float64-ordering-slice-tikv.md`
- `tests/conformance/first-float64-ordering-slice.md`
- `tests/differential/first-float64-ordering-slice.md`
- `tests/differential/first-float64-ordering-slice-artifacts.md`
- `crates/tiforth-adapter-tikv/src/first_float64_ordering_slice.rs`
- `crates/tiforth-harness-differential/src/first_float64_ordering_slice_tikv.rs`
- `inventory/first-float64-ordering-slice-tikv-case-results.json`

## Compatibility Notes

### `column(index)` Float64 Passthrough

#### TiKV Surface

- the TiKV adapter lowers float64 passthrough requests into SQL projection over
  adapter-owned aliases
- checked-in TiKV float64 case-results include both ordinary finite values and
  special values (`-Infinity`, `Infinity`, `NaN`, and signed zero)

#### Recorded TiKV Facts

- `float64-column-passthrough` returns 3 rows (`"-1.5"`, `"0.0"`, `"2.25"`)
  with schema `f:float64, nullable=false`
- `float64-special-values-passthrough` returns 5 rows (`"-Infinity"`,
  `"-0.0"`, `"0.0"`, `"Infinity"`, `"NaN"`) with schema
  `f:float64, nullable=false`

### `is_not_null(column(index))` Float64 Filtering

#### TiKV Surface

- the TiKV adapter lowers first-float64 filter requests into SQL with
  `WHERE ... IS NOT NULL`
- checked-in TiKV float64 case-results include all-kept and mixed keep/drop
  paths

#### Recorded TiKV Facts

- `float64-is-not-null-all-kept` returns all 5 special-value rows with
  `f:float64, nullable=false`
- `float64-is-not-null-mixed-keep-drop` retains 3 non-null rows
  (`"-Infinity"`, `"NaN"`, `"1.0"`) with schema
  `f:float64, nullable=true`

### Comparison-Mode And Canonical Ordering Checkpoint

#### TiKV Surface

- the first float64 slice carries `comparison_mode` in each request and case
  result
- the TiKV adapter supports both documented modes:
  `row-order-preserved` and `float64-multiset-canonical`

#### Recorded TiKV Facts

- `float64-canonical-ordering-normalization` records 6 rows in TiKV output as
  `"NaN"`, `"1.0"`, `"-Infinity"`, `"Infinity"`, `"0.0"`, `"-0.0"`

### Missing Column Error Normalization

#### TiKV Surface

- the adapter emits a deliberate missing-column SQL reference
  (`__missing_column_1`) for the documented out-of-range case
- error normalization maps TiKV missing-column signals into shared
  `error_class = missing_column`

#### Recorded TiKV Facts

- `float64-missing-column-error` normalizes to
  `error_class = missing_column`
- current checked-in TiKV artifact reports `engine_code = 1054` with message
  `Unknown column '__missing_column_1' in 'where clause'`

### Unsupported Floating Type Normalization

#### TiKV Surface

- the first float64 shared boundary admits only `float64`; `float32` input is a
  documented unsupported case
- adapter normalization maps this path into shared
  `error_class = unsupported_floating_type`

#### Recorded TiKV Facts

- `unsupported-floating-type-error` normalizes to
  `error_class = unsupported_floating_type`
- current checked-in TiKV artifact reports `engine_code = 1105` with an
  adapter-owned message indicating `float32` input is out of scope for this
  slice

## Boundary For This Artifact

- this note records TiKV-side compatibility evidence only for the first
  float64 ordering slice
- it does not redefine the shared adapter request or response contract
- it does not add pairwise drift-report artifacts or live-runner captures
- broader floating semantics such as arithmetic, casts, and coercion remain
  follow-on work
