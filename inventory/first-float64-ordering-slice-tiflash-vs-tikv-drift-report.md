# First Float64 Ordering Slice TiFlash-vs-TiKV Drift Report

Status: issue #294 follow-on harness checkpoint

Verified: 2026-03-20

## Evidence Source

- this checkpoint runs the current TiFlash and TiKV adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-float64-ordering-slice-artifacts.md`

## Engines

- `tiflash`
- `tikv`

## Spec Refs

- `docs/design/first-float64-ordering-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-float64-ordering-slice.md`
- `tests/differential/first-float64-ordering-slice.md`

## Summary

- `match`: 7
- `drift`: 0
- `unsupported`: 0

## Cases

### `float64-column-passthrough`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 3 row(s) for `float64-column-passthrough` with field `f` normalized as `float64` under `row-order-preserved`.
- evidence_refs: `inventory/first-float64-ordering-slice-tiflash-case-results.json#float64-column-passthrough`, `inventory/first-float64-ordering-slice-tikv-case-results.json#float64-column-passthrough`

### `float64-special-values-passthrough`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 5 row(s) for `float64-special-values-passthrough` with field `f` normalized as `float64` under `row-order-preserved`.
- evidence_refs: `inventory/first-float64-ordering-slice-tiflash-case-results.json#float64-special-values-passthrough`, `inventory/first-float64-ordering-slice-tikv-case-results.json#float64-special-values-passthrough`

### `float64-is-not-null-all-kept`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 5 row(s) for `float64-is-not-null-all-kept` with field `f` normalized as `float64` under `row-order-preserved`.
- evidence_refs: `inventory/first-float64-ordering-slice-tiflash-case-results.json#float64-is-not-null-all-kept`, `inventory/first-float64-ordering-slice-tikv-case-results.json#float64-is-not-null-all-kept`

### `float64-is-not-null-mixed-keep-drop`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 3 row(s) for `float64-is-not-null-mixed-keep-drop` with field `f` normalized as `float64` under `row-order-preserved`.
- evidence_refs: `inventory/first-float64-ordering-slice-tiflash-case-results.json#float64-is-not-null-mixed-keep-drop`, `inventory/first-float64-ordering-slice-tikv-case-results.json#float64-is-not-null-mixed-keep-drop`

### `float64-canonical-ordering-normalization`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 6 row(s) for `float64-canonical-ordering-normalization` with field `f` normalized as `float64` under `float64-multiset-canonical`.
- evidence_refs: `inventory/first-float64-ordering-slice-tiflash-case-results.json#float64-canonical-ordering-normalization`, `inventory/first-float64-ordering-slice-tikv-case-results.json#float64-canonical-ordering-normalization`

### `float64-missing-column-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiFlash and TiKV both normalized `float64-missing-column-error` as `missing_column`.
- evidence_refs: `inventory/first-float64-ordering-slice-tiflash-case-results.json#float64-missing-column-error`, `inventory/first-float64-ordering-slice-tikv-case-results.json#float64-missing-column-error`

### `unsupported-floating-type-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiFlash and TiKV both normalized `unsupported-floating-type-error` as `unsupported_floating_type`.
- evidence_refs: `inventory/first-float64-ordering-slice-tiflash-case-results.json#unsupported-floating-type-error`, `inventory/first-float64-ordering-slice-tikv-case-results.json#unsupported-floating-type-error`
