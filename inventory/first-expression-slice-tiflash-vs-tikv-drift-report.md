# First Expression Slice TiFlash-vs-TiKV Drift Report

Status: issue #245 harness checkpoint

Verified: 2026-03-19

## Evidence Source

- this checkpoint runs the current TiFlash and TiKV adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-expression-slice-artifacts.md`

## Engines

- `tiflash`
- `tikv`

## Spec Refs

- `docs/spec/milestone-1-expression-projection.md`
- `docs/spec/type-system.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/differential/first-expression-slice.md`

## Summary

- `match`: 4
- `drift`: 2
- `unsupported`: 0

## Cases

### `column-passthrough`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 3 row(s) for `column-passthrough` with field `a` normalized as `int32`.
- evidence_refs: `inventory/first-expression-slice-tiflash-case-results.json#column-passthrough`, `inventory/first-expression-slice-tikv-case-results.json#column-passthrough`

### `literal-int32-seven`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 3 row(s) for `literal-int32-seven` with field `lit` normalized as `int32`.
- evidence_refs: `inventory/first-expression-slice-tiflash-case-results.json#literal-int32-seven`, `inventory/first-expression-slice-tikv-case-results.json#literal-int32-seven`

### `literal-int32-null`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 3 row(s) for `literal-int32-null` with field `lit` normalized as `int32`.
- evidence_refs: `inventory/first-expression-slice-tiflash-case-results.json#literal-int32-null`, `inventory/first-expression-slice-tikv-case-results.json#literal-int32-null`

### `add-int32-literal`

- status: `drift`
- comparison_dimensions: `logical_type`
- summary: TiFlash and TiKV disagree on `logical_type` for `add-int32-literal`.
- evidence_refs: `inventory/first-expression-slice-tiflash-case-results.json#add-int32-literal`, `inventory/first-expression-slice-tikv-case-results.json#add-int32-literal`

### `add-int32-null-propagation`

- status: `drift`
- comparison_dimensions: `logical_type`
- summary: TiFlash and TiKV disagree on `logical_type` for `add-int32-null-propagation`.
- evidence_refs: `inventory/first-expression-slice-tiflash-case-results.json#add-int32-null-propagation`, `inventory/first-expression-slice-tikv-case-results.json#add-int32-null-propagation`

### `add-int32-overflow-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiFlash and TiKV both normalized `add-int32-overflow-error` as `arithmetic_overflow`.
- evidence_refs: `inventory/first-expression-slice-tiflash-case-results.json#add-int32-overflow-error`, `inventory/first-expression-slice-tikv-case-results.json#add-int32-overflow-error`
