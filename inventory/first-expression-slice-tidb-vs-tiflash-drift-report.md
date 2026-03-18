# First Expression Slice TiDB-vs-TiFlash Drift Report

Status: issue #113 harness checkpoint

Verified: 2026-03-18

## Evidence Source

- this checkpoint runs the current TiDB and TiFlash adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-expression-slice-artifacts.md`

## Engines

- `tidb`
- `tiflash`

## Spec Refs

- `docs/spec/milestone-1-expression-projection.md`
- `docs/spec/type-system.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/differential/first-expression-slice.md`

## Summary

- `match`: 5
- `drift`: 0
- `unsupported`: 1

## Cases

### `column-passthrough`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `column-passthrough` with field `a` normalized as `int32`.
- evidence_refs: `inventory/first-expression-slice-tidb-case-results.json#column-passthrough`, `inventory/first-expression-slice-tiflash-case-results.json#column-passthrough`

### `literal-int32-seven`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `literal-int32-seven` with field `lit` normalized as `int32`.
- evidence_refs: `inventory/first-expression-slice-tidb-case-results.json#literal-int32-seven`, `inventory/first-expression-slice-tiflash-case-results.json#literal-int32-seven`

### `literal-int32-null`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `literal-int32-null` with field `lit` normalized as `int32`.
- evidence_refs: `inventory/first-expression-slice-tidb-case-results.json#literal-int32-null`, `inventory/first-expression-slice-tiflash-case-results.json#literal-int32-null`

### `add-int32-literal`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `add-int32-literal` with field `a_plus_one` normalized as `int32`.
- evidence_refs: `inventory/first-expression-slice-tidb-case-results.json#add-int32-literal`, `inventory/first-expression-slice-tiflash-case-results.json#add-int32-literal`

### `add-int32-null-propagation`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `add-int32-null-propagation` with field `a_plus_one` normalized as `int32`.
- evidence_refs: `inventory/first-expression-slice-tidb-case-results.json#add-int32-null-propagation`, `inventory/first-expression-slice-tiflash-case-results.json#add-int32-null-propagation`

### `add-int32-overflow-error`

- status: `unsupported`
- comparison_dimensions: `error_class`
- summary: tidb normalized `add-int32-overflow-error` as `adapter_unavailable` while tiflash normalized it as `arithmetic_overflow`; the pair remains explicitly unsupported.
- evidence_refs: `inventory/first-expression-slice-tidb-case-results.json#add-int32-overflow-error`, `inventory/first-expression-slice-tiflash-case-results.json#add-int32-overflow-error`
- follow_up: Decide whether tidb should add a shared-`int32` overflow strategy for `add-int32-overflow-error` or remain explicitly unsupported.
