# First Signed-Widening Add Int64 Slice TiDB-vs-TiFlash Drift Report

Status: issue #434 harness checkpoint

Verified: 2026-03-22

## Evidence Source

- this checkpoint runs the current TiDB and TiFlash signed-widening adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-signed-widening-add-int64-slice-artifacts.md`

## Engines

- `tidb`
- `tiflash`

## Spec Refs

- `docs/design/first-signed-widening-add-int64-slice.md`
- `docs/spec/functions/numeric-add-family-completion.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-signed-widening-add-int64-slice.md`
- `tests/differential/first-signed-widening-add-int64-slice.md`

## Summary

- `match`: 8
- `drift`: 0
- `unsupported`: 0

## Cases

### `int64-column-passthrough`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `int64-column-passthrough` with field `s64` normalized as `int64` under `row-order-preserved`.
- evidence_refs: `inventory/first-signed-widening-add-int64-slice-tidb-case-results.json#int64-column-passthrough`, `inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json#int64-column-passthrough`

### `int64-add-basic`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `int64-add-basic` with field `sum` normalized as `int64` under `row-order-preserved`.
- evidence_refs: `inventory/first-signed-widening-add-int64-slice-tidb-case-results.json#int64-add-basic`, `inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json#int64-add-basic`

### `int64-add-null-propagation`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `int64-add-null-propagation` with field `sum` normalized as `int64` under `row-order-preserved`.
- evidence_refs: `inventory/first-signed-widening-add-int64-slice-tidb-case-results.json#int64-add-null-propagation`, `inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json#int64-add-null-propagation`

### `signed-widening-int32-plus-int64`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `signed-widening-int32-plus-int64` with field `sum` normalized as `int64` under `row-order-preserved`.
- evidence_refs: `inventory/first-signed-widening-add-int64-slice-tidb-case-results.json#signed-widening-int32-plus-int64`, `inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json#signed-widening-int32-plus-int64`

### `signed-widening-int64-plus-int32`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `signed-widening-int64-plus-int32` with field `sum` normalized as `int64` under `row-order-preserved`.
- evidence_refs: `inventory/first-signed-widening-add-int64-slice-tidb-case-results.json#signed-widening-int64-plus-int32`, `inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json#signed-widening-int64-plus-int32`

### `int64-add-overflow-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `int64-add-overflow-error` as `arithmetic_overflow`.
- evidence_refs: `inventory/first-signed-widening-add-int64-slice-tidb-case-results.json#int64-add-overflow-error`, `inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json#int64-add-overflow-error`

### `signed-widening-int32-plus-int64-overflow-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `signed-widening-int32-plus-int64-overflow-error` as `arithmetic_overflow`.
- evidence_refs: `inventory/first-signed-widening-add-int64-slice-tidb-case-results.json#signed-widening-int32-plus-int64-overflow-error`, `inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json#signed-widening-int32-plus-int64-overflow-error`

### `int64-missing-column-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `int64-missing-column-error` as `missing_column`.
- evidence_refs: `inventory/first-signed-widening-add-int64-slice-tidb-case-results.json#int64-missing-column-error`, `inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json#int64-missing-column-error`
