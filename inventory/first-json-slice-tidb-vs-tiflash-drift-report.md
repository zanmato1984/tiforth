# First JSON Slice TiDB-vs-TiFlash Drift Report

Status: issue #356 harness checkpoint

Verified: 2026-03-21

## Evidence Source

- this checkpoint runs the current TiDB and TiFlash adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-json-slice-artifacts.md`

## Engines

- `tidb`
- `tiflash`

## Spec Refs

- `docs/design/first-json-semantic-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-json-slice.md`
- `tests/differential/first-json-slice.md`

## Summary

- `match`: 11
- `drift`: 0
- `unsupported`: 0

## Cases

### `json-column-passthrough`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `json-column-passthrough` with field `j` normalized as `json`.
- evidence_refs: `inventory/first-json-slice-tidb-case-results.json#json-column-passthrough`, `inventory/first-json-slice-tiflash-case-results.json#json-column-passthrough`

### `json-column-null-preserve`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 4 row(s) for `json-column-null-preserve` with field `j` normalized as `json`.
- evidence_refs: `inventory/first-json-slice-tidb-case-results.json#json-column-null-preserve`, `inventory/first-json-slice-tiflash-case-results.json#json-column-null-preserve`

### `json-object-canonicalization`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 4 row(s) for `json-object-canonicalization` with field `j` normalized as `json`.
- evidence_refs: `inventory/first-json-slice-tidb-case-results.json#json-object-canonicalization`, `inventory/first-json-slice-tiflash-case-results.json#json-object-canonicalization`

### `json-array-order-preserved`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 2 row(s) for `json-array-order-preserved` with field `j` normalized as `json`.
- evidence_refs: `inventory/first-json-slice-tidb-case-results.json#json-array-order-preserved`, `inventory/first-json-slice-tiflash-case-results.json#json-array-order-preserved`

### `json-is-not-null-all-kept`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `json-is-not-null-all-kept` with field `j` normalized as `json`.
- evidence_refs: `inventory/first-json-slice-tidb-case-results.json#json-is-not-null-all-kept`, `inventory/first-json-slice-tiflash-case-results.json#json-is-not-null-all-kept`

### `json-is-not-null-all-dropped`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 0 row(s) for `json-is-not-null-all-dropped` with field `j` normalized as `json`.
- evidence_refs: `inventory/first-json-slice-tidb-case-results.json#json-is-not-null-all-dropped`, `inventory/first-json-slice-tiflash-case-results.json#json-is-not-null-all-dropped`

### `json-is-not-null-mixed-keep-drop`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `json-is-not-null-mixed-keep-drop` with field `j` normalized as `json`.
- evidence_refs: `inventory/first-json-slice-tidb-case-results.json#json-is-not-null-mixed-keep-drop`, `inventory/first-json-slice-tiflash-case-results.json#json-is-not-null-mixed-keep-drop`

### `json-missing-column-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `json-missing-column-error` as `missing_column`.
- evidence_refs: `inventory/first-json-slice-tidb-case-results.json#json-missing-column-error`, `inventory/first-json-slice-tiflash-case-results.json#json-missing-column-error`

### `unsupported-json-ordering-comparison-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `unsupported-json-ordering-comparison-error` as `unsupported_json_comparison`.
- evidence_refs: `inventory/first-json-slice-tidb-case-results.json#unsupported-json-ordering-comparison-error`, `inventory/first-json-slice-tiflash-case-results.json#unsupported-json-ordering-comparison-error`

### `unsupported-json-cast-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `unsupported-json-cast-error` as `unsupported_json_cast`.
- evidence_refs: `inventory/first-json-slice-tidb-case-results.json#unsupported-json-cast-error`, `inventory/first-json-slice-tiflash-case-results.json#unsupported-json-cast-error`

### `unsupported-cast-to-json-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `unsupported-cast-to-json-error` as `unsupported_json_cast`.
- evidence_refs: `inventory/first-json-slice-tidb-case-results.json#unsupported-cast-to-json-error`, `inventory/first-json-slice-tiflash-case-results.json#unsupported-cast-to-json-error`
