# First Struct Slice TiDB-vs-TiFlash Drift Report

Status: issue #360 harness checkpoint

Verified: 2026-03-21

## Evidence Source

- this checkpoint runs the current TiDB and TiFlash adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-struct-slice-artifacts.md`

## Engines

- `tidb`
- `tiflash`

## Spec Refs

- `docs/design/first-struct-aware-handoff-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-struct-slice.md`
- `tests/differential/first-struct-slice.md`

## Summary

- `match`: 5
- `drift`: 0
- `unsupported`: 0

## Cases

### `struct-column-passthrough`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `struct-column-passthrough` with field `s` normalized as `struct<a:int32,b:int32?>`.
- evidence_refs: `inventory/first-struct-slice-tidb-case-results.json#struct-column-passthrough`, `inventory/first-struct-slice-tiflash-case-results.json#struct-column-passthrough`

### `struct-column-null-preserve`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `struct-column-null-preserve` with field `s` normalized as `struct<a:int32,b:int32?>`.
- evidence_refs: `inventory/first-struct-slice-tidb-case-results.json#struct-column-null-preserve`, `inventory/first-struct-slice-tiflash-case-results.json#struct-column-null-preserve`

### `struct-child-null-preserve`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `struct-child-null-preserve` with field `s` normalized as `struct<a:int32,b:int32?>`.
- evidence_refs: `inventory/first-struct-slice-tidb-case-results.json#struct-child-null-preserve`, `inventory/first-struct-slice-tiflash-case-results.json#struct-child-null-preserve`

### `struct-missing-column-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `struct-missing-column-error` as `missing_column`.
- evidence_refs: `inventory/first-struct-slice-tidb-case-results.json#struct-missing-column-error`, `inventory/first-struct-slice-tiflash-case-results.json#struct-missing-column-error`

### `unsupported-nested-family-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `unsupported-nested-family-error` as `unsupported_nested_family`.
- evidence_refs: `inventory/first-struct-slice-tidb-case-results.json#unsupported-nested-family-error`, `inventory/first-struct-slice-tiflash-case-results.json#unsupported-nested-family-error`
