# First Map Slice TiDB-vs-TiFlash Drift Report

Status: issue #362 harness checkpoint

Verified: 2026-03-21

## Evidence Source

- this checkpoint runs the current TiDB and TiFlash adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-map-slice-artifacts.md`

## Engines

- `tidb`
- `tiflash`

## Spec Refs

- `docs/design/first-map-aware-handoff-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-map-slice.md`
- `tests/differential/first-map-slice.md`

## Summary

- `match`: 5
- `drift`: 0
- `unsupported`: 0

## Cases

### `map-column-passthrough`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `map-column-passthrough` with field `m` normalized as `map<int32,int32?>`.
- evidence_refs: `inventory/first-map-slice-tidb-case-results.json#map-column-passthrough`, `inventory/first-map-slice-tiflash-case-results.json#map-column-passthrough`

### `map-column-null-preserve`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `map-column-null-preserve` with field `m` normalized as `map<int32,int32?>`.
- evidence_refs: `inventory/first-map-slice-tidb-case-results.json#map-column-null-preserve`, `inventory/first-map-slice-tiflash-case-results.json#map-column-null-preserve`

### `map-value-null-preserve`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `map-value-null-preserve` with field `m` normalized as `map<int32,int32?>`.
- evidence_refs: `inventory/first-map-slice-tidb-case-results.json#map-value-null-preserve`, `inventory/first-map-slice-tiflash-case-results.json#map-value-null-preserve`

### `map-missing-column-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `map-missing-column-error` as `missing_column`.
- evidence_refs: `inventory/first-map-slice-tidb-case-results.json#map-missing-column-error`, `inventory/first-map-slice-tiflash-case-results.json#map-missing-column-error`

### `unsupported-nested-family-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `unsupported-nested-family-error` as `unsupported_nested_family`.
- evidence_refs: `inventory/first-map-slice-tidb-case-results.json#unsupported-nested-family-error`, `inventory/first-map-slice-tiflash-case-results.json#unsupported-nested-family-error`
