# First Union Slice TiFlash-vs-TiKV Drift Report

Status: issue #368 harness checkpoint

Verified: 2026-03-21

## Evidence Source

- this checkpoint runs the current TiFlash and TiKV adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-union-slice-artifacts.md`

## Engines

- `tiflash`
- `tikv`

## Spec Refs

- `docs/design/first-union-aware-handoff-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-union-slice.md`
- `tests/differential/first-union-slice.md`

## Summary

- `match`: 5
- `drift`: 0
- `unsupported`: 0

## Cases

### `union-column-passthrough`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: tiflash and tikv both returned matching rows for `union-column-passthrough` (3 row(s), schema [u:dense_union<i:int32,n:int32?>]).
- evidence_refs: `inventory/first-union-slice-tiflash-case-results.json#union-column-passthrough`, `inventory/first-union-slice-tikv-case-results.json#union-column-passthrough`

### `union-variant-switch-preserve`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: tiflash and tikv both returned matching rows for `union-variant-switch-preserve` (3 row(s), schema [u:dense_union<i:int32,n:int32?>]).
- evidence_refs: `inventory/first-union-slice-tiflash-case-results.json#union-variant-switch-preserve`, `inventory/first-union-slice-tikv-case-results.json#union-variant-switch-preserve`

### `union-variant-null-preserve`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: tiflash and tikv both returned matching rows for `union-variant-null-preserve` (3 row(s), schema [u:dense_union<i:int32,n:int32?>]).
- evidence_refs: `inventory/first-union-slice-tiflash-case-results.json#union-variant-null-preserve`, `inventory/first-union-slice-tikv-case-results.json#union-variant-null-preserve`

### `union-missing-column-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: tiflash and tikv both normalized `union-missing-column-error` to `missing_column`.
- evidence_refs: `inventory/first-union-slice-tiflash-case-results.json#union-missing-column-error`, `inventory/first-union-slice-tikv-case-results.json#union-missing-column-error`

### `unsupported-nested-family-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: tiflash and tikv both normalized `unsupported-nested-family-error` to `unsupported_nested_family`.
- evidence_refs: `inventory/first-union-slice-tiflash-case-results.json#unsupported-nested-family-error`, `inventory/first-union-slice-tikv-case-results.json#unsupported-nested-family-error`
