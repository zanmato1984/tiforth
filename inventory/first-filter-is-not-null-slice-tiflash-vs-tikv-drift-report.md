# First Filter Slice TiFlash-vs-TiKV Drift Report

Status: issue #249 follow-on harness checkpoint

Verified: 2026-03-20

## Evidence Source

- this checkpoint runs the current TiFlash and TiKV adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-filter-is-not-null-slice-artifacts.md`

## Engines

- `tiflash`
- `tikv`

## Spec Refs

- `docs/spec/first-filter-is-not-null.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-filter-is-not-null-slice.md`
- `tests/differential/first-filter-is-not-null-slice.md`

## Summary

- `match`: 5
- `drift`: 0
- `unsupported`: 0

## Cases

### `all-rows-kept`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 3 row(s) for `all-rows-kept` with field `a` normalized as `int32`.
- evidence_refs: `inventory/first-filter-is-not-null-slice-tiflash-case-results.json#all-rows-kept`, `inventory/first-filter-is-not-null-slice-tikv-case-results.json#all-rows-kept`

### `all-rows-dropped`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 0 row(s) for `all-rows-dropped` with field `a` normalized as `int32`.
- evidence_refs: `inventory/first-filter-is-not-null-slice-tiflash-case-results.json#all-rows-dropped`, `inventory/first-filter-is-not-null-slice-tikv-case-results.json#all-rows-dropped`

### `mixed-keep-drop`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned matching row output for `mixed-keep-drop`.
- evidence_refs: `inventory/first-filter-is-not-null-slice-tiflash-case-results.json#mixed-keep-drop`, `inventory/first-filter-is-not-null-slice-tikv-case-results.json#mixed-keep-drop`

### `missing-column-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiFlash and TiKV both normalized `missing-column-error` as `missing_column`.
- evidence_refs: `inventory/first-filter-is-not-null-slice-tiflash-case-results.json#missing-column-error`, `inventory/first-filter-is-not-null-slice-tikv-case-results.json#missing-column-error`

### `unsupported-predicate-type-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiFlash and TiKV both normalized `unsupported-predicate-type-error` as `unsupported_predicate_type`.
- evidence_refs: `inventory/first-filter-is-not-null-slice-tiflash-case-results.json#unsupported-predicate-type-error`, `inventory/first-filter-is-not-null-slice-tikv-case-results.json#unsupported-predicate-type-error`
