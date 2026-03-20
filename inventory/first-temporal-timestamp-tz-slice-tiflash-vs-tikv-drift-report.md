# First Temporal Timestamp-Timezone Slice TiFlash-vs-TiKV Drift Report

Status: issue #306 follow-on harness checkpoint

Verified: 2026-03-20

## Evidence Source

- this checkpoint runs the current TiFlash and TiKV adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-temporal-timestamp-tz-slice-artifacts.md`

## Engines

- `tiflash`
- `tikv`

## Spec Refs

- `docs/design/first-temporal-timestamp-tz-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-temporal-timestamp-tz-slice.md`
- `tests/differential/first-temporal-timestamp-tz-slice.md`

## Summary

- `match`: 10
- `drift`: 0
- `unsupported`: 0

## Cases

### `timestamp-tz-column-passthrough`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 3 row(s) for `timestamp-tz-column-passthrough` with field `ts` normalized as `timestamp_tz(us)`.
- evidence_refs: `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json#timestamp-tz-column-passthrough`, `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json#timestamp-tz-column-passthrough`

### `timestamp-tz-equivalent-instant-normalization`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 3 row(s) for `timestamp-tz-equivalent-instant-normalization` with field `ts` normalized as `timestamp_tz(us)`.
- evidence_refs: `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json#timestamp-tz-equivalent-instant-normalization`, `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json#timestamp-tz-equivalent-instant-normalization`

### `timestamp-tz-column-null-preserve`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 4 row(s) for `timestamp-tz-column-null-preserve` with field `ts` normalized as `timestamp_tz(us)`.
- evidence_refs: `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json#timestamp-tz-column-null-preserve`, `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json#timestamp-tz-column-null-preserve`

### `timestamp-tz-is-not-null-all-kept`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 3 row(s) for `timestamp-tz-is-not-null-all-kept` with field `ts` normalized as `timestamp_tz(us)`.
- evidence_refs: `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json#timestamp-tz-is-not-null-all-kept`, `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json#timestamp-tz-is-not-null-all-kept`

### `timestamp-tz-is-not-null-all-dropped`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 0 row(s) for `timestamp-tz-is-not-null-all-dropped` with field `ts` normalized as `timestamp_tz(us)`.
- evidence_refs: `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json#timestamp-tz-is-not-null-all-dropped`, `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json#timestamp-tz-is-not-null-all-dropped`

### `timestamp-tz-is-not-null-mixed-keep-drop`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 2 row(s) for `timestamp-tz-is-not-null-mixed-keep-drop` with field `ts` normalized as `timestamp_tz(us)`.
- evidence_refs: `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json#timestamp-tz-is-not-null-mixed-keep-drop`, `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json#timestamp-tz-is-not-null-mixed-keep-drop`

### `timestamp-tz-order-asc-nulls-last`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiFlash and TiKV both returned 4 row(s) for `timestamp-tz-order-asc-nulls-last` with field `ts` normalized as `timestamp_tz(us)`.
- evidence_refs: `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json#timestamp-tz-order-asc-nulls-last`, `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json#timestamp-tz-order-asc-nulls-last`

### `timestamp-tz-missing-column-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiFlash and TiKV both normalized `timestamp-tz-missing-column-error` as `missing_column`.
- evidence_refs: `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json#timestamp-tz-missing-column-error`, `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json#timestamp-tz-missing-column-error`

### `unsupported-temporal-timestamp-without-timezone-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiFlash and TiKV both normalized `unsupported-temporal-timestamp-without-timezone-error` as `unsupported_temporal_type`.
- evidence_refs: `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json#unsupported-temporal-timestamp-without-timezone-error`, `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json#unsupported-temporal-timestamp-without-timezone-error`

### `unsupported-temporal-timestamp-unit-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiFlash and TiKV both normalized `unsupported-temporal-timestamp-unit-error` as `unsupported_temporal_unit`.
- evidence_refs: `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json#unsupported-temporal-timestamp-unit-error`, `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json#unsupported-temporal-timestamp-unit-error`
