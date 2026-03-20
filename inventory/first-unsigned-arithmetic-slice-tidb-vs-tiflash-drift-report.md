# First Unsigned Arithmetic Slice TiDB-vs-TiFlash Drift Report

Status: issue #310 harness checkpoint

Verified: 2026-03-20

## Evidence Source

- this checkpoint runs the current TiDB and TiFlash unsigned adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-unsigned-arithmetic-slice-artifacts.md`

## Engines

- `tidb`
- `tiflash`

## Spec Refs

- `docs/design/first-unsigned-arithmetic-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-unsigned-arithmetic-slice.md`
- `tests/differential/first-unsigned-arithmetic-slice.md`

## Summary

- `match`: 9
- `drift`: 0
- `unsupported`: 0

## Cases

### `uint64-column-passthrough`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `uint64-column-passthrough` with field `u` normalized as `uint64` under `row-order-preserved`.
- evidence_refs: `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json#uint64-column-passthrough`, `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json#uint64-column-passthrough`

### `uint64-literal-projection`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `uint64-literal-projection` with field `seven` normalized as `uint64` under `row-order-preserved`.
- evidence_refs: `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json#uint64-literal-projection`, `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json#uint64-literal-projection`

### `uint64-add-basic`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `uint64-add-basic` with field `sum` normalized as `uint64` under `row-order-preserved`.
- evidence_refs: `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json#uint64-add-basic`, `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json#uint64-add-basic`

### `uint64-add-null-propagation`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `uint64-add-null-propagation` with field `sum` normalized as `uint64` under `row-order-preserved`.
- evidence_refs: `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json#uint64-add-null-propagation`, `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json#uint64-add-null-propagation`

### `uint64-add-overflow-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `uint64-add-overflow-error` as `unsigned_overflow`.
- evidence_refs: `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json#uint64-add-overflow-error`, `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json#uint64-add-overflow-error`

### `uint64-is-not-null-mixed-keep-drop`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 2 row(s) for `uint64-is-not-null-mixed-keep-drop` with field `u` normalized as `uint64` under `row-order-preserved`.
- evidence_refs: `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json#uint64-is-not-null-mixed-keep-drop`, `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json#uint64-is-not-null-mixed-keep-drop`

### `uint64-missing-column-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `uint64-missing-column-error` as `missing_column`.
- evidence_refs: `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json#uint64-missing-column-error`, `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json#uint64-missing-column-error`

### `mixed-signed-unsigned-arithmetic-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `mixed-signed-unsigned-arithmetic-error` as `mixed_signed_unsigned`.
- evidence_refs: `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json#mixed-signed-unsigned-arithmetic-error`, `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json#mixed-signed-unsigned-arithmetic-error`

### `unsupported-unsigned-family-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `unsupported-unsigned-family-error` as `unsupported_unsigned_family`.
- evidence_refs: `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json#unsupported-unsigned-family-error`, `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json#unsupported-unsigned-family-error`
