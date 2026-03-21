# First Collation String Slice TiDB-vs-TiFlash Drift Report

Status: issue #358 harness checkpoint

Verified: 2026-03-21

## Evidence Source

- this checkpoint runs the current TiDB and TiFlash adapter cores through deterministic harness fixture runners
- live engine connection and orchestration remain out of scope for this artifact set
- the stable artifact-carrier boundary lives in `tests/differential/first-collation-string-slice-artifacts.md`

## Engines

- `tidb`
- `tiflash`

## Spec Refs

- `docs/design/first-collation-string-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-collation-string-slice.md`
- `tests/differential/first-collation-string-slice.md`

## Summary

- `match`: 10
- `drift`: 0
- `unsupported`: 0

## Cases

### `utf8-column-passthrough-binary`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `utf8-column-passthrough-binary` with field `s` normalized as `utf8`.
- evidence_refs: `inventory/first-collation-string-slice-tidb-case-results.json#utf8-column-passthrough-binary`, `inventory/first-collation-string-slice-tiflash-case-results.json#utf8-column-passthrough-binary`

### `utf8-column-passthrough-unicode-ci`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `utf8-column-passthrough-unicode-ci` with field `s` normalized as `utf8`.
- evidence_refs: `inventory/first-collation-string-slice-tidb-case-results.json#utf8-column-passthrough-unicode-ci`, `inventory/first-collation-string-slice-tiflash-case-results.json#utf8-column-passthrough-unicode-ci`

### `utf8-is-not-null-mixed-keep-drop`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 2 row(s) for `utf8-is-not-null-mixed-keep-drop` with field `s` normalized as `utf8`.
- evidence_refs: `inventory/first-collation-string-slice-tidb-case-results.json#utf8-is-not-null-mixed-keep-drop`, `inventory/first-collation-string-slice-tiflash-case-results.json#utf8-is-not-null-mixed-keep-drop`

### `utf8-binary-eq-literal-alpha`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `utf8-binary-eq-literal-alpha` with field `cmp` normalized as `boolean`.
- evidence_refs: `inventory/first-collation-string-slice-tidb-case-results.json#utf8-binary-eq-literal-alpha`, `inventory/first-collation-string-slice-tiflash-case-results.json#utf8-binary-eq-literal-alpha`

### `utf8-unicode-ci-eq-literal-alpha`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `utf8-unicode-ci-eq-literal-alpha` with field `cmp` normalized as `boolean`.
- evidence_refs: `inventory/first-collation-string-slice-tidb-case-results.json#utf8-unicode-ci-eq-literal-alpha`, `inventory/first-collation-string-slice-tiflash-case-results.json#utf8-unicode-ci-eq-literal-alpha`

### `utf8-binary-lt-literal-beta`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 3 row(s) for `utf8-binary-lt-literal-beta` with field `cmp` normalized as `boolean`.
- evidence_refs: `inventory/first-collation-string-slice-tidb-case-results.json#utf8-binary-lt-literal-beta`, `inventory/first-collation-string-slice-tiflash-case-results.json#utf8-binary-lt-literal-beta`

### `utf8-unicode-ci-ordering-normalization`

- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: TiDB and TiFlash both returned 4 row(s) for `utf8-unicode-ci-ordering-normalization` with field `s` normalized as `utf8`.
- evidence_refs: `inventory/first-collation-string-slice-tidb-case-results.json#utf8-unicode-ci-ordering-normalization`, `inventory/first-collation-string-slice-tiflash-case-results.json#utf8-unicode-ci-ordering-normalization`

### `utf8-missing-column-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `utf8-missing-column-error` as `missing_column`.
- evidence_refs: `inventory/first-collation-string-slice-tidb-case-results.json#utf8-missing-column-error`, `inventory/first-collation-string-slice-tiflash-case-results.json#utf8-missing-column-error`

### `unknown-collation-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `unknown-collation-error` as `unknown_collation`.
- evidence_refs: `inventory/first-collation-string-slice-tidb-case-results.json#unknown-collation-error`, `inventory/first-collation-string-slice-tiflash-case-results.json#unknown-collation-error`

### `unsupported-collation-type-error`

- status: `match`
- comparison_dimensions: `error_class`
- summary: TiDB and TiFlash both normalized `unsupported-collation-type-error` as `unsupported_collation_type`.
- evidence_refs: `inventory/first-collation-string-slice-tidb-case-results.json#unsupported-collation-type-error`, `inventory/first-collation-string-slice-tiflash-case-results.json#unsupported-collation-type-error`
