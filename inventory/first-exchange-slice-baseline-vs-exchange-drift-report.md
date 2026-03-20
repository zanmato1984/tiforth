# First Exchange Slice Differential Parity Report

Status: issue #183 harness checkpoint, issue #221 artifact-carrier checkpoint

Verified: 2026-03-20

## Spec Refs

- `docs/design/first-in-contract-exchange-slice.md`
- `tests/differential/first-expression-slice.md`
- `tests/differential/first-filter-is-not-null-slice.md`
- `tests/differential/first-exchange-slice.md`
- `tests/differential/first-exchange-slice-artifacts.md`

## Summary

- `match`: 33
- `drift`: 0

## Cases

### `first-expression-slice/column-passthrough`

- subject: `tidb_case_results`
- engine: `tidb`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tidb` `column-passthrough` on `tidb_case_results`.

### `first-expression-slice/literal-int32-seven`

- subject: `tidb_case_results`
- engine: `tidb`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tidb` `literal-int32-seven` on `tidb_case_results`.

### `first-expression-slice/literal-int32-null`

- subject: `tidb_case_results`
- engine: `tidb`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tidb` `literal-int32-null` on `tidb_case_results`.

### `first-expression-slice/add-int32-literal`

- subject: `tidb_case_results`
- engine: `tidb`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tidb` `add-int32-literal` on `tidb_case_results`.

### `first-expression-slice/add-int32-null-propagation`

- subject: `tidb_case_results`
- engine: `tidb`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tidb` `add-int32-null-propagation` on `tidb_case_results`.

### `first-expression-slice/add-int32-overflow-error`

- subject: `tidb_case_results`
- engine: `tidb`
- status: `match`
- comparison_dimensions: `error_class`
- summary: Baseline and exchange paths matched for `tidb` `add-int32-overflow-error` on `tidb_case_results`.

### `first-expression-slice/column-passthrough`

- subject: `tiflash_case_results`
- engine: `tiflash`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tiflash` `column-passthrough` on `tiflash_case_results`.

### `first-expression-slice/literal-int32-seven`

- subject: `tiflash_case_results`
- engine: `tiflash`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tiflash` `literal-int32-seven` on `tiflash_case_results`.

### `first-expression-slice/literal-int32-null`

- subject: `tiflash_case_results`
- engine: `tiflash`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tiflash` `literal-int32-null` on `tiflash_case_results`.

### `first-expression-slice/add-int32-literal`

- subject: `tiflash_case_results`
- engine: `tiflash`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tiflash` `add-int32-literal` on `tiflash_case_results`.

### `first-expression-slice/add-int32-null-propagation`

- subject: `tiflash_case_results`
- engine: `tiflash`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tiflash` `add-int32-null-propagation` on `tiflash_case_results`.

### `first-expression-slice/add-int32-overflow-error`

- subject: `tiflash_case_results`
- engine: `tiflash`
- status: `match`
- comparison_dimensions: `error_class`
- summary: Baseline and exchange paths matched for `tiflash` `add-int32-overflow-error` on `tiflash_case_results`.

### `first-filter-is-not-null-slice/all-rows-kept`

- subject: `tidb_case_results`
- engine: `tidb`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tidb` `all-rows-kept` on `tidb_case_results`.

### `first-filter-is-not-null-slice/all-rows-dropped`

- subject: `tidb_case_results`
- engine: `tidb`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tidb` `all-rows-dropped` on `tidb_case_results`.

### `first-filter-is-not-null-slice/mixed-keep-drop`

- subject: `tidb_case_results`
- engine: `tidb`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tidb` `mixed-keep-drop` on `tidb_case_results`.

### `first-filter-is-not-null-slice/missing-column-error`

- subject: `tidb_case_results`
- engine: `tidb`
- status: `match`
- comparison_dimensions: `error_class`
- summary: Baseline and exchange paths matched for `tidb` `missing-column-error` on `tidb_case_results`.

### `first-filter-is-not-null-slice/unsupported-predicate-type-error`

- subject: `tidb_case_results`
- engine: `tidb`
- status: `match`
- comparison_dimensions: `error_class`
- summary: Baseline and exchange paths matched for `tidb` `unsupported-predicate-type-error` on `tidb_case_results`.

### `first-filter-is-not-null-slice/all-rows-kept`

- subject: `tiflash_case_results`
- engine: `tiflash`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tiflash` `all-rows-kept` on `tiflash_case_results`.

### `first-filter-is-not-null-slice/all-rows-dropped`

- subject: `tiflash_case_results`
- engine: `tiflash`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tiflash` `all-rows-dropped` on `tiflash_case_results`.

### `first-filter-is-not-null-slice/mixed-keep-drop`

- subject: `tiflash_case_results`
- engine: `tiflash`
- status: `match`
- comparison_dimensions: `field_name`, `field_nullability`, `logical_type`, `row_count`, `row_values`
- summary: Baseline and exchange paths matched for `tiflash` `mixed-keep-drop` on `tiflash_case_results`.

### `first-filter-is-not-null-slice/missing-column-error`

- subject: `tiflash_case_results`
- engine: `tiflash`
- status: `match`
- comparison_dimensions: `error_class`
- summary: Baseline and exchange paths matched for `tiflash` `missing-column-error` on `tiflash_case_results`.

### `first-filter-is-not-null-slice/unsupported-predicate-type-error`

- subject: `tiflash_case_results`
- engine: `tiflash`
- status: `match`
- comparison_dimensions: `error_class`
- summary: Baseline and exchange paths matched for `tiflash` `unsupported-predicate-type-error` on `tiflash_case_results`.

### `first-expression-slice/column-passthrough`

- subject: `tidb_vs_tiflash_drift`
- status: `match`
- comparison_dimensions: `drift_status`
- summary: Baseline and exchange paths kept drift status `match` for `column-passthrough`.

### `first-expression-slice/literal-int32-seven`

- subject: `tidb_vs_tiflash_drift`
- status: `match`
- comparison_dimensions: `drift_status`
- summary: Baseline and exchange paths kept drift status `match` for `literal-int32-seven`.

### `first-expression-slice/literal-int32-null`

- subject: `tidb_vs_tiflash_drift`
- status: `match`
- comparison_dimensions: `drift_status`
- summary: Baseline and exchange paths kept drift status `match` for `literal-int32-null`.

### `first-expression-slice/add-int32-literal`

- subject: `tidb_vs_tiflash_drift`
- status: `match`
- comparison_dimensions: `drift_status`
- summary: Baseline and exchange paths kept drift status `match` for `add-int32-literal`.

### `first-expression-slice/add-int32-null-propagation`

- subject: `tidb_vs_tiflash_drift`
- status: `match`
- comparison_dimensions: `drift_status`
- summary: Baseline and exchange paths kept drift status `match` for `add-int32-null-propagation`.

### `first-expression-slice/add-int32-overflow-error`

- subject: `tidb_vs_tiflash_drift`
- status: `match`
- comparison_dimensions: `drift_status`
- summary: Baseline and exchange paths kept drift status `unsupported` for `add-int32-overflow-error`.

### `first-filter-is-not-null-slice/all-rows-kept`

- subject: `tidb_vs_tiflash_drift`
- status: `match`
- comparison_dimensions: `drift_status`
- summary: Baseline and exchange paths kept drift status `match` for `all-rows-kept`.

### `first-filter-is-not-null-slice/all-rows-dropped`

- subject: `tidb_vs_tiflash_drift`
- status: `match`
- comparison_dimensions: `drift_status`
- summary: Baseline and exchange paths kept drift status `match` for `all-rows-dropped`.

### `first-filter-is-not-null-slice/mixed-keep-drop`

- subject: `tidb_vs_tiflash_drift`
- status: `match`
- comparison_dimensions: `drift_status`
- summary: Baseline and exchange paths kept drift status `match` for `mixed-keep-drop`.

### `first-filter-is-not-null-slice/missing-column-error`

- subject: `tidb_vs_tiflash_drift`
- status: `match`
- comparison_dimensions: `drift_status`
- summary: Baseline and exchange paths kept drift status `match` for `missing-column-error`.

### `first-filter-is-not-null-slice/unsupported-predicate-type-error`

- subject: `tidb_vs_tiflash_drift`
- status: `match`
- comparison_dimensions: `drift_status`
- summary: Baseline and exchange paths kept drift status `match` for `unsupported-predicate-type-error`.
