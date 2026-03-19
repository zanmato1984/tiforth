# First Filter `is_not_null` Slice Cases

Spec source: `docs/spec/first-filter-is-not-null.md`

## Canonical Cases

- `all rows kept`: `is_not_null(column(0))` keeps every row when `column(0)` has no nulls
- `all rows dropped`: `is_not_null(column(0))` drops every row when `column(0)` is all nulls
- `mixed keep/drop`: filter keeps only rows where `column(0)` is non-null and preserves retained-row order
- `schema preserved`: output keeps the same field set and field order as input
- `value passthrough`: retained rows preserve source values for every output column
- `missing column`: out-of-range `column(index)` fails as an execution error
- `unsupported predicate type`: non-`int32` predicate input fails as an execution error

## Executable Harness Boundary

Issue #149 turns this checkpoint into executable local conformance coverage in:

- `crates/tiforth-kernel/tests/filter_is_not_null.rs`

That coverage now asserts at least one executable case for each canonical behavior above:

- all-rows-kept result retention
- all-rows-dropped result retention
- mixed keep/drop with schema, order, and value passthrough checks
- missing-column execution error
- unsupported-predicate-type execution error

## Follow-On Coverage

Later issues may extend this first executable filter coverage with additional fixture snapshots, broader logical-type support, and cross-engine differential execution artifacts for the same slice IDs under `tests/differential/first-filter-is-not-null-slice.md`.
