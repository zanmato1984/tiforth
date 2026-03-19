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

## Initial Harness Boundary

Issue #139 defines this as a docs-first conformance checkpoint for the first post-gate filter boundary.

This file fixes expected behavior for future executable coverage without expanding current milestone-1 projection-only kernel code.

## Follow-On Coverage

When the first filter implementation issue lands, executable conformance coverage should include at least one fixture or test assertion for each canonical case above.
