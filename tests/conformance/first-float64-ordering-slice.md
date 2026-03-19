# First Float64 NaN/Infinity Ordering Slice Cases

Status: issue #194 docs checkpoint

Spec source: `docs/design/first-float64-ordering-slice.md`

## Canonical Cases

- `float64 column passthrough`: `column(index)` over finite `float64` input
  preserves row count, finite values, null positions, and field nullability
- `float64 special-value passthrough`: passthrough preserves `-Infinity`,
  `Infinity`, `NaN`, and signed-zero (`-0.0`/`0.0`) distinctions
- `float64 nullable passthrough`: nullable `float64` input remains nullable with
  unchanged null placement
- `float64 predicate all kept`: `is_not_null(column(0))` over non-null
  `float64` values (including `NaN` and infinities) keeps every row
- `float64 predicate mixed keep/drop`: mixed-null `float64` input keeps only
  non-null rows while preserving retained-row order and full-row passthrough
- `float64 NaN equality`: comparison intent for this checkpoint treats
  `NaN = NaN` and `NaN = finite` as false
- `float64 NaN ordered comparison`: comparison intent for this checkpoint
  treats ordered comparisons involving `NaN` as false
- `float64 canonical ordering`: canonical differential ordering intent is
  `-Infinity < finite < Infinity < NaN`, with `-0.0` before `0.0` only as a
  stable tie-break for row canonicalization
- `missing column`: out-of-range `column(index)` in projection or predicate
  paths fails as an execution error
- `unsupported floating family`: floating inputs outside `float64` (for example
  `float32`) fail as execution errors in this checkpoint

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-float64-ordering-slice.md`
- `adapters/first-float64-ordering-slice.md`

## Executable Harness Boundary

This is currently a docs-first checkpoint. Local executable kernel coverage for
`float64` special-value comparison and ordering semantics remains follow-on
work.
