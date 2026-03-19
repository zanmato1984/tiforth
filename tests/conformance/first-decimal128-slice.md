# First Decimal `decimal128` Slice Cases

Status: issue #189 docs checkpoint

Spec source: `docs/design/first-decimal-semantic-slice.md`

## Canonical Cases

- `decimal128 column passthrough`: `column(index)` over
  `decimal128(precision, scale)` preserves row count, values, null positions,
  logical type, and precision/scale metadata
- `decimal128 nullable passthrough`: nullable `decimal128` input remains
  nullable with unchanged null placement and schema metadata
- `decimal128 predicate all kept`: `is_not_null(column(0))` over non-null
  `decimal128` keeps every row
- `decimal128 predicate all dropped`: `is_not_null(column(0))` over all-null
  `decimal128` drops every row
- `decimal128 predicate mixed keep/drop`: mixed-null `decimal128` input keeps
  only non-null rows while preserving retained-row order and full-row
  passthrough
- `missing column`: out-of-range `column(index)` in projection or predicate
  paths fails as an execution error
- `unsupported decimal family`: decimal inputs outside `decimal128` (for
  example `decimal256`) fail as execution errors in this slice
- `invalid decimal metadata`: `decimal128` inputs with invalid precision/scale
  metadata fail as execution errors in this slice

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-decimal128-slice.md`
- `adapters/first-decimal128-slice.md`

## Executable Harness Boundary

No local executable decimal conformance coverage exists yet in
`crates/tiforth-kernel`; this checkpoint currently fixes docs-first semantic
and differential anchors only.
