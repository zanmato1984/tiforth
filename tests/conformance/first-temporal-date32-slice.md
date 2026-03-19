# First Temporal `date32` Slice Cases

Status: issue #176 docs checkpoint, issue #178 executable local checkpoint

Spec source: `docs/design/first-temporal-semantic-slice.md`

## Canonical Cases

- `date32 column passthrough`: `column(index)` over `date32` preserves row
  count, day-domain values, null positions, and field nullability
- `date32 nullable passthrough`: nullable `date32` input remains nullable with
  unchanged null placement
- `date32 predicate all kept`: `is_not_null(column(0))` over non-null `date32`
  keeps every row
- `date32 predicate all dropped`: `is_not_null(column(0))` over all-null
  `date32` drops every row
- `date32 predicate mixed keep/drop`: mixed-null `date32` input keeps only
  non-null rows while preserving retained-row order and full-row passthrough
- `missing column`: out-of-range `column(index)` in projection or predicate
  paths fails as an execution error
- `unsupported temporal family`: temporal inputs outside `date32` (for example
  `timestamp`) fail as execution errors in this slice

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-temporal-date32-slice.md`
- `adapters/first-temporal-date32-slice.md`

## Executable Harness Boundary

This checkpoint now has local executable conformance coverage in:

- `crates/tiforth-kernel/tests/temporal_date32_slice.rs`
