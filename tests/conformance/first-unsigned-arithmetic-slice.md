# First Unsigned Arithmetic Slice Cases

Status: issue #300 docs checkpoint

Spec source: `docs/design/first-unsigned-arithmetic-slice.md`

## Canonical Cases

- `uint64 column passthrough`: `column(index)` over `uint64` preserves row
  count, `uint64` value tokens, SQL `NULL` placement, and field nullability
- `uint64 literal projection`: `literal<uint64>(value)` projects one stable
  `uint64` value token per row with `nullable = false` for non-null literal
  probes
- `uint64 add no overflow`: `add<uint64>(lhs, rhs)` over in-range operands
  returns `uint64` results without implicit widening or signedness changes
- `uint64 add null propagation`: nullable `uint64` operands propagate SQL
  `NULL` row-wise in `add<uint64>(lhs, rhs)`
- `uint64 add overflow`: `add<uint64>(lhs, rhs)` overflow fails as an execution
  error and does not wrap or saturate
- `uint64 predicate mixed keep/drop`: `is_not_null(column(index))` over mixed
  nullable `uint64` input keeps only non-null rows while preserving kept-row
  order
- `mixed signed and unsigned arithmetic`: mixed `int64` and `uint64` arithmetic
  requests fail as execution errors in this checkpoint
- `unsupported unsigned family`: unsigned logical types outside this checkpoint
  (for example `uint32`) fail as execution errors
- `missing column`: out-of-range `column(index)` in projection or predicate
  paths fails as an execution error

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-unsigned-arithmetic-slice.md`
- `adapters/first-unsigned-arithmetic-slice.md`

## Executable Harness Boundary

This checkpoint is docs-first only. Local executable kernel conformance for
unsigned arithmetic remains follow-on scope.
