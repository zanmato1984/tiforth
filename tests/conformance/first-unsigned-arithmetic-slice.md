# First Unsigned Arithmetic Slice Cases

Status: issue #300 docs checkpoint, issue #308 local executable kernel checkpoint, issue #310 differential harness checkpoint

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

Issue #308 adds the first executable local kernel conformance coverage for this
checkpoint in:

- `crates/tiforth-kernel/tests/unsigned_arithmetic_slice.rs`

Issue #310 adds the first executable TiDB/TiFlash differential harness
coverage and checked-in artifact set for the same checkpoint in:

- `crates/tiforth-harness-differential/src/first_unsigned_arithmetic_slice.rs`
- `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json`
- `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json`
- `inventory/first-unsigned-arithmetic-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-unsigned-arithmetic-slice-tidb-vs-tiflash-drift-report.json`
