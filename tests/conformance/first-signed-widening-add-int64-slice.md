# First Signed-Widening `add<int64>` Slice Cases

Status: issue #426 docs checkpoint, issue #429 local executable kernel checkpoint, issue #434 differential harness checkpoint

Spec source: `docs/design/first-signed-widening-add-int64-slice.md`

## Canonical Cases

- `int64 column passthrough`: `column(index)` over `int64` preserves row count,
  signed value tokens, and field nullability
- `int64 add no overflow`: exact `add<int64>(lhs, rhs)` returns `int64`
  results without wraparound or post-selection widening
- `int64 add null propagation`: nullable `int64` operands propagate SQL `NULL`
  row-wise through the selected `add<int64>` overload
- `signed widening int32 plus int64`: `int32 + int64` selects `add<int64>` and
  returns `int64` row outcomes
- `signed widening int64 plus int32`: `int64 + int32` selects `add<int64>` and
  returns `int64` row outcomes
- `int64 add overflow`: exact `add<int64>(lhs, rhs)` overflow fails as an
  execution error and does not wrap or saturate
- `signed widening int32 plus int64 overflow`: overflow in an admitted
  `int32 + int64` request still fails as an execution error because selection
  already chose `add<int64>`
- `missing column`: out-of-range `column(index)` fails as an execution error

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-signed-widening-add-int64-slice.md`
- `adapters/first-signed-widening-add-int64-slice.md`

## Executable Harness Boundary

Issue #429 adds the first executable local kernel conformance coverage for this
checkpoint in:

- `crates/tiforth-kernel/tests/signed_widening_add_int64_slice.rs`

Issue #434 adds executable TiDB/TiFlash differential harness coverage and
checked-in `inventory/` artifacts for these same cases in:

- `crates/tiforth-harness-differential/src/first_signed_widening_add_int64_slice.rs`
- `inventory/first-signed-widening-add-int64-slice-tidb-case-results.json`
- `inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json`
- `inventory/first-signed-widening-add-int64-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-signed-widening-add-int64-slice-tidb-vs-tiflash-drift-report.json`
