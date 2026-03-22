# First Widening `add<float64>` Slice Cases

Status: issue #427 docs checkpoint, issue #432 local executable kernel checkpoint

Spec source: `docs/design/first-widening-add-float64-slice.md`

## Canonical Cases

- `float64 column passthrough`: `column(index)` over `float64` preserves row
  count, canonical float64 value tokens, and field nullability
- `float64 add basic`: exact `add<float64>(lhs, rhs)` returns `float64`
  results without falling back to integer overflow handling
- `float64 add null propagation`: nullable `float64` operands propagate SQL
  `NULL` row-wise through the selected `add<float64>` overload
- `float64 add special values`: exact `add<float64>(lhs, rhs)` treats
  `NaN`, `Infinity`, `-Infinity`, and signed zero as ordinary non-null row
  outcomes under the shared float64 normalization rules
- `widening int32 plus float64`: `int32 + float64` selects `add<float64>` and
  returns `float64` row outcomes
- `widening int64 plus float64`: `int64 + float64` selects `add<float64>` and
  returns `float64` row outcomes
- `widening float64 plus int32`: `float64 + int32` selects `add<float64>` and
  returns `float64` row outcomes
- `widening float64 plus int64`: `float64 + int64` selects `add<float64>` and
  returns `float64` row outcomes
- `missing column`: out-of-range `column(index)` fails as an execution error

## Differential Anchor

Cross-engine comparison for these cases is defined in:

- `tests/differential/first-widening-add-float64-slice.md`
- `adapters/first-widening-add-float64-slice.md`

## Executable Harness Boundary

Issue #432 adds the first executable local kernel conformance coverage for this
checkpoint in:

- `crates/tiforth-kernel/tests/widening_add_float64_slice.rs`

Executable TiDB/TiFlash differential harness coverage and checked-in
`inventory/` artifacts remain follow-on scope for the same active epic.
