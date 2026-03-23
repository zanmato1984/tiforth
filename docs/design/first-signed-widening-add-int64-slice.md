# First Signed-Widening `add<int64>` Slice

Status: issue #426 design checkpoint

Verified: 2026-03-22

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #276 `spec: define overflow behavior checkpoint for follow-on operator families`
- #409 `epic: complete function-family program`
- #414 `spec: pin the first complete function family as numeric add/plus`
- #422 `spec: complete the numeric add/plus family boundary`
- #423 `spec: fix decimal add result derivation for the numeric add/plus family`
- #426 `design: define first signed-widening add/int64 slice for the numeric add/plus family`

## Question

What is the first narrow shared slice that should admit `add<int64>` for the
numeric `add/plus` family without widening the active epic into `float64`,
decimal execution, or broader signed-family coverage?

## Inputs Considered

- `docs/spec/functions/numeric-add-family.md`
- `docs/spec/functions/numeric-add-family-completion.md`
- `docs/spec/type-system.md`
- `docs/contracts/data.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/differential/first-expression-slice.md`
- `docs/design/first-unsigned-arithmetic-slice.md`
- issue #426

## Design Summary

The first admitted signed `add<int64>` slice is a narrow docs-first checkpoint:

- admitted carrier baseline: passthrough `column(index)` over `int64`
- admitted add cases: exact `int64 + int64` plus the minimal signed-widening
  pairs `int32 + int64` and `int64 + int32`
- mixed `int32` and `int64` widening is admitted, not deferred
- signed overflow remains an execution error whenever the selected overload is
  `add<int64>`

This keeps the slice inside the active numeric add family while making the
already-admitted `int32 < int64 < float64` ladder concrete at slice level.

## Admitted Slice Scope

This first signed slice is intentionally narrow.

It admits only:

- `column(index)` passthrough over `int64` as the slice's schema and row-carrier
  baseline
- `add<int64>(lhs, rhs)` when both operands already resolve to `int64`
- `add<int64>(lhs, rhs)` when signature selection widens exactly one operand
  from `int32` to `int64` under the admitted ladder
- result nullability `lhs.nullable OR rhs.nullable`
- row-wise null propagation after the `add<int64>` overload is selected

It does not add a new signed-family matrix beyond that first result type.

## Admitted Widening Decision

For this slice, mixed `int32` and `int64` add is admitted.

That decision is required by the existing family-completion boundary rather than
being a new family-level expansion:

- issue #422 already admits `int32 + int64` and `int64 + int32` as selections
  of `add<int64>`
- leaving those mixed-width pairs deferred here would keep the admitted ladder
  abstract and would force later adapter and harness work to reopen the same
  question
- including exactly those two widening pairs is the smallest slice that turns
  the accepted ladder into stable case IDs, adapter requests, and future
  evidence anchors

The slice does not widen farther to `float64`, and it does not admit broader
signed-width families such as `int16`, `int8`, or `int128`.

## Signed Overflow-As-Error Boundary

This slice reuses the existing signed overflow rule from the numeric add family:

- once signature selection chooses `add<int64>`, execution does not wrap,
  saturate, or widen again
- row-wise overflow is an execution error for exact `int64 + int64`
- row-wise overflow is also an execution error for admitted widening cases
  `int32 + int64` and `int64 + int32` because those pairs still select
  `add<int64>`
- shared differential and adapter docs for this slice normalize that failure as
  `arithmetic_overflow`

This keeps signed `add<int64>` aligned with the existing signed
overflow-as-error rule rather than inventing a separate widening-only exception.

## Operand-Support Boundary

This first slice keeps operand support column-backed only.

It does not require new docs-first coverage for:

- `literal<int64>(value)`
- `literal<int32>(value)` as a new widening probe
- `is_not_null(column(index))` over `int64`

Those can be admitted later if the active epic needs them, but they are not the
minimum boundary required to make the first signed `add<int64>` slice stable.

## Coverage Anchor Docs

Issue #426 fixes the docs-first anchor set for this slice:

- design boundary:
  `docs/design/first-signed-widening-add-int64-slice.md`
- conformance anchor:
  `tests/conformance/first-signed-widening-add-int64-slice.md`
- differential slice:
  `tests/differential/first-signed-widening-add-int64-slice.md`
- differential artifact carriers:
  `tests/differential/first-signed-widening-add-int64-slice-artifacts.md`
- adapter request and response boundary:
  `adapters/first-signed-widening-add-int64-slice.md`

Those docs fix stable `slice_id`, `input_ref`, `projection_ref`, `case_id`,
normalized error classes, and planned evidence filenames before executable work
lands.

## Evidence Boundary

This checkpoint is docs-first only.

It fixes the planned checked-in artifact names for later executable coverage:

- `inventory/first-signed-widening-add-int64-slice-tidb-case-results.json`
- `inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json`
- `inventory/first-signed-widening-add-int64-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-signed-widening-add-int64-slice-tidb-vs-tiflash-drift-report.json`

No executable kernel, adapter, harness, or `inventory/` claim is made by this
checkpoint itself.

## Out Of Scope For This Checkpoint
