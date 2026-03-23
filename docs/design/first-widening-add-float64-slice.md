# First Widening `add<float64>` Slice

Status: issue #427 design checkpoint

Verified: 2026-03-22

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #194 `design: define first float64 NaN, infinity, and ordering checkpoint`
- #409 `epic: complete function-family program`
- #414 `spec: pin the first complete function family as numeric add/plus`
- #422 `spec: complete the numeric add/plus family boundary`
- #426 `design: define first signed-widening add/int64 slice for the numeric add/plus family`
- #427 `design: define first widening add/float64 slice for the numeric add/plus family`

## Question

What is the first narrow shared slice that should admit `add<float64>` for the
numeric `add/plus` family without widening the active epic into `float32`,
decimal execution, or broader float-coercion policy?

## Inputs Considered

- `docs/spec/functions/numeric-add-family.md`
- `docs/spec/functions/numeric-add-family-completion.md`
- `docs/spec/type-system.md`
- `docs/contracts/data.md`
- `docs/design/first-float64-ordering-slice.md`
- `docs/design/first-signed-widening-add-int64-slice.md`
- `tests/conformance/first-float64-ordering-slice.md`
- `tests/differential/first-float64-ordering-slice.md`
- issue #427

## Design Summary

The first admitted `add<float64>` slice is a narrow docs-first checkpoint:

- admitted carrier baseline: passthrough `column(index)` over `float64`
- admitted add cases: exact `float64 + float64` plus the minimal widening
  pairs `int32 + float64`, `int64 + float64`, `float64 + int32`, and
  `float64 + int64`
- mixed `int32` and `int64` with `float64` widening is admitted, not deferred
- selected `add<float64>` reuses the existing `float64` NaN, infinity, and
  signed-zero rules instead of the integer overflow-as-error rule

This keeps the slice inside the active numeric add family while making the
already-admitted `int32 < int64 < float64` ladder concrete at the float64 leg.

## Admitted Slice Scope

This first float64 add slice is intentionally narrow.

It admits only:

- `column(index)` passthrough over `float64` as the slice's schema and
  row-carrier baseline
- `add<float64>(lhs, rhs)` when both operands already resolve to `float64`
- `add<float64>(lhs, rhs)` when signature selection widens exactly one operand
  from `int32` or `int64` to `float64` under the admitted ladder
- result nullability `lhs.nullable OR rhs.nullable`
- row-wise null propagation after the `add<float64>` overload is selected
- normalized float64 result rows using the existing canonical tokens from
  `docs/design/first-float64-ordering-slice.md`

It does not add a broader floating-family or mixed-numeric matrix beyond that
first result type.

## Admitted Widening Decision

For this slice, mixed `int32` and `int64` with `float64` widening is admitted.

That decision is required by the existing family-completion boundary rather
than being a new family-level expansion:

- issue #422 already admits `int32 + float64`, `int64 + float64`,
  `float64 + int32`, and `float64 + int64` as selections of `add<float64>`
- leaving those mixed-width pairs deferred here would keep the admitted
  `float64` leg abstract and would force later adapter and harness work to
  reopen the same question
- including exactly those four pairs is the smallest slice that turns the
  accepted widening leg into stable case IDs, adapter requests, and future
  evidence anchors

The slice does not widen farther to `float32`, and it does not admit decimal
or mixed signed/unsigned success semantics.

## Float64 Special-Value Boundary

This slice reuses the shared `float64` special-value boundary from
`docs/design/first-float64-ordering-slice.md`.

That means:

- once signature selection chooses `add<float64>`, execution does not convert
  float arithmetic into an overflow error path
- `NaN`, `Infinity`, `-Infinity`, `0.0`, and `-0.0` remain ordinary non-null
  `float64` row outcomes when produced by the selected overload
- shared differential and adapter docs normalize those outcomes with the same
  canonical tokens already accepted for the first float64 ordering slice:
  `-Infinity`, `Infinity`, `NaN`, and finite decimal strings with signed-zero
  distinction
- the first slice should therefore treat representative results such as
  `Infinity + finite -> Infinity`, `-Infinity + Infinity -> NaN`,
  `NaN + finite -> NaN`, and `-0.0 + -0.0 -> -0.0` as ordinary float64 row
  outputs rather than as arithmetic failures

## Widening Precision Boundary

This slice admits the integer-to-float64 widening pairs themselves, but it does
not reopen a broader precision-policy checkpoint.

For this first docs-first slice:

- named mixed success cases should use `int32` and `int64` inputs whose exact
  mathematical values are representable in `float64`
- shared comparison is over the selected `float64` result tokens after
  widening, not over preserved pre-widening integer identity
- near-`2^53` precision-loss probes, engine-specific float text formatting
  outside the accepted canonical tokens, and NaN-payload preservation remain
  deferred

This keeps the slice narrow while still making the admitted widening leg
reviewable.

## Operand-Support Boundary

This first slice keeps operand support column-backed only.

It does not require new docs-first coverage for:

- `literal<float64>(value)`
- `literal<int32>(value)` or `literal<int64>(value)` as widening probes
- `is_not_null(column(index))` over `float64` inside the add slice

Those can be admitted later if the active epic needs them, but they are not
the minimum boundary required to make the first widening `add<float64>` slice
stable.

## Coverage Anchor Docs

Issue #427 fixes the docs-first anchor set for this slice:

- design boundary: `docs/design/first-widening-add-float64-slice.md`
- conformance anchor: `tests/conformance/first-widening-add-float64-slice.md`
- differential slice:
  `tests/differential/first-widening-add-float64-slice.md`
- differential artifact carriers:
  `tests/differential/first-widening-add-float64-slice-artifacts.md`
- adapter request and response boundary:
  `adapters/first-widening-add-float64-slice.md`

Those docs fix stable `slice_id`, `input_ref`, `projection_ref`, `case_id`,
comparison-mode usage, normalized float64 row tokens, and planned evidence
filenames before executable work lands.

## Evidence Boundary

This checkpoint is docs-first only.

It fixes the planned checked-in artifact names for later executable coverage:

- `inventory/first-widening-add-float64-slice-tidb-case-results.json`
- `inventory/first-widening-add-float64-slice-tiflash-case-results.json`
- `inventory/first-widening-add-float64-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-widening-add-float64-slice-tidb-vs-tiflash-drift-report.json`

No executable kernel, adapter, harness, or `inventory/` claim is made by this
checkpoint itself.

## Out Of Scope For This Checkpoint

- exact or mixed `float32` add success semantics
- explicit integer-to-float64 precision-loss probes near `2^53`
- `literal<float64>`, `literal<int32>`, or `literal<int64>` add probes
- `is_not_null(column(index))` over `float64` inside this add slice
- decimal add and decimal/non-decimal rescue semantics
- mixed signed and unsigned success semantics
- TiKV single-engine or pairwise float64 add checkpoints
- executable local kernel coverage, executable TiDB/TiFlash harness coverage,
  and checked-in `inventory/` evidence

## Result

The first widening `add<float64>` slice is now explicit and narrow:
`float64` column passthrough plus exact and minimally widened `add<float64>`
coverage anchors, with shared NaN, infinity, and signed-zero result
normalization fixed for future conformance, differential, adapter, and
artifact work.
