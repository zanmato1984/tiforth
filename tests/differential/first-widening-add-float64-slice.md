# First Differential Widening `add<float64>` Slice

Status: issue #427 design checkpoint

Related issues:

- #194 `design: define first float64 NaN, infinity, and ordering checkpoint`
- #409 `epic: complete function-family program`
- #414 `spec: pin the first complete function family as numeric add/plus`
- #422 `spec: complete the numeric add/plus family boundary`
- #426 `design: define first signed-widening add/int64 slice for the numeric add/plus family`
- #427 `design: define first widening add/float64 slice for the numeric add/plus family`

## Question

Which cross-engine comparison should `tiforth` define first for the narrow
`add<float64>` slice without widening the active epic into `float32`, decimal
execution, or broader float-coercion policy?

## Inputs Considered

- `docs/design/first-widening-add-float64-slice.md`
- `docs/design/first-float64-ordering-slice.md`
- `docs/spec/functions/numeric-add-family-completion.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-widening-add-float64-slice.md`
- `tests/differential/README.md`
- `tests/differential/first-widening-add-float64-slice-artifacts.md`
- issue #427

## First Slice Decision

### 1. Engines

The first widening `add<float64>` differential slice compares `TiDB` and
`TiFlash`.

This keeps the next add-family checkpoint aligned with the current first
differential engine pair while leaving TiKV follow-on work separate.

### 2. Case Family

The first widening `add<float64>` differential slice uses only the families
fixed by `docs/design/first-widening-add-float64-slice.md`:

- passthrough `column(index)` over `float64`
- exact `add<float64>(lhs, rhs)` over `float64 + float64`
- admitted widening pairs `int32 + float64`, `int64 + float64`,
  `float64 + int32`, and `float64 + int64`

Include these cases first:

- `float64` column passthrough
- exact `float64` add success
- exact `float64` add null propagation
- exact `float64` add special-value propagation
- admitted widening `int32 + float64`
- admitted widening `int64 + float64`
- admitted widening `float64 + int32`
- admitted widening `float64 + int64`
- missing column execution error

Defer these cases from the first widening-float64 differential checkpoint:

- `literal<float64>`, `literal<int32>`, and `literal<int64>` add probes
- `is_not_null(column(index))` over `float64` inside this add slice
- explicit precision-loss probes near `2^53`
- exact or mixed `float32` add success semantics
- decimal add and decimal/non-decimal rescue semantics
- mixed signed and unsigned success semantics
- runtime and ownership traces
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-widening-add-float64-slice` as the stable `slice_id` in adapter
requests, normalized case results, and later drift reports for this
checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-float64-basic`: one non-null `float64` field `f64` with rows
  `-1.5`, `0.0`, and `2.25`
- `first-float64-add-basic`: two non-null `float64` fields `lhs64` and `rhs64`
  with rows `(1.5, 2.25)`, `(-3.0, 4.5)`, and `(0.25, 0.75)`
- `first-float64-add-nullable`: two nullable `float64` fields `lhs64` and
  `rhs64` with rows `(null, 1.0)`, `(2.5, null)`, and `(-3.0, 4.5)`
- `first-float64-add-special-values`: two non-null `float64` fields `lhs64`
  and `rhs64` with rows `(Infinity, 1.0)`, `(-Infinity, Infinity)`,
  `(NaN, 2.0)`, and `(-0.0, -0.0)`
- `first-int32-float64-add-basic`: one non-null `int32` field `lhs32` and one
  non-null `float64` field `rhs64` with rows `(1, 2.25)`, `(-3, 4.5)`, and
  `(7, 0.5)`
- `first-int64-float64-add-basic`: one non-null `int64` field `lhs64` and one
  non-null `float64` field `rhs64` with rows `(1, 2.25)`, `(-3, 4.5)`, and
  `(1000000000000, 0.5)`
- `first-float64-int32-add-basic`: one non-null `float64` field `lhs64` and
  one non-null `int32` field `rhs32` with rows `(2.25, 1)`, `(4.5, -3)`, and
  `(0.5, 7)`
- `first-float64-int64-add-basic`: one non-null `float64` field `lhs64` and
  one non-null `int64` field `rhs64` with rows `(2.25, 1)`, `(4.5, -3)`, and
  `(1.0, 1000000000000)`

Those widening fixtures intentionally keep all integer operands inside the
exact `float64` integer range so the first slice does not reopen a broader
precision-loss checkpoint.

### 2c. Shared Projection References

Use these stable `projection_ref` identifiers for this slice:

- `column-0`: passthrough `column(0)`
- `column-1`: out-of-range `column(1)` probe for missing-column behavior on
  one-field input
- `add-float64-column-0-column-1`: selected
  `add<float64>(column(0), column(1))` after exact-match or admitted widening
  resolution

These refs describe semantic intent, not engine-native SQL text.

### 2d. Shared Comparison Mode

Use this stable `comparison_mode` value for this slice:

- `row-order-preserved`: compare normalized `rows[]` in output order

### 2e. Shared Case IDs

Use these stable `case_id` assignments for this first widening-float64 slice:

- `float64-column-passthrough`:
  `input_ref = first-float64-basic`, `projection_ref = column-0`
- `float64-add-basic`:
  `input_ref = first-float64-add-basic`,
  `projection_ref = add-float64-column-0-column-1`
- `float64-add-null-propagation`:
  `input_ref = first-float64-add-nullable`,
  `projection_ref = add-float64-column-0-column-1`
- `float64-add-special-values`:
  `input_ref = first-float64-add-special-values`,
  `projection_ref = add-float64-column-0-column-1`
- `widening-int32-plus-float64`:
  `input_ref = first-int32-float64-add-basic`,
  `projection_ref = add-float64-column-0-column-1`
- `widening-int64-plus-float64`:
  `input_ref = first-int64-float64-add-basic`,
  `projection_ref = add-float64-column-0-column-1`
- `widening-float64-plus-int32`:
  `input_ref = first-float64-int32-add-basic`,
  `projection_ref = add-float64-column-0-column-1`
- `widening-float64-plus-int64`:
  `input_ref = first-float64-int64-add-basic`,
  `projection_ref = add-float64-column-0-column-1`
- `float64-add-missing-column-error`:
  `input_ref = first-float64-basic`, `projection_ref = column-1`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-widening-add-float64-slice.md`.

Stable artifact-carrier fields and planned checked-in filenames for this slice
now live in
`tests/differential/first-widening-add-float64-slice-artifacts.md`.

### 2f. Shared Spec References

For this first widening-float64 differential slice, every shared request
currently uses this `spec_refs[]` set:

- `docs/design/first-widening-add-float64-slice.md`
- `docs/design/first-float64-ordering-slice.md`
- `docs/spec/functions/numeric-add-family-completion.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-widening-add-float64-slice.md`
- `tests/differential/first-widening-add-float64-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first widening-float64 slice, harness code compares:

- output field order and field names
- logical result types and nullability
- row count
- row values and null positions
- normalized outcome class: `rows` or `error`

For row outcomes in this slice, shared comparison normalizes non-null
`float64` values into the canonical tokens already accepted for
`first-float64-ordering-slice`:

- `-Infinity`
- `Infinity`
- `NaN`
- finite decimal strings, including explicit signed-zero tokens (`-0.0`,
  `0.0`)

For widening-success cases, the compared logical result type is still
`float64` because signature selection already chose `add<float64>`.

For this slice, shared comparison is over those selected `float64` result
tokens only; it does not compare preserved pre-widening integer identity.

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

This first widening-float64 slice should not compare:

- physical plans
- timing or performance
- local runtime, admission, or ownership events

### 4. Adapter Responsibilities

Shared differential docs own:

- case IDs and spec references
- input sets plus projection intent references
- normalized result and error comparison rules
- drift classification meanings

Adapters remain responsible for:

- engine-native query construction or expression execution plumbing
- session setup, planner hints, and connection details
- translation from engine-native output into the normalized comparison carrier
- translation from engine-native failures into normalized `error_class` values

This keeps adapters thin while letting the first widening-float64 differential
slice target real engine behavior.

The minimal shared adapter request and response boundary for this slice lives
in `adapters/first-widening-add-float64-slice.md`.

## Follow-On Boundary

Later issues may extend this slice to cover:

- executable local kernel coverage for this checkpoint
- executable TiDB/TiFlash adapter and harness coverage plus checked-in
  `inventory/` artifacts
- explicit near-`2^53` precision-loss probes for admitted widening cases
- `literal<float64>`, `literal<int32>`, and `literal<int64>` add probes
- predicate companions for this add slice
- exact or mixed `float32` add checkpoints
- decimal add checkpoints and TiKV single-engine or pairwise float64 add work

Until then, this checkpoint fixes only the first widening-float64 differential
semantics, request IDs, adapter-boundary shape, and normalized comparison
rules for the TiDB-versus-TiFlash add-family follow-on.
