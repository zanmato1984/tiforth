# First Differential Signed-Widening `add<int64>` Slice

Status: issue #426 design checkpoint, issue #434 executable checkpoint

Related issues:

- #276 `spec: define overflow behavior checkpoint for follow-on operator families`
- #409 `epic: complete function-family program`
- #414 `spec: pin the first complete function family as numeric add/plus`
- #422 `spec: complete the numeric add/plus family boundary`
- #423 `spec: fix decimal add result derivation for the numeric add/plus family`
- #426 `design: define first signed-widening add/int64 slice for the numeric add/plus family`
- #434 `milestone-1: implement first executable signed-widening add/int64 TiDB/TiFlash differential slice`

## Question

Which cross-engine comparison should `tiforth` define first for the narrow
signed `add<int64>` slice without widening the active epic into `float64`,
decimal execution, or broader signed-family coverage?

## Inputs Considered

- `docs/design/first-signed-widening-add-int64-slice.md`
- `docs/spec/functions/numeric-add-family-completion.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-signed-widening-add-int64-slice.md`
- `tests/differential/README.md`
- `tests/differential/first-expression-slice.md`
- `tests/differential/first-signed-widening-add-int64-slice-artifacts.md`
- issue #426

## First Slice Decision

### 1. Engines

The first signed-widening `add<int64>` differential slice compares `TiDB` and
`TiFlash`.

This keeps the next add-family checkpoint aligned with the current first
differential engine pair while leaving TiKV follow-on work separate.

### 2. Case Family

The first signed-widening `add<int64>` differential slice uses only the
families fixed by `docs/design/first-signed-widening-add-int64-slice.md`:

- passthrough `column(index)` over `int64`
- `add<int64>(lhs, rhs)` over exact `int64 + int64`
- `add<int64>(lhs, rhs)` over admitted widening pairs `int32 + int64` and
  `int64 + int32`

Include these cases first:

- `int64` column passthrough
- exact `int64` add success
- exact `int64` add null propagation
- admitted signed widening `int32 + int64`
- admitted signed widening `int64 + int32`
- exact `int64` add overflow execution error
- admitted signed widening `int32 + int64` overflow execution error
- missing column execution error

Defer these cases from the first signed-widening differential checkpoint:

- `literal<int64>` and `literal<int32>` projection probes
- `is_not_null(column(index))` over `int64`
- broader signed-width families such as `int16`
- `float64` add or decimal add cases
- mixed signed and unsigned success semantics
- runtime and ownership traces
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-signed-widening-add-int64-slice` as the stable `slice_id` in adapter
requests, normalized case results, and later drift reports for this checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-int64-basic`: one non-null `int64` field `s64` with rows `-7`, `0`,
  and `42`
- `first-int64-add-basic`: two non-null `int64` fields `lhs` and `rhs` with
  rows `(1, 2)`, `(-3, 4)`, and `(10, 20)`
- `first-int64-add-nullable`: two nullable `int64` fields `lhs` and `rhs` with
  rows `(null, 1)`, `(2, null)`, and `(-3, 4)`
- `first-int32-int64-add-basic`: one non-null `int32` field `lhs32` and one
  non-null `int64` field `rhs64` with rows `(1, 2)`, `(-3, 4)`, and
  `(100, 200)`
- `first-int64-int32-add-basic`: one non-null `int64` field `lhs64` and one
  non-null `int32` field `rhs32` with rows `(2, 1)`, `(4, -3)`, and
  `(200, 100)`
- `first-int64-add-overflow`: two non-null `int64` fields `lhs` and `rhs` with
  row `(9223372036854775807, 1)`
- `first-int32-int64-add-overflow`: one non-null `int32` field `lhs32` and one
  non-null `int64` field `rhs64` with row `(1, 9223372036854775807)`

### 2c. Shared Projection References

Use these stable `projection_ref` identifiers for this slice:

- `column-0`: passthrough `column(0)`
- `column-1`: out-of-range `column(1)` probe for missing-column behavior on
  one-field input
- `add-int64-column-0-column-1`: selected `add<int64>(column(0), column(1))`
  after exact-match or admitted signed-widening resolution

These refs describe semantic intent, not engine-native SQL text.

### 2d. Shared Case IDs

Use these stable `case_id` assignments for this first signed slice:

- `int64-column-passthrough`:
  `input_ref = first-int64-basic`, `projection_ref = column-0`
- `int64-add-basic`:
  `input_ref = first-int64-add-basic`,
  `projection_ref = add-int64-column-0-column-1`
- `int64-add-null-propagation`:
  `input_ref = first-int64-add-nullable`,
  `projection_ref = add-int64-column-0-column-1`
- `signed-widening-int32-plus-int64`:
  `input_ref = first-int32-int64-add-basic`,
  `projection_ref = add-int64-column-0-column-1`
- `signed-widening-int64-plus-int32`:
  `input_ref = first-int64-int32-add-basic`,
  `projection_ref = add-int64-column-0-column-1`
- `int64-add-overflow-error`:
  `input_ref = first-int64-add-overflow`,
  `projection_ref = add-int64-column-0-column-1`
- `signed-widening-int32-plus-int64-overflow-error`:
  `input_ref = first-int32-int64-add-overflow`,
  `projection_ref = add-int64-column-0-column-1`
- `int64-missing-column-error`:
  `input_ref = first-int64-basic`, `projection_ref = column-1`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-signed-widening-add-int64-slice.md`.

Stable artifact-carrier fields and checked-in filenames for this slice now live
in `tests/differential/first-signed-widening-add-int64-slice-artifacts.md`.

### 2e. Shared Spec References

For this first signed-widening differential slice, every shared request
currently uses this `spec_refs[]` set:

- `docs/design/first-signed-widening-add-int64-slice.md`
- `docs/spec/functions/numeric-add-family-completion.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-signed-widening-add-int64-slice.md`
- `tests/differential/first-signed-widening-add-int64-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first signed-widening slice, harness code compares:

- output field order and field names
- logical result types and nullability
- row count
- row values and null positions
- normalized outcome class: `rows` or `error`

For row outcomes in this slice, shared comparison normalizes non-null signed
values as JSON numeric scalars, with SQL `NULL` remaining carrier `null`.

For widening-success cases, the compared logical result type is still `int64`
because signature selection already chose `add<int64>`.

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

This first signed-widening slice should not compare:

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

This keeps adapters thin while letting the first signed-widening differential
slice target real engine behavior.

The minimal shared adapter request and response boundary for this slice lives in
`adapters/first-signed-widening-add-int64-slice.md`.

## Follow-On Boundary

Issue #429 already adds executable local kernel coverage for this checkpoint,
and issue #434 now adds executable TiDB/TiFlash adapter and harness coverage
plus checked-in `inventory/` artifacts.

Later issues may extend this slice to cover:

- `literal<int64>` and `literal<int32>` signed add probes
- `float64` and decimal add slices
- TiKV single-engine and pairwise checkpoints for signed add

This checkpoint now fixes the first signed-widening differential semantics,
request IDs, adapter-boundary shape, and normalized comparison rules exercised
by the TiDB-versus-TiFlash add-family checkpoint.
