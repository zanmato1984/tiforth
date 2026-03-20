# First Differential Decimal `decimal128` Slice

Status: issue #189 design checkpoint, issue #206 executable differential checkpoint, issue #278 TiKV boundary checkpoint

Related issues:

- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #189 `design: define first decimal semantic slice boundary`
- #278 `design: define first TiKV decimal128 adapter request/response surface`

## Question

Which cross-engine comparison should `tiforth` define first for a narrow
`decimal128` semantic checkpoint?

## Inputs Considered

- `docs/design/first-decimal-semantic-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-decimal128-slice.md`
- `tests/differential/README.md`
- `adapters/first-decimal128-slice-tikv.md`
- issue #189
- issue #278

## First Slice Decision

### 1. Engines

The first decimal differential slice compares `TiDB` and `TiFlash`.

This keeps the decimal checkpoint aligned with the existing first differential
engine pair while keeping adapter and harness expansion narrow.

### 2. Case Family

The first decimal differential slice uses only families fixed by
`docs/design/first-decimal-semantic-slice.md`:

- passthrough `column(index)` over `decimal128`
- `is_not_null(column(index))` over `decimal128`

Include these cases first:

- decimal128 column passthrough
- decimal128 column null preservation
- decimal128 predicate all kept
- decimal128 predicate all dropped
- decimal128 predicate mixed keep/drop
- missing column execution error
- unsupported decimal family execution error
- invalid decimal metadata execution error

Defer these cases from the first decimal differential checkpoint:

- decimal arithmetic, cast, and coercion behavior
- decimal rescaling or rounding policy
- runtime and ownership traces (claim handoff, shrink, release, cancellation)
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-decimal128-slice` as the stable `slice_id` in adapter requests,
normalized case results, and later drift reports for this checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-decimal128-basic`: one non-null `decimal128(10,2)` field `d` with
  rows `"1.00"`, `"2.50"`, `"-3.75"`
- `first-decimal128-nullable`: one nullable `decimal128(10,2)` field `d` with
  rows `"1.00"`, `null`, `"-3.75"`, `null`
- `first-decimal128-all-null`: one nullable `decimal128(10,2)` field `d` with
  rows `null`, `null`, `null`
- `first-decimal256-basic`: one non-null `decimal256(40,4)` field `d256` used
  to assert unsupported-decimal-family errors in this first decimal slice
- `first-decimal128-invalid-scale`: one field whose declared
  `decimal128(10,12)` precision/scale metadata intentionally violates
  `scale <= precision` for invalid-metadata error coverage

### 2c. Shared Projection And Filter References

Use these stable references for this slice:

- `projection_ref = column-0`: passthrough `column(0)`
- `filter_ref = is-not-null-column-0`: `is_not_null(column(0))`
- `filter_ref = is-not-null-column-1`: `is_not_null(column(1))`

### 2d. Shared Case IDs

Use these stable `case_id` assignments for this first decimal slice:

- `decimal128-column-passthrough`:
  `input_ref = first-decimal128-basic`, `projection_ref = column-0`
- `decimal128-column-null-preserve`:
  `input_ref = first-decimal128-nullable`, `projection_ref = column-0`
- `decimal128-is-not-null-all-kept`:
  `input_ref = first-decimal128-basic`, `filter_ref = is-not-null-column-0`
- `decimal128-is-not-null-all-dropped`:
  `input_ref = first-decimal128-all-null`, `filter_ref = is-not-null-column-0`
- `decimal128-is-not-null-mixed-keep-drop`:
  `input_ref = first-decimal128-nullable`,
  `filter_ref = is-not-null-column-0`
- `decimal128-missing-column-error`:
  `input_ref = first-decimal128-basic`, `filter_ref = is-not-null-column-1`
- `unsupported-decimal-type-error`:
  `input_ref = first-decimal256-basic`, `projection_ref = column-0`
- `invalid-decimal-metadata-error`:
  `input_ref = first-decimal128-invalid-scale`, `projection_ref = column-0`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-decimal128-slice.md`.

The TiKV request and response boundary for this same slice is defined in
`adapters/first-decimal128-slice-tikv.md`.

### 2e. Shared Spec References

For this first decimal differential slice, every shared request currently uses
this `spec_refs[]` set:

- `docs/design/first-decimal-semantic-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-decimal128-slice.md`
- `tests/differential/first-decimal128-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first decimal slice, harness code compares:

- output field order and field names
- logical result types and nullability
- decimal precision and scale metadata through the normalized `logical_type`
  token (`decimal128(<precision>,<scale>)`)
- row count
- row values and null positions
- normalized outcome class: `rows` or `error`

For `decimal128` row outcomes, shared comparison should use canonical decimal
string values, not engine-native floating or scientific-notation renderings.

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

The first decimal slice should not compare:

- physical plans
- timing or performance
- local runtime, admission, or ownership events

### 4. Adapter Responsibilities

Shared differential docs own:

- case IDs and spec references
- input sets plus projection and filter intent references
- normalized result and error comparison rules
- drift classification meanings

Adapters remain responsible for:

- engine-native query construction or expression execution plumbing
- session setup, planner hints, and connection details
- translation from engine-native output into the normalized comparison carrier
- translation from engine-native failures into normalized `error_class` values

This keeps adapters thin while letting the first decimal differential slice
target real engine behavior.

The minimal shared adapter request and response boundary for this slice lives
in `adapters/first-decimal128-slice.md`, and the TiKV request and response
boundary for the same slice is anchored in
`adapters/first-decimal128-slice-tikv.md`.

## Follow-On Boundary

Later issues may extend this slice to cover:

- broader decimal logical families and precision/scale policy
- decimal arithmetic, cast, and coercion semantics
- executable TiKV single-engine and pairwise checkpoints for this slice

Issue #206 now adds executable adapter/harness wiring plus checked-in decimal
`case-results` and `drift-report` artifacts for this slice.

Issue #278 now adds the docs-first TiKV request and response boundary in
`adapters/first-decimal128-slice-tikv.md`.

Until then, this checkpoint fixes only the first decimal differential
semantics, request IDs, adapter-boundary shape, normalized comparison
rules.
