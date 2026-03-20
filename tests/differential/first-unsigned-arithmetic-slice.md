# First Differential Unsigned Arithmetic Slice

Status: issue #300 design checkpoint

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #141 `spec: define signed/unsigned interaction checkpoint for initial coercion lattice`
- #276 `spec: define overflow behavior checkpoint for follow-on operator families`
- #300 `design: define first unsigned arithmetic semantic slice boundary`

## Question

Which cross-engine comparison should `tiforth` define first for a narrow
unsigned arithmetic checkpoint that adds unsigned expression coverage without
expanding into broad signed/unsigned coercion policy?

## Inputs Considered

- `docs/design/first-unsigned-arithmetic-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-unsigned-arithmetic-slice.md`
- `tests/differential/README.md`
- issue #300

## First Slice Decision

### 1. Engines

The first unsigned arithmetic differential slice compares `TiDB` and `TiFlash`.

This keeps the unsigned arithmetic checkpoint aligned with the current first
differential engine pair while keeping adapter and harness expansion narrow.

### 2. Case Family

The first unsigned arithmetic differential slice uses only families fixed by
`docs/design/first-unsigned-arithmetic-slice.md`:

- passthrough `column(index)` over `uint64`
- `literal<uint64>(value)` projection probes
- `add<uint64>(lhs, rhs)` arithmetic probes
- `is_not_null(column(index))` over `uint64`

Include these cases first:

- uint64 column passthrough
- uint64 literal projection
- uint64 add success
- uint64 add null propagation
- uint64 add overflow execution error
- uint64 predicate mixed keep/drop
- missing column execution error
- mixed signed/unsigned arithmetic execution error
- unsupported unsigned family execution error

Defer these cases from the first unsigned arithmetic differential checkpoint:

- unsigned families beyond `uint64`
- unsigned arithmetic families beyond `add<uint64>`
- unsigned cast and coercion expansion
- runtime and ownership traces (claim handoff, shrink, release, cancellation)
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-unsigned-arithmetic-slice` as the stable `slice_id` in adapter
requests, normalized case results, and later drift reports for this checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-uint64-basic`: one non-null `uint64` field `u` with rows
  `"0"`, `"7"`, and `"42"`
- `first-uint64-nullable`: one nullable `uint64` field `u` with rows
  `null`, `"5"`, `null`, and `"9"`
- `first-uint64-add-basic`: two non-null `uint64` fields `lhs` and `rhs` with
  rows `("1", "2")`, `("3", "4")`, and `("10", "20")`
- `first-uint64-add-nullable`: two nullable `uint64` fields `lhs` and `rhs`
  with rows `(null, "1")`, `("2", null)`, and `("3", "4")`
- `first-uint64-add-overflow`: two non-null `uint64` fields `lhs` and `rhs`
  with row `("18446744073709551615", "1")`
- `first-mixed-int64-uint64`: one non-null `int64` field `s` and one non-null
  `uint64` field `u` with row `("1", "1")`
- `first-uint32-basic`: one non-null `uint32` field `u32` used to assert
  unsupported-unsigned-family errors in this first unsigned slice

### 2c. Shared Projection And Filter References

Use these stable references for this slice:

- `projection_ref = column-0`: passthrough `column(0)`
- `projection_ref = column-2`: out-of-range `column(2)` probe for
  missing-column behavior
- `projection_ref = literal-uint64-7`: `literal<uint64>(7)` projection probe
- `projection_ref = add-uint64-column-0-column-1`:
  `add<uint64>(column(0), column(1))`
- `filter_ref = is-not-null-column-0`: `is_not_null(column(0))`

### 2d. Shared Case IDs

Use these stable `case_id` assignments for this first unsigned slice:

- `uint64-column-passthrough`:
  `input_ref = first-uint64-basic`, `projection_ref = column-0`
- `uint64-literal-projection`:
  `input_ref = first-uint64-basic`, `projection_ref = literal-uint64-7`
- `uint64-add-basic`:
  `input_ref = first-uint64-add-basic`,
  `projection_ref = add-uint64-column-0-column-1`
- `uint64-add-null-propagation`:
  `input_ref = first-uint64-add-nullable`,
  `projection_ref = add-uint64-column-0-column-1`
- `uint64-add-overflow-error`:
  `input_ref = first-uint64-add-overflow`,
  `projection_ref = add-uint64-column-0-column-1`
- `uint64-is-not-null-mixed-keep-drop`:
  `input_ref = first-uint64-nullable`,
  `filter_ref = is-not-null-column-0`
- `uint64-missing-column-error`:
  `input_ref = first-uint64-basic`, `projection_ref = column-2`
- `mixed-signed-unsigned-arithmetic-error`:
  `input_ref = first-mixed-int64-uint64`,
  `projection_ref = add-uint64-column-0-column-1`
- `unsupported-unsigned-family-error`:
  `input_ref = first-uint32-basic`, `projection_ref = column-0`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-unsigned-arithmetic-slice.md`.

### 2e. Shared Spec References

For this first unsigned arithmetic differential slice, every shared request
currently uses this `spec_refs[]` set:

- `docs/design/first-unsigned-arithmetic-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-unsigned-arithmetic-slice.md`
- `tests/differential/first-unsigned-arithmetic-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first unsigned arithmetic slice, harness code compares:

- output field order and field names
- logical result types and nullability
- row count
- row values and null positions
- normalized outcome class: `rows` or `error`

For `uint64` row outcomes in this slice, shared comparison normalizes non-null
values into canonical base-10 integer strings so full `uint64` range values
remain exact in the normalized carrier.

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

The first unsigned arithmetic slice should not compare:

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

This keeps adapters thin while letting the first unsigned arithmetic
differential slice target real engine behavior.

The minimal shared adapter request and response boundary for this slice lives in
`adapters/first-unsigned-arithmetic-slice.md`.

## Follow-On Boundary

Later issues may extend this slice to cover:

- TiKV adapter and pairwise drift checkpoints for this slice
- broader unsigned family and cast/coercion checkpoints
- checked-in artifact carriers and executable harness wiring for this slice
- unsigned arithmetic families beyond `add<uint64>`

Until then, this checkpoint fixes only the first unsigned arithmetic
differential semantics, request IDs, adapter-boundary shape, and normalized
comparison rules.
