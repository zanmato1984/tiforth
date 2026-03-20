# First Differential Float64 NaN/Infinity Ordering Slice

Status: issue #194 design checkpoint, issue #208 harness checkpoint, issue #286 TiKV boundary checkpoint

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #194 `design: define first float64 NaN, infinity, and ordering checkpoint`
- #208 `harness: execute first-float64-ordering-slice differential artifacts for TiDB and TiFlash`
- #286 `design: define TiKV adapter boundary for first-float64-ordering-slice`

## Question

Which cross-engine comparison should `tiforth` define first for a narrow
`float64` special-value and ordering checkpoint?

## Inputs Considered

- `docs/design/first-float64-ordering-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-float64-ordering-slice.md`
- `tests/differential/README.md`
- `adapters/first-float64-ordering-slice-tikv.md`
- issue #194
- issue #208
- issue #286

## First Slice Decision

### 1. Engines

The first float64 differential slice compares `TiDB` and `TiFlash`.

This keeps the float64 checkpoint aligned with the existing first differential
engine pair while keeping adapter and harness expansion narrow.

### 2. Case Family

The first float64 differential slice uses only families fixed by
`docs/design/first-float64-ordering-slice.md`:

- passthrough `column(index)` over `float64`
- `is_not_null(column(index))` over `float64`
- canonical row-comparison mode for float64 special-value ordering intent

Include these cases first:

- float64 column passthrough
- float64 special-value passthrough
- float64 predicate all kept
- float64 predicate mixed keep/drop
- float64 canonical ordering normalization
- missing column execution error
- unsupported floating family execution error

Defer these cases from the first float64 differential checkpoint:

- float arithmetic, cast, or coercion behavior
- SQL `ORDER BY` semantics, null placement policy, and collation interaction
- runtime and ownership traces (claim handoff, shrink, release, cancellation)
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-float64-ordering-slice` as the stable `slice_id` in adapter
requests, normalized case results, and later drift reports for this checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-float64-basic`: one non-null `float64` field `f` with rows
  `-1.5`, `0.0`, `2.25`
- `first-float64-special-values`: one non-null `float64` field `f` with rows
  `-Infinity`, `-0.0`, `0.0`, `Infinity`, `NaN`
- `first-float64-nullable-special-values`: one nullable `float64` field `f`
  with rows `null`, `-Infinity`, `null`, `NaN`, `1.0`
- `first-float64-ordering-scramble`: one non-null `float64` field `f` with
  rows `NaN`, `1.0`, `-Infinity`, `Infinity`, `0.0`, `-0.0`
- `first-float32-basic`: one non-null `float32` field `f32` used to assert
  unsupported-floating-family errors in this first float64 slice

### 2c. Shared Projection And Filter References

Use these stable references for this slice:

- `projection_ref = column-0`: passthrough `column(0)`
- `filter_ref = is-not-null-column-0`: `is_not_null(column(0))`
- `filter_ref = is-not-null-column-1`: `is_not_null(column(1))`

### 2d. Shared Comparison Modes

Use these stable `comparison_mode` values for this slice:

- `comparison_mode = row-order-preserved`: compare `rows[]` in output order
- `comparison_mode = float64-multiset-canonical`: compare `rows[]` as a
  canonicalized multiset by the shared float64 total-order rule below

### 2e. Shared Case IDs

Use these stable `case_id` assignments for this first float64 slice:

- `float64-column-passthrough`:
  `input_ref = first-float64-basic`, `projection_ref = column-0`,
  `comparison_mode = row-order-preserved`
- `float64-special-values-passthrough`:
  `input_ref = first-float64-special-values`, `projection_ref = column-0`,
  `comparison_mode = row-order-preserved`
- `float64-is-not-null-all-kept`:
  `input_ref = first-float64-special-values`,
  `filter_ref = is-not-null-column-0`,
  `comparison_mode = row-order-preserved`
- `float64-is-not-null-mixed-keep-drop`:
  `input_ref = first-float64-nullable-special-values`,
  `filter_ref = is-not-null-column-0`,
  `comparison_mode = row-order-preserved`
- `float64-canonical-ordering-normalization`:
  `input_ref = first-float64-ordering-scramble`, `projection_ref = column-0`,
  `comparison_mode = float64-multiset-canonical`
- `float64-missing-column-error`:
  `input_ref = first-float64-basic`, `filter_ref = is-not-null-column-1`
- `unsupported-floating-type-error`:
  `input_ref = first-float32-basic`, `projection_ref = column-0`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-float64-ordering-slice.md`.

The TiKV request and response boundary for this same slice is defined in
`adapters/first-float64-ordering-slice-tikv.md`.

### 2f. Shared Spec References

For this first float64 differential slice, every shared request currently uses
this `spec_refs[]` set:

- `docs/design/first-float64-ordering-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-float64-ordering-slice.md`
- `tests/differential/first-float64-ordering-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first float64 slice, harness code compares:

- output field order and field names
- logical result types and nullability
- row count
- row values and null positions
- normalized outcome class: `rows` or `error`

For `float64` row outcomes in this slice, shared comparison normalizes values
into canonical tokens:

- `-Infinity`
- `Infinity`
- `NaN`
- finite decimal strings, including explicit signed-zero tokens (`-0.0`, `0.0`)

When `comparison_mode = row-order-preserved`, compare normalized rows in order.

When `comparison_mode = float64-multiset-canonical`, compare normalized rows
after stable sort by this total-order rule:

1. `-Infinity`
2. finite values in numeric ascending order
3. `Infinity`
4. `NaN`

Within finite values, `-0.0` sorts before `0.0` only as a deterministic
canonicalization tie-break. This rule is a harness comparison convention for
this slice; it is not a claim about shared SQL `ORDER BY` semantics.

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

The first float64 slice should not compare:

- physical plans
- timing or performance
- local runtime, admission, or ownership events

### 4. Adapter Responsibilities

Shared differential docs own:

- case IDs and spec references
- input sets plus projection and filter intent references
- comparison modes and normalized comparison rules
- drift classification meanings

Adapters remain responsible for:

- engine-native query construction or expression execution plumbing
- session setup, planner hints, and connection details
- translation from engine-native output into the normalized comparison carrier
- translation from engine-native failures into normalized `error_class` values

This keeps adapters thin while letting the first float64 differential slice
target real engine behavior.

The minimal shared adapter request and response boundary for this slice lives
in `adapters/first-float64-ordering-slice.md`.

Issue #208 now adds executable adapter/harness wiring plus checked-in float64 `case-results` and `drift-report` artifacts for this slice.

Issue #286 adds the docs-first TiKV request and response boundary in
`adapters/first-float64-ordering-slice-tikv.md`.

## Follow-On Boundary

Later issues may extend this slice to cover:

- live differential runner coverage against real TiDB and TiFlash environments
- float arithmetic and coercion semantics
- SQL ordering and null-ordering policy checkpoints
- TiKV executable single-engine and pairwise coverage beyond the docs-first
  boundary in `adapters/first-float64-ordering-slice-tikv.md`

Until then, this checkpoint fixes only the first float64 differential
semantics, case IDs, adapter-boundary shape, comparison-mode behavior, and
normalized comparison rules.
