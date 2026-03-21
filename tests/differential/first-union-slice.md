# First Differential Union Slice

Status: issue #241 design checkpoint, issue #340 artifact-carrier checkpoint, issue #366 executable differential checkpoint

Related issues:

- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #151 `design: define first nested-family shared handoff slice and claim-ownership boundary`
- #226 `design: define first struct nested handoff slice checkpoint`
- #230 `docs: define first map nested handoff slice checkpoint`
- #241 `docs: define first union nested handoff slice checkpoint`
- #340 `docs: define first-union-slice differential artifact carriers`
- #366 `harness: execute first-union-slice differential artifacts`

## Question

Which cross-engine comparison should `tiforth` define first for a narrow
`union` checkpoint that extends nested handoff coverage beyond existing
`list<int32>`, `struct<a:int32, b:int32?>`, and `map<int32, int32?>`
passthrough boundaries?

## Inputs Considered

- `docs/design/first-union-aware-handoff-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-union-slice.md`
- `tests/differential/first-union-slice-artifacts.md`
- `tests/differential/README.md`
- issue #241
- issue #340
- issue #366

## First Slice Decision

### 1. Engines

The first union differential slice compares `TiDB` and `TiFlash`.

This keeps the union checkpoint aligned with existing first differential engine
pairs while keeping adapter and harness expansion narrow.

### 2. Case Family

The first union differential slice uses only the union passthrough family fixed
by `docs/design/first-union-aware-handoff-slice.md`:

- passthrough `column(index)` over `dense_union<i:int32, n:int32?>`

Include these cases first:

- union column passthrough
- union variant-switch preservation
- union variant-null preservation
- missing column execution error
- unsupported nested family execution error

Defer these cases from the first union differential checkpoint:

- nested predicate behavior (`is_not_null(column(index))`) over `union`
- nested compute behavior beyond passthrough `column(index)`
- broader union modes (`sparse_union`) and wider nested combinations
- runtime and ownership traces (claim handoff, shrink, release, cancellation)
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-union-slice` as the stable `slice_id` in adapter requests,
normalized case results, and later drift reports for this checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-union-basic`: one non-null `dense_union<i:int32, n:int32?>` field `u`
  with rows `{tag: i, value: 1}`, `{tag: n, value: 2}`, and
  `{tag: i, value: 3}`
- `first-union-nullable-variant`: one non-null
  `dense_union<i:int32, n:int32?>` field `u` with rows
  `{tag: n, value: null}`, `{tag: i, value: 4}`, and `{tag: n, value: 5}`
- `first-map-basic`: one non-null `map<int32, int32>` field `m` used to assert
  unsupported nested-family errors in this first union slice

### 2c. Shared Projection References

Use these stable references for this slice:

- `projection_ref = column-0`: passthrough `column(0)`
- `projection_ref = column-1`: out-of-range `column(1)` probe for
  missing-column behavior

### 2d. Shared Case IDs

Use these stable `case_id` assignments for this first union slice:

- `union-column-passthrough`:
  `input_ref = first-union-basic`, `projection_ref = column-0`
- `union-variant-switch-preserve`:
  `input_ref = first-union-basic`, `projection_ref = column-0`
- `union-variant-null-preserve`:
  `input_ref = first-union-nullable-variant`, `projection_ref = column-0`
- `union-missing-column-error`:
  `input_ref = first-union-basic`, `projection_ref = column-1`
- `unsupported-nested-family-error`:
  `input_ref = first-map-basic`, `projection_ref = column-0`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-union-slice.md`.

### 2e. Shared Spec References

For this first union differential slice, every shared request currently uses
this `spec_refs[]` set:

- `docs/design/first-union-aware-handoff-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-union-slice.md`
- `tests/differential/first-union-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first union slice, harness code compares:

- output field order and field names
- logical result types and nullability
- row count
- row values, variant tags, and per-variant payload preservation
- normalized outcome class: `rows` or `error`

For union row outcomes, shared comparison uses canonical carriers in `rows[]`:

- each union value is one canonical JSON object with stable keys (`tag`, then
  `value`)
- `tag` values use stable variant names (`i` or `n`)
- `value` uses JSON numbers or `null` according to the selected variant payload
- this first slice does not require top-level nullable union rows; if a
  follow-on slice adds them, explicit carrier `null` semantics should be added
  there

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

The first union slice should not compare:

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

This keeps adapters thin while letting the first union differential slice
target real engine behavior.

The minimal shared adapter request and response boundary for this slice lives
in `adapters/first-union-slice.md`.

Stable artifact-carrier fields and checked-in filenames for this slice
now live in `tests/differential/first-union-slice-artifacts.md`.

## Follow-On Boundary

Later issues may extend this slice to cover:

- nested predicate and compute behavior beyond passthrough `column(index)`
- broader nested-family logical types (`sparse_union`, wider unions, and nested
  combinations)
- TiKV participation

Until then, this checkpoint fixes the first union differential semantics, request IDs, adapter-boundary shape, normalized comparison rules, and checked-in artifact-carrier evidence for the TiDB-versus-TiFlash checkpoint.
