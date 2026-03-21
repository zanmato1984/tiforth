# First Differential Struct Slice

Status: issue #226 design checkpoint, issue #331 artifact-carrier checkpoint

Related issues:

- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #151 `design: define first nested-family shared handoff slice and claim-ownership boundary`
- #226 `design: define first struct nested handoff slice checkpoint`
- #331 `docs: define first-struct-slice differential artifact carriers`

## Question

Which cross-engine comparison should `tiforth` define first for a narrow
`struct` checkpoint that extends nested handoff coverage beyond the existing
`list<int32>` passthrough boundary?

## Inputs Considered

- `docs/design/first-struct-aware-handoff-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-struct-slice.md`
- `tests/differential/README.md`
- `tests/differential/first-struct-slice-artifacts.md`
- issue #226
- issue #331

## First Slice Decision

### 1. Engines

The first struct differential slice compares `TiDB` and `TiFlash`.

This keeps the struct checkpoint aligned with the existing first differential
engine pair while keeping adapter and harness expansion narrow.

### 2. Case Family

The first struct differential slice uses only the struct passthrough family
fixed by `docs/design/first-struct-aware-handoff-slice.md`:

- passthrough `column(index)` over `struct<a:int32, b:int32?>`

Include these cases first:

- struct column passthrough
- struct nullable passthrough
- struct child-null preservation
- missing column execution error
- unsupported nested family execution error

Defer these cases from the first struct differential checkpoint:

- nested predicate behavior (`is_not_null(column(index))`) over `struct`
- nested compute behavior beyond passthrough `column(index)`
- nested families beyond this checkpoint (`map`, `union`, nested combinations)
- runtime and ownership traces (claim handoff, shrink, release, cancellation)
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-struct-slice` as the stable `slice_id` in adapter requests,
normalized case results, and later drift reports for this checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-struct-basic`: one non-null `struct<a:int32, b:int32?>` field `s`
  with rows `{a: 1, b: 2}`, `{a: 3, b: 4}`, `{a: 5, b: 6}`
- `first-struct-nullable`: one nullable `struct<a:int32, b:int32?>` field `s`
  with rows `{a: 1, b: null}`, `null`, `{a: 2, b: 3}`
- `first-map-basic`: one non-null `map<int32, int32>` field `m` used to assert
  unsupported nested-family errors in this first struct slice

### 2c. Shared Projection References

Use these stable references for this slice:

- `projection_ref = column-0`: passthrough `column(0)`
- `projection_ref = column-1`: out-of-range `column(1)` probe for
  missing-column behavior

### 2d. Shared Case IDs

Use these stable `case_id` assignments for this first struct slice:

- `struct-column-passthrough`:
  `input_ref = first-struct-basic`, `projection_ref = column-0`
- `struct-column-null-preserve`:
  `input_ref = first-struct-nullable`, `projection_ref = column-0`
- `struct-child-null-preserve`:
  `input_ref = first-struct-nullable`, `projection_ref = column-0`
- `struct-missing-column-error`:
  `input_ref = first-struct-basic`, `projection_ref = column-1`
- `unsupported-nested-family-error`:
  `input_ref = first-map-basic`, `projection_ref = column-0`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-struct-slice.md`.

### 2e. Shared Spec References

For this first struct differential slice, every shared request currently uses
this `spec_refs[]` set:

- `docs/design/first-struct-aware-handoff-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-struct-slice.md`
- `tests/differential/first-struct-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first struct slice, harness code compares:

- output field order and field names
- logical result types and nullability
- row count
- row values and null placement for top-level struct values and child fields
- normalized outcome class: `rows` or `error`

For `struct` row outcomes, shared comparison uses canonical object carriers in
`rows[]`:

- each non-null struct value is one canonical JSON object whose key order
  matches shared struct field order (`a`, then `b`)
- field values use JSON numbers or `null`
- SQL `NULL` struct rows are represented as carrier `null`

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

The first struct slice should not compare:

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

This keeps adapters thin while letting the first struct differential slice
target real engine behavior.

The minimal shared adapter request and response boundary for this slice lives
in `adapters/first-struct-slice.md`.

Stable artifact-carrier fields and planned checked-in filenames for this slice
now live in `tests/differential/first-struct-slice-artifacts.md`.

## Follow-On Boundary

Later issues may extend this slice to cover:

- executable adapter and harness wiring for this slice
- checked-in inventory refresh against the carrier contract in
  `tests/differential/first-struct-slice-artifacts.md`
- nested predicate and compute behavior beyond passthrough `column(index)`
- broader nested-family logical types (`map`, `union`, nested combinations)
- TiKV participation

Until then, this checkpoint fixes only the first struct differential
semantics, request IDs, adapter-boundary shape, normalized comparison rules,
and planned artifact-carrier contract for the TiDB-versus-TiFlash checkpoint.
