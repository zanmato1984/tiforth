# First Differential Map Slice

Status: issue #230 design checkpoint

Related issues:

- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #151 `design: define first nested-family shared handoff slice and claim-ownership boundary`
- #226 `design: define first struct nested handoff slice checkpoint`
- #230 `docs: define first map nested handoff slice checkpoint`

## Question

Which cross-engine comparison should `tiforth` define first for a narrow
`map` checkpoint that extends nested handoff coverage beyond existing
`list<int32>` and `struct<a:int32, b:int32?>` passthrough boundaries?

## Inputs Considered

- `docs/design/first-map-aware-handoff-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-map-slice.md`
- `tests/differential/README.md`
- issue #230

## First Slice Decision

### 1. Engines

The first map differential slice compares `TiDB` and `TiFlash`.

This keeps the map checkpoint aligned with existing first differential engine
pairs while keeping adapter and harness expansion narrow.

### 2. Case Family

The first map differential slice uses only the map passthrough family fixed by
`docs/design/first-map-aware-handoff-slice.md`:

- passthrough `column(index)` over `map<int32, int32?>`

Include these cases first:

- map column passthrough
- map nullable passthrough
- map value-null preservation
- missing column execution error
- unsupported nested family execution error

Defer these cases from the first map differential checkpoint:

- nested predicate behavior (`is_not_null(column(index))`) over `map`
- nested compute behavior beyond passthrough `column(index)`
- nested families beyond this checkpoint (`union`, nested combinations)
- runtime and ownership traces (claim handoff, shrink, release, cancellation)
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-map-slice` as the stable `slice_id` in adapter requests,
normalized case results, and later drift reports for this checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-map-basic`: one non-null `map<int32, int32?>` field `m` with rows
  `[{key: 1, value: 2}, {key: 3, value: 4}]`, `[{key: 5, value: 6}]`, and `[]`
- `first-map-nullable`: one nullable `map<int32, int32?>` field `m` with rows
  `[{key: 1, value: null}]`, `null`, and `[{key: 2, value: 3}]`
- `first-union-basic`: one `union` field `u` used to assert unsupported
  nested-family errors in this first map slice

### 2c. Shared Projection References

Use these stable references for this slice:

- `projection_ref = column-0`: passthrough `column(0)`
- `projection_ref = column-1`: out-of-range `column(1)` probe for
  missing-column behavior

### 2d. Shared Case IDs

Use these stable `case_id` assignments for this first map slice:

- `map-column-passthrough`:
  `input_ref = first-map-basic`, `projection_ref = column-0`
- `map-column-null-preserve`:
  `input_ref = first-map-nullable`, `projection_ref = column-0`
- `map-value-null-preserve`:
  `input_ref = first-map-nullable`, `projection_ref = column-0`
- `map-missing-column-error`:
  `input_ref = first-map-basic`, `projection_ref = column-1`
- `unsupported-nested-family-error`:
  `input_ref = first-union-basic`, `projection_ref = column-0`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-map-slice.md`.

### 2e. Shared Spec References

For this first map differential slice, every shared request currently uses this
`spec_refs[]` set:

- `docs/design/first-map-aware-handoff-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-map-slice.md`
- `tests/differential/first-map-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first map slice, harness code compares:

- output field order and field names
- logical result types and nullability
- row count
- row values and null placement for top-level map rows and per-entry values
- normalized outcome class: `rows` or `error`

For map row outcomes, shared comparison uses canonical carriers in `rows[]`:

- each non-null map value is one canonical JSON array of entry objects with
  stable keys (`key`, then `value`)
- entry keys use JSON numbers and must not be `null`
- entry values use JSON numbers or `null`
- SQL `NULL` map rows are represented as carrier `null`

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

The first map slice should not compare:

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

This keeps adapters thin while letting the first map differential slice target
real engine behavior.

The minimal shared adapter request and response boundary for this slice lives
in `adapters/first-map-slice.md`.

## Follow-On Boundary

Later issues may extend this slice to cover:

- nested predicate and compute behavior beyond passthrough `column(index)`
- broader nested-family logical types (`union`, nested combinations)
- checked-in artifact carriers and live-runner wiring for this slice
- TiKV participation

Until then, this checkpoint fixes only the first map differential semantics,
request IDs, adapter-boundary shape, and normalized comparison rules.
