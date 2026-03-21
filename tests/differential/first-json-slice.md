# First Differential JSON Slice

Status: issue #224 design checkpoint, issue #272 artifact-carrier checkpoint, issue #356 executable differential checkpoint

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #224 `design: define first JSON semantic slice boundary`
- #272 `docs: define first-json-slice differential artifact carriers`
- #356 `harness: execute first-json-slice differential artifacts`

## Question

Which cross-engine comparison should `tiforth` define first for a narrow JSON
checkpoint that makes comparability and cast boundaries explicit without
claiming broad JSON function semantics?

## Inputs Considered

- `docs/design/first-json-semantic-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-json-slice.md`
- `tests/differential/README.md`
- issue #224

## First Slice Decision

### 1. Engines

The first JSON differential slice compares `TiDB` and `TiFlash`.

This keeps the JSON checkpoint aligned with the existing first differential
engine pair while keeping adapter and harness expansion narrow.

### 2. Case Family

The first JSON differential slice uses only families fixed by
`docs/design/first-json-semantic-slice.md`:

- passthrough `column(index)` over `json`
- `is_not_null(column(index))` over `json`
- boundary probes for unsupported JSON ordering comparison and unsupported JSON
  cast requests

Include these cases first:

- JSON column passthrough
- JSON nullable passthrough (including SQL `NULL` and JSON literal `null`)
- JSON object canonicalization
- JSON array-order preservation
- JSON predicate all kept
- JSON predicate all dropped
- JSON predicate mixed keep/drop
- missing column execution error
- unsupported JSON ordering comparison execution error
- unsupported JSON cast execution error

Defer these cases from the first JSON differential checkpoint:

- JSON path extraction and mutation
- JSON containment and existence operators
- successful explicit cast semantics (`utf8 -> json`, `json -> utf8`, ...)
- runtime and ownership traces (claim handoff, shrink, release, cancellation)
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-json-slice` as the stable `slice_id` in adapter requests,
normalized case results, and later drift reports for this checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-json-basic`: one non-null `json` field `j` with rows
  `{\"a\":1}`, `[1,2]`, `true`
- `first-json-nullable`: one nullable `json` field `j` with rows
  `{\"a\":1}`, SQL `null`, JSON literal `null`, `{\"b\":2,\"a\":1}`
- `first-json-all-null`: one nullable `json` field `j` with rows
  SQL `null`, SQL `null`, SQL `null`
- `first-json-array-order`: one non-null `json` field `j` with rows `[1,2]`,
  `[2,1]`
- `first-utf8-json-text`: one non-null `utf8` field `s` with row
  `{\"a\":1}` for unsupported-cast coverage

### 2c. Shared Operation References

Use these stable references for this slice:

- `projection_ref = column-0`: passthrough `column(0)`
- `filter_ref = is-not-null-column-0`: `is_not_null(column(0))`
- `filter_ref = is-not-null-column-1`: `is_not_null(column(1))`
- `comparison_ref = less-than-column-0-column-0`: ordered comparison probe for
  unsupported JSON comparison semantics
- `cast_ref = json-column-0-to-int32`: explicit cast probe for unsupported
  `json -> int32`
- `cast_ref = utf8-column-0-to-json`: explicit cast probe for unsupported
  `utf8 -> json`

### 2d. Shared Case IDs

Use these stable `case_id` assignments for this first JSON slice:

- `json-column-passthrough`:
  `input_ref = first-json-basic`, `projection_ref = column-0`
- `json-column-null-preserve`:
  `input_ref = first-json-nullable`, `projection_ref = column-0`
- `json-object-canonicalization`:
  `input_ref = first-json-nullable`, `projection_ref = column-0`
- `json-array-order-preserved`:
  `input_ref = first-json-array-order`, `projection_ref = column-0`
- `json-is-not-null-all-kept`:
  `input_ref = first-json-basic`, `filter_ref = is-not-null-column-0`
- `json-is-not-null-all-dropped`:
  `input_ref = first-json-all-null`, `filter_ref = is-not-null-column-0`
- `json-is-not-null-mixed-keep-drop`:
  `input_ref = first-json-nullable`, `filter_ref = is-not-null-column-0`
- `json-missing-column-error`:
  `input_ref = first-json-basic`, `filter_ref = is-not-null-column-1`
- `unsupported-json-ordering-comparison-error`:
  `input_ref = first-json-basic`,
  `comparison_ref = less-than-column-0-column-0`
- `unsupported-json-cast-error`:
  `input_ref = first-json-basic`, `cast_ref = json-column-0-to-int32`
- `unsupported-cast-to-json-error`:
  `input_ref = first-utf8-json-text`, `cast_ref = utf8-column-0-to-json`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-json-slice.md`.

### 2e. Shared Spec References

For this first JSON differential slice, every shared request currently uses this
`spec_refs[]` set:

- `docs/design/first-json-semantic-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-json-slice.md`
- `tests/differential/first-json-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first JSON slice, harness code compares:

- output field order and field names
- logical result types and nullability
- row count
- row values and SQL-null positions
- normalized outcome class: `rows` or `error`

For JSON row outcomes, shared comparison uses canonical JSON value tokens in
`rows[]`:

- each non-null JSON value is one canonical JSON text string
- canonical JSON text is minified with deterministic object-key ordering
- array element order stays unchanged and remains significant
- SQL `NULL` is represented as JSON `null` in the outer `rows[]` carrier,
  while JSON literal `null` is represented as the JSON string `"null"`

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

The first JSON slice should not compare:

- physical plans
- timing or performance
- local runtime, admission, or ownership events

### 4. Adapter Responsibilities

Shared differential docs own:

- case IDs and spec references
- input sets plus operation intent references
- normalized result and error comparison rules
- drift classification meanings

Adapters remain responsible for:

- engine-native query construction or expression execution plumbing
- session setup, planner hints, and connection details
- translation from engine-native output into the normalized comparison carrier
- translation from engine-native failures into normalized `error_class` values

This keeps adapters thin while letting the first JSON differential slice target
real engine behavior.

The minimal shared adapter request and response boundary for this slice lives
in `adapters/first-json-slice.md`.

## Follow-On Boundary

Issue #356 now executes this slice through `crates/tiforth-adapter-tidb/src/first_json_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_json_slice.rs`, and `crates/tiforth-harness-differential/src/first_json_slice.rs`, with checked-in artifacts under `inventory/first-json-slice-*`.

Later issues may extend this slice to cover:

- successful JSON cast semantics and parse-error normalization beyond the current unsupported-cast boundary
- JSON comparison and operator families beyond this unsupported-ordering boundary
- TiKV participation

This checkpoint now fixes the first JSON differential semantics, request IDs, adapter-boundary shape, normalized comparison rules, and unsupported-ordering or cast error boundaries.
