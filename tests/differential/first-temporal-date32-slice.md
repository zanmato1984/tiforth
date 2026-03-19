# First Differential Temporal `date32` Slice

Status: issue #176 design checkpoint, issue #185 artifact-carrier checkpoint

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #185 `docs: define first temporal date32 differential artifact carriers`

## Question

Which cross-engine comparison should `tiforth` define first for the narrow
`date32` temporal checkpoint fixed by `docs/design/first-temporal-semantic-slice.md`?

## Inputs Considered

- `docs/design/first-temporal-semantic-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-temporal-date32-slice.md`
- `tests/differential/README.md`
- issue #176

## First Slice Decision

### 1. Engines

The first temporal differential slice compares `TiDB` and `TiFlash`.

This keeps the temporal checkpoint aligned with the existing first differential
engine pair while keeping adapter and harness expansion narrow.

### 2. Case Family

The first temporal differential slice uses the first temporal families already
fixed by `docs/design/first-temporal-semantic-slice.md`:

- passthrough `column(index)` over `date32`
- `is_not_null(column(index))` over `date32`

Include these cases first:

- date32 column passthrough
- date32 column null preservation
- date32 predicate all kept
- date32 predicate all dropped
- date32 predicate mixed keep/drop
- missing column execution error
- unsupported temporal family execution error

Defer these cases from the first temporal differential checkpoint:

- timezone-aware timestamp normalization or ordering
- temporal arithmetic, cast, extract, or truncation families
- runtime and ownership traces (claim handoff, shrink, release, cancellation)
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-temporal-date32-slice` as the stable `slice_id` in adapter requests,
normalized case results, and later drift reports for this checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-temporal-date32-basic`: one non-null `date32` field `d` with day-domain
  rows `0`, `1`, `2`
- `first-temporal-date32-nullable`: one nullable `date32` field `d` with
  day-domain rows `0`, `null`, `2`, `null`
- `first-temporal-date32-all-null`: one nullable `date32` field `d` with rows
  `null`, `null`, `null`
- `first-temporal-timestamp-basic`: one non-null `timestamp` field `ts` used to
  assert unsupported-temporal-family errors in this first temporal slice

### 2c. Shared Projection And Filter References

Use these stable references for this slice:

- `projection_ref = column-0`: passthrough `column(0)`
- `filter_ref = is-not-null-column-0`: `is_not_null(column(0))`
- `filter_ref = is-not-null-column-1`: `is_not_null(column(1))`

### 2d. Shared Case IDs

Use these stable `case_id` assignments for this first temporal slice:

- `date32-column-passthrough`:
  `input_ref = first-temporal-date32-basic`, `projection_ref = column-0`
- `date32-column-null-preserve`:
  `input_ref = first-temporal-date32-nullable`, `projection_ref = column-0`
- `date32-is-not-null-all-kept`:
  `input_ref = first-temporal-date32-basic`,
  `filter_ref = is-not-null-column-0`
- `date32-is-not-null-all-dropped`:
  `input_ref = first-temporal-date32-all-null`,
  `filter_ref = is-not-null-column-0`
- `date32-is-not-null-mixed-keep-drop`:
  `input_ref = first-temporal-date32-nullable`,
  `filter_ref = is-not-null-column-0`
- `date32-missing-column-error`:
  `input_ref = first-temporal-date32-basic`,
  `filter_ref = is-not-null-column-1`
- `unsupported-temporal-type-error`:
  `input_ref = first-temporal-timestamp-basic`, `projection_ref = column-0`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-temporal-date32-slice.md`.

### 2e. Shared Spec References

For this first temporal differential slice, every shared request currently uses
this `spec_refs[]` set:

- `docs/design/first-temporal-semantic-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-temporal-date32-slice.md`
- `tests/differential/first-temporal-date32-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first temporal slice, future harness code should compare:

- output field order and field names
- logical result types and nullability
- row count
- row values and null positions
- normalized outcome class: `rows` or `error`

For `date32` row outcomes, shared comparison should use normalized day-domain
integer values rather than engine-specific formatted date strings.

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

The first temporal slice should not compare:

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

This keeps adapters thin while letting the first temporal differential slice
target real engine behavior.

The minimal shared adapter request and response boundary for this slice lives
in `adapters/first-temporal-date32-slice.md`.

## Follow-On Boundary

Later issues may extend this slice to cover:

- checked-in inventory refresh from live runner output against the carrier
  contract in `tests/differential/first-temporal-date32-slice-artifacts.md`
- broader temporal logical families and timezone-sensitive behavior
- temporal arithmetic, cast, extract, or truncation semantics
- TiKV participation

Until then, this checkpoint fixes only the first temporal differential
semantics, request IDs, adapter-boundary shape, and normalized comparison
rules.
