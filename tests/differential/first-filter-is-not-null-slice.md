# First Differential Filter Slice: `is_not_null(column(index))`

Status: issue #147 design checkpoint

Related issues:

- #137 `design: choose first post-gate kernel expansion boundary`
- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #147 `design: define first differential filter slice and adapter boundary for is_not_null`
- #153 `harness: execute first-filter-is-not-null differential artifacts for TiDB and TiFlash`
- #247 `design: define first TiKV differential filter adapter request/response surface`
- #249 `adapter: execute first-filter-is-not-null-slice through TiKV`

## Question

Which cross-engine comparison should `tiforth` define first for the post-gate
filter semantic boundary?

## Inputs Considered

- `docs/spec/first-filter-is-not-null.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-filter-is-not-null-slice.md`
- `docs/design/first-post-gate-kernel-boundary.md`
- `tests/differential/README.md`
- `adapters/first-expression-slice.md`
- `tests/differential/first-filter-is-not-null-slice-artifacts.md`
- issue #147
- issue #153

## First Slice Decision

### 1. Engines

The first differential filter slice compares `TiDB` and `TiFlash`.

This keeps the first filter checkpoint aligned with the existing first
differential engine pair while avoiding immediate multi-engine expansion.

### 2. Case Family

The first differential filter slice uses only the first filter predicate family
already fixed by `docs/spec/first-filter-is-not-null.md`:
`is_not_null(column(index))`.

Include these cases first:

- all rows kept
- all rows dropped
- mixed keep/drop with retained-row order and full-row passthrough checks
- missing column execution error
- unsupported predicate type execution error

Defer these cases from the first differential filter checkpoint:

- memory-admission deny or spill/retry behavior
- runtime and ownership traces (claim handoff, shrink, release, cancellation,
  ownership violations)
- predicate families beyond `is_not_null(column(index))`
- non-TiDB/TiFlash engines

The rule is simple: compare first-filter row-selection semantics, not local
runtime bookkeeping.

### 2a. Shared Slice ID

Use `first-filter-is-not-null-slice` as the stable `slice_id` in adapter
requests, normalized case results, and later drift reports for this checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for the first filter slice:

- `first-filter-is-not-null-int32-basic`: one non-null `int32` field `a` with
  rows `1`, `2`, `3`
- `first-filter-is-not-null-int32-all-null`: one nullable `int32` field `a`
  with rows `null`, `null`, `null`
- `first-filter-is-not-null-int32-mixed-two-column`: two fields, nullable
  `int32` `a` with rows `1`, `null`, `3`, `null` and non-null `int32` `b` with
  rows `10`, `20`, `30`, `40`
- `first-filter-is-not-null-utf8-basic`: one non-null `utf8` field `s` with
  rows `"x"`, `"y"`, `"z"`

### 2c. Shared Filter References

Use these stable `filter_ref` identifiers for the first filter slice:

- `is-not-null-column-0`: `is_not_null(column(0))`
- `is-not-null-column-1`: `is_not_null(column(1))`

### 2d. Shared Case IDs

Use these stable `case_id` assignments for the first filter slice:

- `all-rows-kept`: `input_ref = first-filter-is-not-null-int32-basic`,
  `filter_ref = is-not-null-column-0`
- `all-rows-dropped`:
  `input_ref = first-filter-is-not-null-int32-all-null`,
  `filter_ref = is-not-null-column-0`
- `mixed-keep-drop`:
  `input_ref = first-filter-is-not-null-int32-mixed-two-column`,
  `filter_ref = is-not-null-column-0`
- `missing-column-error`: `input_ref = first-filter-is-not-null-int32-basic`,
  `filter_ref = is-not-null-column-1`
- `unsupported-predicate-type-error`:
  `input_ref = first-filter-is-not-null-utf8-basic`,
  `filter_ref = is-not-null-column-0`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-filter-is-not-null-slice.md`.

### 2e. Shared Spec References

For this first differential filter slice, every shared request currently uses
the same `spec_refs[]` set:

- `docs/spec/first-filter-is-not-null.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-filter-is-not-null-slice.md`
- `tests/differential/first-filter-is-not-null-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first differential filter slice, future harness code should compare:

- output field order and field names
- logical result types and nullability
- row count
- row values and null positions
- normalized outcome class: `rows` or `error`

For error cases, the harness should match on normalized `error_class` values,
not exact engine text.

The first filter slice should not compare:

- physical plans
- timing or performance
- local runtime, admission, or ownership events

### 4. Adapter Responsibilities

Shared differential docs own:

- case IDs and spec references
- input sets and filter intent references
- normalized result and error comparison rules
- drift classification meanings

Adapters remain responsible for:

- engine-native query construction or expression execution plumbing
- session setup, planner hints, and connection details
- translation from engine-native output into the normalized comparison carrier
- translation from engine-native failures into the normalized `error_class`

This keeps adapters thin while letting the first filter differential slice
target real engine behavior.

The minimal shared adapter request and response boundary for this slice lives
in `adapters/first-filter-is-not-null-slice.md`.

## Follow-On Boundary

Later issues may extend this slice to cover:

- checked-in artifact refresh from live TiDB and TiFlash runner output when shared
  review environments are available
- broader predicate families and wider logical-type coverage
- TiKV pairwise participation beyond the current single-engine TiKV case-results checkpoint

Until then, this checkpoint fixes only the first differential filter semantics,
request IDs, adapter-boundary shape, and stable first artifact carriers.
