# First Differential Temporal `timestamp_tz(us)` Slice

Status: issue #280 design checkpoint, issue #290 TiKV boundary checkpoint, issue #298 artifact-carrier checkpoint, issue #304 harness checkpoint

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #178 `milestone-1: implement first executable temporal date32 slice in local kernel`
- #280 `design: define first timezone-aware timestamp semantic slice checkpoint`
- #290 `design: define TiKV adapter boundary for first-temporal-timestamp-tz-slice`
- #298 `docs: define first-temporal-timestamp-tz differential artifact carriers`
- #304 `harness: execute first timestamp_tz(us) differential artifacts`

## Question

Which cross-engine comparison should `tiforth` define first for a narrow
timezone-aware timestamp checkpoint that fixes normalization and ordering intent
without claiming broad temporal-family behavior?

## Inputs Considered

- `docs/design/first-temporal-timestamp-tz-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-temporal-timestamp-tz-slice.md`
- `tests/differential/README.md`
- `adapters/first-temporal-timestamp-tz-slice-tikv.md`
- `tests/differential/first-temporal-timestamp-tz-slice-artifacts.md`
- issue #280
- issue #290
- issue #298
- issue #304

## First Slice Decision

### 1. Engines

The first timezone-aware timestamp differential slice compares `TiDB` and
`TiFlash`.

This keeps the timestamp checkpoint aligned with the existing first
cross-engine differential pair while keeping adapter and harness expansion
narrow.

### 2. Case Family

The first timestamp-timezone differential slice uses only families fixed by
`docs/design/first-temporal-timestamp-tz-slice.md`:

- passthrough `column(index)` over `timestamp_tz(us)`
- `is_not_null(column(index))` over `timestamp_tz(us)`
- ordering probe `order-by(column(index), asc, nulls_last)` over
  `timestamp_tz(us)`
- boundary probes for unsupported timestamp-without-timezone and unsupported
  timestamp-unit requests

Include these cases first:

- timestamp-timezone column passthrough
- timestamp-timezone equivalent-instant normalization
- timestamp-timezone nullable passthrough
- timestamp-timezone predicate all kept
- timestamp-timezone predicate all dropped
- timestamp-timezone predicate mixed keep/drop
- timestamp-timezone ordering ascending with nulls last
- missing column execution error
- unsupported timestamp-without-timezone execution error
- unsupported timestamp-unit execution error

Defer these cases from the first timestamp-timezone differential checkpoint:

- temporal arithmetic, cast, extract, truncation, and interval families
- timezone-name canonicalization and timezone-database negotiation
- runtime and ownership traces (claim handoff, shrink, release, cancellation)
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-temporal-timestamp-tz-slice` as the stable `slice_id` in adapter
requests, normalized case results, and later drift reports for this checkpoint.

### 2b. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-temporal-timestamp-tz-basic`: one non-null `timestamp_tz(us)` field
  `ts` with rows `1970-01-01T00:00:00+00:00`, `1970-01-01T00:00:01+00:00`,
  `1970-01-01T00:00:02+00:00`
- `first-temporal-timestamp-tz-equivalent-instants`: one non-null
  `timestamp_tz(us)` field `ts` with rows `2024-01-01T00:00:00+00:00`,
  `2023-12-31T19:00:00-05:00`, `2024-01-01T09:00:00+09:00`
- `first-temporal-timestamp-tz-nullable`: one nullable `timestamp_tz(us)`
  field `ts` with rows `1970-01-01T00:00:00+00:00`, `null`,
  `1970-01-01T00:00:02+00:00`, `null`
- `first-temporal-timestamp-tz-all-null`: one nullable `timestamp_tz(us)`
  field `ts` with rows `null`, `null`, `null`
- `first-temporal-timestamp-naive-basic`: one non-null `timestamp`
  (no-timezone) field `ts` used to assert unsupported-temporal-type errors
- `first-temporal-timestamp-tz-ms-basic`: one non-null `timestamp_tz(ms)`
  field `ts` used to assert unsupported-temporal-unit errors

### 2c. Shared Operation References

Use these stable references for this slice:

- `projection_ref = column-0`: passthrough `column(0)`
- `filter_ref = is-not-null-column-0`: `is_not_null(column(0))`
- `filter_ref = is-not-null-column-1`: `is_not_null(column(1))`
- `ordering_ref = order-by-column-0-asc-nulls-last`:
  ordering probe over `column(0)`

### 2d. Shared Case IDs

Use these stable `case_id` assignments for this first timestamp-timezone slice:

- `timestamp-tz-column-passthrough`:
  `input_ref = first-temporal-timestamp-tz-basic`,
  `projection_ref = column-0`
- `timestamp-tz-equivalent-instant-normalization`:
  `input_ref = first-temporal-timestamp-tz-equivalent-instants`,
  `projection_ref = column-0`
- `timestamp-tz-column-null-preserve`:
  `input_ref = first-temporal-timestamp-tz-nullable`,
  `projection_ref = column-0`
- `timestamp-tz-is-not-null-all-kept`:
  `input_ref = first-temporal-timestamp-tz-basic`,
  `filter_ref = is-not-null-column-0`
- `timestamp-tz-is-not-null-all-dropped`:
  `input_ref = first-temporal-timestamp-tz-all-null`,
  `filter_ref = is-not-null-column-0`
- `timestamp-tz-is-not-null-mixed-keep-drop`:
  `input_ref = first-temporal-timestamp-tz-nullable`,
  `filter_ref = is-not-null-column-0`
- `timestamp-tz-order-asc-nulls-last`:
  `input_ref = first-temporal-timestamp-tz-nullable`,
  `ordering_ref = order-by-column-0-asc-nulls-last`
- `timestamp-tz-missing-column-error`:
  `input_ref = first-temporal-timestamp-tz-basic`,
  `filter_ref = is-not-null-column-1`
- `unsupported-temporal-timestamp-without-timezone-error`:
  `input_ref = first-temporal-timestamp-naive-basic`,
  `projection_ref = column-0`
- `unsupported-temporal-timestamp-unit-error`:
  `input_ref = first-temporal-timestamp-tz-ms-basic`,
  `projection_ref = column-0`

The adapter-facing request and response boundary for TiDB and TiFlash
identifiers is defined in `adapters/first-temporal-timestamp-tz-slice.md`.

The TiKV request and response boundary for this same slice is defined in
`adapters/first-temporal-timestamp-tz-slice-tikv.md`.

### 2e. Shared Spec References

For this first timestamp-timezone differential slice, every shared request
currently uses this `spec_refs[]` set:

- `docs/design/first-temporal-timestamp-tz-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-temporal-timestamp-tz-slice.md`
- `tests/differential/first-temporal-timestamp-tz-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first timestamp-timezone slice, harness code compares:

- output field order and field names
- logical result types and nullability
- row count
- row values and null positions
- normalized outcome class: `rows` or `error`

For `timestamp_tz(us)` row outcomes, shared comparison uses normalized UTC
epoch-microsecond integers in `rows[]` so equivalent instants compare equal
regardless of source offset syntax.

For ordering-probe cases, shared comparison verifies ascending normalized
UTC-instant order with null rows last.

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

The first timestamp-timezone slice should not compare:

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

This keeps adapters thin while letting the first timestamp-timezone
differential slice target real engine behavior.

The minimal shared adapter request and response boundary for this slice lives
in `adapters/first-temporal-timestamp-tz-slice.md`.

The TiKV request and response boundary for this same slice is defined in
`adapters/first-temporal-timestamp-tz-slice-tikv.md`.

## Follow-On Boundary

Later issues may extend this slice to cover:

- checked-in inventory artifact carriers and refresh workflow for this slice
- additional timestamp units, timestamp-without-timezone semantics, and
  timezone-name canonicalization
- temporal arithmetic, cast, extract, truncation, and interval semantics
- executable TiKV single-engine and pairwise checkpoints on top of the
  docs-first `first-temporal-timestamp-tz-slice` TiKV adapter boundary

Until then, this checkpoint fixes only the first timezone-aware timestamp
semantics, request IDs, adapter-boundary shape, and normalized comparison
rules.
