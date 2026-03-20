# First Differential Collation-Sensitive String Slice

Status: issue #233 design checkpoint

Related issues:

- #143 `docs: define initial collation scope and ownership boundary`
- #233 `design: define first string collation semantic slice`

## Question

Which cross-engine comparison should `tiforth` define first for a narrow
collation-sensitive `utf8` checkpoint that makes shared collation identifiers,
comparison intent, and ordering intent explicit?

## Inputs Considered

- `docs/design/first-collation-string-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-collation-string-slice.md`
- `tests/differential/README.md`
- issue #233

## First Slice Decision

### 1. Engines

The first collation-sensitive string differential slice compares `TiDB` and
`TiFlash`.

This keeps the collation checkpoint aligned with the existing first
differential engine pair while keeping adapter and harness expansion narrow.

### 2. Case Family

The first collation-sensitive string differential slice uses only families
fixed by `docs/design/first-collation-string-slice.md`:

- passthrough `column(index)` over `utf8`
- `is_not_null(column(index))` over `utf8`
- collation-tagged `collation_eq(lhs, rhs, collation_ref)` probes
- collation-tagged `collation_lt(lhs, rhs, collation_ref)` probes
- collation-tagged canonical ordering probes over `column(index)`

Include these cases first:

- UTF-8 column passthrough under `binary`
- UTF-8 column passthrough under `unicode_ci`
- UTF-8 predicate mixed keep/drop
- UTF-8 binary equality case-sensitive probe
- UTF-8 unicode_ci equality case-insensitive probe
- UTF-8 binary less-than probe
- UTF-8 unicode_ci ordering normalization probe
- missing column execution error
- unknown collation execution error
- unsupported collation type execution error

Defer these cases from the first collation differential checkpoint:

- locale-specific collation families beyond `binary` and `unicode_ci`
- string function families beyond the comparison and ordering probes above
- runtime and ownership traces (claim handoff, shrink, release, cancellation)
- memory-admission deny or spill-and-retry behavior
- non-TiDB/TiFlash engines

### 2a. Shared Slice ID

Use `first-collation-string-slice` as the stable `slice_id` in adapter
requests, normalized case results, and later drift reports for this
checkpoint.

### 2b. Shared Collation References

Use these stable `collation_ref` identifiers for this slice:

- `binary`: bytewise UTF-8 comparison and ordering intent
- `unicode_ci`: case-folded Unicode comparison and ordering intent
- `unknown-collation`: unsupported collation probe for error normalization

### 2c. Shared Input References

Use these stable `input_ref` identifiers for this slice:

- `first-utf8-basic`: one non-null `utf8` field `s` with rows `"Alpha"`,
  `"alpha"`, `"beta"`
- `first-utf8-nullable`: one nullable `utf8` field `s` with rows `"Alpha"`,
  SQL `null`, `"beta"`
- `first-utf8-ordering`: one non-null `utf8` field `s` with rows `"b"`, `"A"`,
  `"a"`, `"B"`
- `first-int32-basic`: one non-null `int32` field `x` with rows `1`, `2`, `3`
  for unsupported-collation-type coverage

### 2d. Shared Operation References

Use these stable references for this slice:

- `projection_ref = column-0`: passthrough `column(0)`
- `filter_ref = is-not-null-column-0`: `is_not_null(column(0))`
- `filter_ref = is-not-null-column-1`: `is_not_null(column(1))`
- `comparison_ref = eq-column-0-literal-alpha`:
  `collation_eq(column(0), "alpha", collation_ref)`
- `comparison_ref = lt-column-0-literal-beta`:
  `collation_lt(column(0), "beta", collation_ref)`
- `ordering_ref = order-by-column-0-asc`:
  canonical `order_by(column(0), collation_ref, asc)` probe

### 2e. Shared Case IDs

Use these stable `case_id` assignments for this first collation slice:

- `utf8-column-passthrough-binary`: `input_ref = first-utf8-basic`,
  `projection_ref = column-0`, `collation_ref = binary`
- `utf8-column-passthrough-unicode-ci`: `input_ref = first-utf8-basic`,
  `projection_ref = column-0`, `collation_ref = unicode_ci`
- `utf8-is-not-null-mixed-keep-drop`: `input_ref = first-utf8-nullable`,
  `filter_ref = is-not-null-column-0`, `collation_ref = binary`
- `utf8-binary-eq-literal-alpha`: `input_ref = first-utf8-basic`,
  `comparison_ref = eq-column-0-literal-alpha`, `collation_ref = binary`
- `utf8-unicode-ci-eq-literal-alpha`: `input_ref = first-utf8-basic`,
  `comparison_ref = eq-column-0-literal-alpha`, `collation_ref = unicode_ci`
- `utf8-binary-lt-literal-beta`: `input_ref = first-utf8-basic`,
  `comparison_ref = lt-column-0-literal-beta`, `collation_ref = binary`
- `utf8-unicode-ci-ordering-normalization`:
  `input_ref = first-utf8-ordering`, `ordering_ref = order-by-column-0-asc`,
  `collation_ref = unicode_ci`
- `utf8-missing-column-error`: `input_ref = first-utf8-basic`,
  `filter_ref = is-not-null-column-1`, `collation_ref = binary`
- `unknown-collation-error`: `input_ref = first-utf8-basic`,
  `comparison_ref = eq-column-0-literal-alpha`,
  `collation_ref = unknown-collation`
- `unsupported-collation-type-error`: `input_ref = first-int32-basic`,
  `comparison_ref = eq-column-0-literal-alpha`, `collation_ref = binary`

The adapter-facing request and response boundary for these identifiers is
defined in `adapters/first-collation-string-slice.md`.

### 2f. Shared Spec References

For this first collation differential slice, every shared request currently
uses this `spec_refs[]` set:

- `docs/design/first-collation-string-slice.md`
- `docs/spec/type-system.md`
- `tests/conformance/first-collation-string-slice.md`
- `tests/differential/first-collation-string-slice.md`

Executable adapters should copy these refs through unchanged into normalized
`case result` records so later drift artifacts can cite the same shared
semantic sources.

### 3. Comparison Rules

For the first collation-sensitive string slice, harness code compares:

- output field order and field names
- logical result types and nullability
- row count
- row values and SQL-null positions
- normalized outcome class: `rows` or `error`

For row outcomes:

- passthrough and ordering cases use UTF-8 strings plus carrier `null`
- comparison-probe cases use `boolean` row values (`true`, `false`, `null`)
- canonical ordering under `unicode_ci` uses folded keys first; ties are
  normalized using original UTF-8 byte sequence, then original row position

For this slice, the expected canonical ordering for
`input_ref = first-utf8-ordering` with `ordering_ref = order-by-column-0-asc`
and `collation_ref = unicode_ci` is:

- `"A"`, `"a"`, `"B"`, `"b"`

For error cases, the harness should match normalized `error_class` values, not
exact engine text.

The first collation slice should not compare:

- physical plans
- timing or performance
- local runtime, admission, or ownership events

### 4. Adapter Responsibilities

Shared differential docs own:

- case IDs and spec references
- input sets plus operation intent references
- collation identifier meanings
- normalized result and error comparison rules
- drift classification meanings

Adapters remain responsible for:

- engine-native query construction or expression execution plumbing
- session setup, planner hints, and connection details
- translation from engine-native output into the normalized comparison carrier
- translation from engine-native failures into normalized `error_class` values

This keeps adapters thin while letting the first collation differential slice
target real engine behavior.

The minimal shared adapter request and response boundary for this slice lives
in `adapters/first-collation-string-slice.md`.

## Follow-On Boundary

Later issues may extend this slice to cover:

- checked-in artifact carriers and live-runner wiring for this slice
- locale-specific collation families and capability normalization
- broader string/binary semantic families beyond this comparison boundary
- TiKV participation

Until then, this checkpoint fixes only the first collation-sensitive string
differential semantics, request IDs, adapter-boundary shape, and normalized
comparison rules.
