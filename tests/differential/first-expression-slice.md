# First Differential Expression Slice

Status: issue #68 design checkpoint

Related issues:

- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #68 `design: define first differential expression slice and drift report format`

## Question

Which cross-engine comparison should `tiforth` define first now that the local milestone-1 expression and projection slice exists?

## Inputs Considered

- `README.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/spec/milestone-1-expression-projection.md`
- `tests/conformance/expression-projection-slice.md`
- `inventory/README.md`
- `adapters/tidb/README.md`
- `adapters/tiflash/README.md`
- `adapters/tikv/README.md`
- issue #68

## First Slice Decision

### 1. Engines

The first differential slice compares `TiDB` and `TiFlash`.

This starts with one engine pair rather than all three adapters.

Why this pair comes first:

- the current milestone-1 semantic surface is projection plus simple expression evaluation over one tabular input
- that slice can be compared between two SQL-facing engines without first inventing a lower-level storage-only harness shape
- two-engine comparison is enough to surface the first semantic drift while keeping result normalization and triage small
- `TiKV` remains in scope for later differential work, but it should join after the first normalized result and drift carrier are settled

### 2. Case Family

The first differential slice reuses the shared semantic core from `tests/conformance/expression-projection-slice.md`.

Include these cases first:

- `column passthrough`
- `direct literal<int32>` for both non-null and `NULL`
- `add literal<int32>`
- `null propagation` for `add<int32>`
- `overflow error` for `add<int32>`

Defer these cases from the first differential checkpoint:

- local admission outcomes such as `reserve-first deny`
- runtime and ownership cases such as claim handoff, shrink, release, cancellation, and ownership violations
- duplicate or mixed-claim runtime coverage
- schema-binding and type-rejection cases such as `missing column` and `unsupported arithmetic type` until the first error-normalization pass is proven on the overflow case

The rule is simple: the first differential slice compares shared expression semantics, not `tiforth`-local runtime bookkeeping.

### 3. Comparison Rules

For the first slice, future differential harness code should compare:

- output field order and field names
- logical result types and nullability
- row count
- row values and null positions
- normalized outcome class: `rows` or `error`

For error cases, the harness should match on a normalized `error_class` rather than exact engine text. Exact engine messages, codes, and planner details remain evidence, not the match key.

The first slice should not compare:

- physical plans
- timing or performance
- `tiforth` local runtime or admission events
- claim counts or ownership traces

### 4. Adapter Responsibilities

Shared differential docs own:

- case IDs and spec references
- input data sets and projection intent
- normalized result and error comparison rules
- drift classification meanings

Adapters remain responsible for:

- engine-native query construction or expression execution plumbing
- session setup, planner hints, and connection details
- translation from engine-native output into the normalized comparison carrier
- translation from engine-native failure output into the chosen `error_class`

This keeps adapters thin while letting the first differential slice target real engine behavior.

## Follow-On Boundary

A later implementation issue may add executable harness code once it can emit the minimum artifact set defined in `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md`.
