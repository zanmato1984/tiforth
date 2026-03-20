# First Differential Filter Adapter Boundary

Status: issue #147 design checkpoint

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #147 `design: define first differential filter slice and adapter boundary for is_not_null`
- #153 `harness: execute first-filter-is-not-null differential artifacts for TiDB and TiFlash`
- #247 `design: define first TiKV differential filter adapter request/response surface`

## Purpose

This note defines the minimal shared boundary between the first differential
filter slice and the engine-specific adapters that execute it.

The goal is to give future harness work one explicit contract between:

- the shared differential filter slice in
  `tests/differential/first-filter-is-not-null-slice.md`
- the stable first-filter artifact carriers in
  `tests/differential/first-filter-is-not-null-slice-artifacts.md`
- the existing drift-report carrier guidance in
  `tests/differential/drift-report-carrier.md`
- TiDB and TiFlash execution plumbing that should stay adapter-local

## Scope

This boundary applies only to the first differential filter slice:

- `slice_id = first-filter-is-not-null-slice`
- engines: `TiDB` and `TiFlash`
- case family:
  `is_not_null(column(index))` cases documented in
  `tests/differential/first-filter-is-not-null-slice.md`

It does **not** yet define:

- TiKV participation in this shared TiDB-versus-TiFlash pairwise checkpoint;
  TiKV-specific request and response boundary details are handled separately in
  `adapters/first-filter-is-not-null-slice-tikv.md`
- generalized predicate execution beyond `is_not_null(column(index))`
- connection management, authentication, or environment provisioning
- a generalized adapter API for later differential families
- engine-plan capture, timing, or runtime-event export

## Shared Ownership

Shared differential docs own:

- `slice_id`, `case_id`, `input_ref`, and `filter_ref`
- semantic meaning for each case
- normalized comparison rules and drift classification meanings
- minimum normalized `case result` fields for this slice

Adapters own:

- engine-native SQL or expression construction
- session setup, planner hints, and connection details
- execution against the target engine
- translation from engine-native rows or failures into the shared normalized
  carrier
- runtime orchestration, timeout or retry policy, cancellation transport, and
  diagnostics capture as bounded by
  `docs/design/adapter-runtime-orchestration-boundary.md`

This keeps shared docs focused on semantics while adapters stay thin and
execution-oriented.

## Request Surface

The first executable differential filter harness should submit one documented
case at a time to one engine-specific adapter.

The shared request surface is:

- `slice_id`: must be `first-filter-is-not-null-slice`
- `case_id`: one of the stable case IDs from
  `tests/differential/first-filter-is-not-null-slice.md`
- `spec_refs[]`: shared repository refs that justify the case
- `input_ref`: one stable input identifier from
  `tests/differential/first-filter-is-not-null-slice.md`
- `filter_ref`: one stable predicate identifier from
  `tests/differential/first-filter-is-not-null-slice.md`

The shared request should **not** carry:

- raw SQL text
- engine-specific planner hints
- connection credentials or DSN details
- expected output rows

An adapter may derive engine-native setup and query text from those refs, but
that derivation remains adapter-local rather than becoming shared semantic
surface.

## Response Surface

Each adapter invocation should return one normalized `case result` record whose
fields match the minimum carrier now defined in
`tests/differential/first-filter-is-not-null-slice-artifacts.md`.

That record must include at least:

- `slice_id`
- `engine`
- `adapter`
- `case_id`
- `spec_refs[]`
- `input_ref`
- `filter_ref`
- `outcome.kind` = `rows` or `error`

### Row Outcomes

When `outcome.kind = rows`, the adapter response must include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using JSON scalars plus `null`
- `row_count`

For this slice, `logical_type` should use shared semantic names such as
`int32`, not engine-native display strings.

### Error Outcomes

When `outcome.kind = error`, the adapter response must include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

The first slice uses this normalized error-class vocabulary:

- `missing_column`: out-of-range `column(index)` reference
- `unsupported_predicate_type`: non-`int32` predicate input
- `adapter_unavailable`: adapter does not yet support a documented case
- `engine_error`: any other engine failure that this slice does not normalize
  more specifically

If one side returns `adapter_unavailable`, the later drift aggregation step may
classify the paired case as `unsupported` in the drift report.

## Field Notes

- `engine` should use stable lowercase names: `tidb` or `tiflash`
- `adapter` should identify the engine bridge, not the environment
- `spec_refs[]` should be copied through unchanged from the shared case
  definition so drift artifacts can cite the same semantic sources
- `input_ref` and `filter_ref` should remain stable identifiers, not rendered
  SQL

## Follow-On Boundary

Later issues may extend this boundary to cover:

- broader error normalization once this first vocabulary is too narrow
- reusable session profiles or adapter capability advertisement
- live TiDB and TiFlash runner orchestration beyond deterministic local
  adapter-core fixtures
- TiKV single-engine executable checkpoint work on top of
  `adapters/first-filter-is-not-null-slice-tikv.md`

Until then, this note fixes only the minimum request-and-response contract for
the first differential filter slice.
