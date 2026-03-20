# First Differential Float64 NaN/Infinity Ordering Adapter Boundary

Status: issue #194 design checkpoint, issue #286 TiKV boundary checkpoint, issue #292 TiKV single-engine checkpoint

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #194 `design: define first float64 NaN, infinity, and ordering checkpoint`
- #286 `design: define TiKV adapter boundary for first-float64-ordering-slice`
- #292 `harness: execute first-float64-ordering-slice TiKV single-engine artifacts`

## Purpose

This note defines the minimal shared boundary between the first differential
float64 ordering slice and the engine-specific adapters that execute it.

The goal is to give future harness work one explicit contract between:

- the shared differential float64 slice in
  `tests/differential/first-float64-ordering-slice.md`
- the shared conformance anchor in
  `tests/conformance/first-float64-ordering-slice.md`
- TiDB and TiFlash execution plumbing that should stay adapter-local

## Scope

This boundary applies only to the first float64 differential slice:

- `slice_id = first-float64-ordering-slice`
- engines: `TiDB` and `TiFlash`
- case families:
  - passthrough `column(index)` over `float64`
  - `is_not_null(column(index))` over `float64`
  - comparison-mode tagged row normalization for float64 special values

It does **not** yet define:

- TiKV-specific single-engine and pairwise checkpoint details, which are
  handled separately in `adapters/first-float64-ordering-slice-tikv.md`
- shared SQL ordering syntax or null-ordering policy
- float arithmetic, cast, or coercion semantics
- connection management, authentication, or environment provisioning
- a generalized adapter API for later float families
- engine-plan capture, timing, or runtime-event export

## Shared Ownership

Shared differential docs own:

- `slice_id`, `case_id`, `input_ref`, and `comparison_mode`
- semantic meaning for each projection and filter case
- normalized comparison rules and drift classification meanings
- the minimum normalized `case result` fields for this slice

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

The first float64 differential harness should submit one documented case at a
time to one engine-specific adapter.

The shared request surface is:

- `slice_id`: must be `first-float64-ordering-slice`
- `case_id`: one of the stable case IDs from
  `tests/differential/first-float64-ordering-slice.md`
- `spec_refs[]`: shared repository refs that justify the case
- `input_ref`: one stable input identifier from
  `tests/differential/first-float64-ordering-slice.md`
- `comparison_mode`: one stable comparison-mode identifier from
  `tests/differential/first-float64-ordering-slice.md`
- exactly one operation reference:
  - `projection_ref` for passthrough `column(index)` cases
  - `filter_ref` for `is_not_null(column(index))` cases

The shared request should **not** carry:

- raw SQL text
- engine-specific planner hints
- connection credentials or DSN details
- expected output rows

An adapter may derive engine-native setup and query text from those refs, but
that derivation remains adapter-local rather than becoming shared semantic
surface.

## Response Surface

Each adapter invocation should return one normalized `case result` record.

That record must include at least:

- `slice_id`
- `engine`
- `adapter`
- `case_id`
- `spec_refs[]`
- `input_ref`
- `comparison_mode`
- exactly one operation reference matching the request (`projection_ref` or
  `filter_ref`)
- `outcome.kind` = `rows` or `error`

### Row Outcomes

When `outcome.kind = rows`, the adapter response must include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using canonical float64 string tokens plus `null`
- `row_count`

For this slice, `logical_type` should use shared semantic names such as
`float64`, not engine-native display strings.

For `float64` row values, adapters should normalize to canonical tokens used by
this slice:

- `-Infinity`
- `Infinity`
- `NaN`
- finite decimal strings with signed-zero distinction (`-0.0`, `0.0`)

### Error Outcomes

When `outcome.kind = error`, the adapter response must include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

This first float64 slice uses this normalized error-class vocabulary:

- `missing_column`: out-of-range `column(index)` reference
- `unsupported_floating_type`: floating logical family outside current
  `float64` scope
- `adapter_unavailable`: adapter does not yet support a documented case
- `engine_error`: any other engine failure that this slice does not normalize
  more specifically

If one side returns `adapter_unavailable`, later drift aggregation may classify
that paired case as `unsupported`.

## Field Notes

- `engine` should use stable lowercase names: `tidb` or `tiflash`
- `adapter` should identify the engine bridge, not the environment
- `spec_refs[]` should be copied through unchanged from shared case
  definitions so drift artifacts can cite the same semantic sources
- `comparison_mode` is request metadata and should be copied into the response
  unchanged
- `input_ref` plus operation refs should remain stable identifiers, not rendered
  SQL

## Follow-On Boundary

Later issues may extend this boundary to cover:

- broader floating error normalization
- reusable session profiles or adapter capability advertisement
- checked-in artifact carriers and live runner wiring for this slice
- TiKV live-runner and pairwise refresh workflow details beyond the current
  TiKV executable single-engine checkpoint

Issue #286 defines the TiKV-specific request and response boundary for this
same slice in `adapters/first-float64-ordering-slice-tikv.md`. Issue #292
adds the first executable TiKV single-engine checkpoint on top of that
boundary.

Until then, this note fixes only the minimum request-and-response contract for
first-slice float64 NaN/infinity and ordering comparison.
