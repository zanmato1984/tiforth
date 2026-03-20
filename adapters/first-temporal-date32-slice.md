# First Differential Temporal `date32` Adapter Boundary

Status: issue #176 design checkpoint, issue #264 TiKV boundary checkpoint, issue #266 TiKV single-engine checkpoint, issue #270 TiKV pairwise checkpoint

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #264 `design: define first TiKV temporal date32 adapter request/response surface`
- #266 `adapter: execute first-temporal-date32-slice through TiKV`
- #270 `harness: add first-temporal-date32-slice TiKV pairwise drift artifacts`

## Purpose

This note defines the minimal shared boundary between the first differential
temporal `date32` slice and the engine-specific adapters that execute it.

The goal is to give harness work one explicit contract between:

- the shared differential temporal slice in
  `tests/differential/first-temporal-date32-slice.md`
- the shared conformance anchor in
  `tests/conformance/first-temporal-date32-slice.md`
- TiDB and TiFlash execution plumbing that should stay adapter-local

## Scope

This boundary applies only to the first temporal differential slice:

- `slice_id = first-temporal-date32-slice`
- engines: `TiDB` and `TiFlash`
- case families:
  - passthrough `column(index)` over `date32`
  - `is_not_null(column(index))` over `date32`

It does **not** yet define:

- TiKV-specific single-engine and pairwise checkpoint details, which are handled separately in
  `adapters/first-temporal-date32-slice-tikv.md`
- timezone-aware timestamp normalization or ordering policy
- temporal arithmetic, cast, extract, or truncation semantics
- connection management, authentication, or environment provisioning
- a generalized adapter API for later temporal families
- engine-plan capture, timing, or runtime-event export

## Shared Ownership

Shared differential docs own:

- `slice_id`, `case_id`, and `input_ref`
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

The first temporal differential harness should submit one documented case at a
time to one engine-specific adapter.

The shared request surface is:

- `slice_id`: must be `first-temporal-date32-slice`
- `case_id`: one of the stable case IDs from
  `tests/differential/first-temporal-date32-slice.md`
- `spec_refs[]`: shared repository refs that justify the case
- `input_ref`: one stable input identifier from
  `tests/differential/first-temporal-date32-slice.md`
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
- exactly one operation reference matching the request (`projection_ref` or
  `filter_ref`)
- `outcome.kind` = `rows` or `error`

### Row Outcomes

When `outcome.kind = rows`, the adapter response must include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using JSON scalars plus `null`
- `row_count`

For this slice, `logical_type` should use shared semantic names such as
`date32`, not engine-native display strings.

For `date32` rows, values should be normalized as signed day-domain integers
(days since Unix epoch) so cross-engine comparison is timezone-agnostic in this
first checkpoint.

### Error Outcomes

When `outcome.kind = error`, the adapter response must include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

This first temporal slice uses this normalized error-class vocabulary:

- `missing_column`: out-of-range `column(index)` reference
- `unsupported_temporal_type`: temporal logical family outside current
  `date32` scope
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
- `input_ref` plus operation refs should remain stable identifiers, not rendered
  SQL

## Follow-On Boundary

Later issues may extend this boundary to cover:

- broader temporal error normalization
- reusable session profiles or adapter capability advertisement
- checked-in artifact carriers and live runner wiring for this slice
- TiKV live-runner and refresh workflow details beyond the current deterministic artifact checkpoints

Until then, this note fixes only the minimum request-and-response contract for
first-slice temporal `date32` differential comparison.
