# First Differential Collation-Sensitive String Adapter Boundary

Status: issue #233 design checkpoint

Related issues:

- #143 `docs: define initial collation scope and ownership boundary`
- #233 `design: define first string collation semantic slice`

## Purpose

This note defines the minimal shared boundary between the first differential
collation-sensitive string slice and the engine-specific adapters that execute
it.

The goal is to give future harness work one explicit contract between:

- the shared differential collation slice in
  `tests/differential/first-collation-string-slice.md`
- the shared conformance anchor in
  `tests/conformance/first-collation-string-slice.md`
- TiDB and TiFlash execution plumbing that should stay adapter-local

## Scope

This boundary applies only to the first collation-sensitive string differential
slice:

- `slice_id = first-collation-string-slice`
- engines: `TiDB` and `TiFlash`
- case families:
  - passthrough `column(index)` over `utf8`
  - `is_not_null(column(index))` over `utf8`
  - collation-tagged comparison probes over `utf8`
  - collation-tagged canonical ordering probes over `utf8`

It does **not** yet define:

- TiKV participation in this slice
- locale-specific collation-family expansion beyond `binary` and `unicode_ci`
- a generalized adapter API for later string and binary families
- connection management, authentication, or environment provisioning
- engine-plan capture, timing, or runtime-event export

## Shared Ownership

Shared differential docs own:

- `slice_id`, `case_id`, `input_ref`, and `collation_ref`
- semantic meaning for each projection, filter, comparison, and ordering case
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

The first collation differential harness should submit one documented case at a
time to one engine-specific adapter.

The shared request surface is:

- `slice_id`: must be `first-collation-string-slice`
- `case_id`: one of the stable case IDs from
  `tests/differential/first-collation-string-slice.md`
- `spec_refs[]`: shared repository refs that justify the case
- `input_ref`: one stable input identifier from
  `tests/differential/first-collation-string-slice.md`
- `collation_ref`: one stable collation identifier from
  `tests/differential/first-collation-string-slice.md`
- exactly one operation reference:
  - `projection_ref` for passthrough `column(index)` cases
  - `filter_ref` for `is_not_null(column(index))` cases
  - `comparison_ref` for collation comparison probes
  - `ordering_ref` for collation ordering probes

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
- `collation_ref`
- exactly one operation reference matching the request (`projection_ref`,
  `filter_ref`, `comparison_ref`, or `ordering_ref`)
- `outcome.kind` = `rows` or `error`

### Row Outcomes

When `outcome.kind = rows`, the adapter response must include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using UTF-8 strings, booleans, and `null`
- `row_count`

For this slice, `logical_type` should use shared semantic names such as
`utf8` and `boolean`, not engine-native display strings.

For ordering-probe cases, adapters should return rows in the collation intent
order fixed by the shared case and `collation_ref`.

### Error Outcomes

When `outcome.kind = error`, the adapter response must include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

This first collation slice uses this normalized error-class vocabulary:

- `missing_column`: out-of-range `column(index)` reference
- `unknown_collation`: unsupported `collation_ref` identifier
- `unsupported_collation_type`: collation-sensitive comparison requested for
  non-`utf8` input
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
- `input_ref`, `collation_ref`, and operation refs should remain stable
  identifiers, not rendered SQL

## Follow-On Boundary

Later issues may extend this boundary to cover:

- checked-in artifact carriers and live-runner wiring for this slice
- broader collation capability advertisement and mapping
- additional string and binary families beyond this first checkpoint

Until then, this note fixes only the minimum request-and-response contract for
first-slice collation-sensitive string differential comparison.
