# First Differential JSON Adapter Boundary

Status: issue #224 design checkpoint

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #224 `design: define first JSON semantic slice boundary`

## Purpose

This note defines the minimal shared boundary between the first differential
JSON slice and the engine-specific adapters that execute it.

The goal is to give future harness work one explicit contract between:

- the shared differential JSON slice in
  `tests/differential/first-json-slice.md`
- the shared conformance anchor in `tests/conformance/first-json-slice.md`
- TiDB and TiFlash execution plumbing that should stay adapter-local

## Scope

This boundary applies only to the first JSON differential slice:

- `slice_id = first-json-slice`
- engines: `TiDB` and `TiFlash`
- case families:
  - passthrough `column(index)` over `json`
  - `is_not_null(column(index))` over `json`
  - unsupported JSON ordering comparison probes
  - unsupported JSON cast probes

It does **not** yet define:

- TiKV participation in this slice
- JSON path extraction and mutation behavior
- JSON containment and existence operators
- successful explicit cast semantics involving `json`
- connection management, authentication, or environment provisioning
- a generalized adapter API for later JSON families
- engine-plan capture, timing, or runtime-event export

## Shared Ownership

Shared differential docs own:

- `slice_id`, `case_id`, and `input_ref`
- semantic meaning for each projection, filter, comparison, and cast probe case
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

The first JSON differential harness should submit one documented case at a time
to one engine-specific adapter.

The shared request surface is:

- `slice_id`: must be `first-json-slice`
- `case_id`: one of the stable case IDs from
  `tests/differential/first-json-slice.md`
- `spec_refs[]`: shared repository refs that justify the case
- `input_ref`: one stable input identifier from
  `tests/differential/first-json-slice.md`
- exactly one operation reference:
  - `projection_ref` for passthrough `column(index)` cases
  - `filter_ref` for `is_not_null(column(index))` cases
  - `comparison_ref` for unsupported-ordering probes
  - `cast_ref` for unsupported-cast probes

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
- exactly one operation reference matching the request
  (`projection_ref`, `filter_ref`, `comparison_ref`, or `cast_ref`)
- `outcome.kind` = `rows` or `error`

### Row Outcomes

When `outcome.kind = rows`, the adapter response must include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using JSON strings plus `null`
- `row_count`

For this slice, `logical_type` should use shared semantic names such as `json`,
not engine-native display strings.

For JSON rows, non-null values should be normalized as canonical JSON value
tokens carried as JSON strings (for example `"{\"a\":1}"`), while SQL `NULL`
remains carrier `null`.

### Error Outcomes

When `outcome.kind = error`, the adapter response must include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

This first JSON slice uses this normalized error-class vocabulary:

- `missing_column`: out-of-range `column(index)` reference
- `unsupported_json_comparison`: ordered JSON comparison outside current slice
  scope
- `unsupported_json_cast`: explicit non-identity cast involving `json` outside
  current slice scope
- `adapter_unavailable`: adapter does not yet support a documented case
- `engine_error`: any other engine failure that this slice does not normalize
  more specifically

If one side returns `adapter_unavailable`, later drift aggregation may classify
that paired case as `unsupported`.

## Field Notes

- `engine` should use stable lowercase names: `tidb` or `tiflash`
- `adapter` should identify the engine bridge, not the environment
- `spec_refs[]` should be copied through unchanged from shared case definitions
  so drift artifacts can cite the same semantic sources
- `input_ref` plus operation refs should remain stable identifiers, not rendered
  SQL

## Follow-On Boundary

Later issues may extend this boundary to cover:

- successful explicit JSON cast semantics and parse-error normalization
- broader JSON comparison-operator normalization
- reusable session profiles or adapter capability advertisement
- executable harness wiring and checked-in artifact carriers for this slice

Until then, this note fixes only the minimum request-and-response contract for
first-slice JSON differential comparison.
