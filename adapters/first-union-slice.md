# First Differential Union Adapter Boundary

Status: issue #241 design checkpoint

Related issues:

- #151 `design: define first nested-family shared handoff slice and claim-ownership boundary`
- #226 `design: define first struct nested handoff slice checkpoint`
- #230 `docs: define first map nested handoff slice checkpoint`
- #241 `docs: define first union nested handoff slice checkpoint`

## Purpose

This note defines the minimal shared boundary between the first differential
union slice and the engine-specific adapters that execute it.

The goal is to give future harness work one explicit contract between:

- the shared differential union slice in `tests/differential/first-union-slice.md`
- the shared conformance anchor in `tests/conformance/first-union-slice.md`
- TiDB and TiFlash execution plumbing that should stay adapter-local

## Scope

This boundary applies only to the first union differential slice:

- `slice_id = first-union-slice`
- engines: `TiDB` and `TiFlash`
- case family:
  - passthrough `column(index)` over `dense_union<i:int32, n:int32?>`

It does **not** yet define:

- TiKV participation in this slice
- nested predicate behavior over `union`
- nested compute behavior beyond passthrough `column(index)`
- broader nested families (including `sparse_union` and nested combinations)
- connection management, authentication, or environment provisioning
- a generalized adapter API for later nested families
- engine-plan capture, timing, or runtime-event export

## Shared Ownership

Shared differential docs own:

- `slice_id`, `case_id`, and `input_ref`
- semantic meaning for each projection case
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

The first union differential harness should submit one documented case at a
time to one engine-specific adapter.

The shared request surface is:

- `slice_id`: must be `first-union-slice`
- `case_id`: one of the stable case IDs from
  `tests/differential/first-union-slice.md`
- `spec_refs[]`: shared repository refs that justify the case
- `input_ref`: one stable input identifier from
  `tests/differential/first-union-slice.md`
- exactly one operation reference:
  - `projection_ref` for passthrough `column(index)` cases

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
- `projection_ref`
- `outcome.kind` = `rows` or `error`

### Row Outcomes

When `outcome.kind = rows`, the adapter response must include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using canonical union carrier objects
- `row_count`

For this slice, `logical_type` should use shared semantic names such as
`dense_union<i:int32,n:int32?>`, not engine-native display strings.

For union rows, each value should be normalized as a canonical JSON object with
stable keys (`tag`, then `value`) where tag is one of `i` or `n` and value is a
JSON number or `null` according to the selected variant payload.

### Error Outcomes

When `outcome.kind = error`, the adapter response must include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

This first union slice uses this normalized error-class vocabulary:

- `missing_column`: out-of-range `column(index)` reference
- `unsupported_nested_family`: nested logical family outside current
  `dense_union<i:int32, n:int32?>` scope
- `adapter_unavailable`: adapter does not yet support a documented case
- `engine_error`: any other engine failure that this slice does not normalize
  more specifically

If one side returns `adapter_unavailable`, later drift aggregation may classify
that paired case as `unsupported`.

## Follow-On Boundary

Later issues may extend this boundary to cover:

- broader nested error normalization
- reusable session profiles or adapter capability advertisement
- checked-in artifact carriers and executable harness wiring for this slice

Until then, this note fixes only the minimum request-and-response contract for
first-slice union differential comparison.
