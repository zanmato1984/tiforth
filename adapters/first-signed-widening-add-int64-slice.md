# First Differential Signed-Widening `add<int64>` Adapter Boundary

Status: issue #426 design checkpoint

Related issues:

- #276 `spec: define overflow behavior checkpoint for follow-on operator families`
- #409 `epic: complete function-family program`
- #422 `spec: complete the numeric add/plus family boundary`
- #426 `design: define first signed-widening add/int64 slice for the numeric add/plus family`

## Purpose

This note defines the minimal shared boundary between the first differential
signed-widening `add<int64>` slice and the engine-specific adapters that
execute it.

The goal is to give future harness work one explicit contract between:

- the shared differential signed-widening slice in
  `tests/differential/first-signed-widening-add-int64-slice.md`
- the shared conformance anchor in
  `tests/conformance/first-signed-widening-add-int64-slice.md`
- the shared artifact-carrier boundary in
  `tests/differential/first-signed-widening-add-int64-slice-artifacts.md`
- TiDB and TiFlash execution plumbing that should stay adapter-local

## Scope

This boundary applies only to the first signed-widening differential slice:

- `slice_id = first-signed-widening-add-int64-slice`
- engines: `TiDB` and `TiFlash`
- case families:
  - passthrough `column(index)` over `int64`
  - exact `add<int64>(lhs, rhs)` probes
  - admitted widening `int32 + int64 -> add<int64>` probes
  - admitted widening `int64 + int32 -> add<int64>` probes

It does **not** yet define:

- TiKV participation in this slice
- `literal<int64>` or `literal<int32>` signed-add probes
- `is_not_null(column(index))` over `int64`
- broader signed-width families beyond `int32` and `int64`
- `float64` add or decimal add checkpoints
- connection management, authentication, or environment provisioning
- a generalized adapter API for later signed checkpoints
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

The first signed-widening differential harness should submit one documented
case at a time to one engine-specific adapter.

The shared request surface is:

- `slice_id`: must be `first-signed-widening-add-int64-slice`
- `case_id`: one of the stable case IDs from
  `tests/differential/first-signed-widening-add-int64-slice.md`
- `spec_refs[]`: shared repository refs that justify the case
- `input_ref`: one stable input identifier from
  `tests/differential/first-signed-widening-add-int64-slice.md`
- `projection_ref`: one stable projection identifier from
  `tests/differential/first-signed-widening-add-int64-slice.md`

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
- `rows[]` using JSON numeric scalars plus `null`
- `row_count`

For this slice, `logical_type` should use shared semantic names such as
`int64`, not engine-native display strings.

For widening-success cases, adapters should still normalize the selected result
as logical type `int64`; engine-local arithmetic metadata must not silently
change that shared result-type identity.

### Error Outcomes

When `outcome.kind = error`, the adapter response must include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

This first signed-widening slice uses this normalized error-class vocabulary:

- `missing_column`: out-of-range `column(index)` reference
- `arithmetic_overflow`: row-wise overflow after `add<int64>` is selected,
  including admitted widening cases
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
- `input_ref` and `projection_ref` should remain stable identifiers, not
  rendered SQL

## Follow-On Boundary

Later issues may extend this boundary to cover:

- executable TiDB/TiFlash adapter and harness coverage plus checked-in
  `inventory/` artifacts for this slice
- broader signed error normalization
- reusable session profiles or adapter capability advertisement
- TiKV single-engine and pairwise checkpoints for signed add

Until then, this note fixes only the minimum request-and-response contract for
first-slice signed-widening differential comparison for the add-family
follow-on.
