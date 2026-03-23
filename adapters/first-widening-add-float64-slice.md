# First Differential Widening `add<float64>` Adapter Boundary

Status: issue #427 design checkpoint

Related issues:

- #194 `design: define first float64 NaN, infinity, and ordering checkpoint`
- #409 `epic: complete function-family program`
- #422 `spec: complete the numeric add/plus family boundary`
- #427 `design: define first widening add/float64 slice for the numeric add/plus family`

## Purpose

This note defines the minimal shared boundary between the first differential
widening `add<float64>` slice and the engine-specific adapters that execute it.

The goal is to give future harness work one explicit contract between:

- the shared differential widening-float64 slice in
  `tests/differential/first-widening-add-float64-slice.md`
- the shared conformance anchor in
  `tests/conformance/first-widening-add-float64-slice.md`
- the shared artifact-carrier boundary in
  `tests/differential/first-widening-add-float64-slice-artifacts.md`
- TiDB and TiFlash execution plumbing that should stay adapter-local

## Scope

This boundary applies only to the first widening-float64 differential slice:

- `slice_id = first-widening-add-float64-slice`
- engines: `TiDB` and `TiFlash`
- case families:
  - passthrough `column(index)` over `float64`
  - exact `add<float64>(lhs, rhs)` probes
  - admitted widening `int32 + float64 -> add<float64>` probes
  - admitted widening `int64 + float64 -> add<float64>` probes
  - admitted widening `float64 + int32 -> add<float64>` probes
  - admitted widening `float64 + int64 -> add<float64>` probes

It does **not** yet define:

- TiKV participation in this slice
- `literal<float64>`, `literal<int32>`, or `literal<int64>` add probes
- `is_not_null(column(index))` over `float64` inside this add slice
- exact or mixed `float32` add checkpoints
- explicit near-`2^53` precision-loss probes
- decimal add or mixed signed/unsigned success semantics
- connection management, authentication, or environment provisioning
- a generalized adapter API for later float checkpoints
- engine-plan capture, timing, or runtime-event export

## Shared Ownership

Shared differential docs own:

- `slice_id`, `case_id`, `input_ref`, and `comparison_mode`
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

The first widening-float64 differential harness should submit one documented
case at a time to one engine-specific adapter.

The shared request surface is:

- `slice_id`: must be `first-widening-add-float64-slice`
- `case_id`: one of the stable case IDs from
  `tests/differential/first-widening-add-float64-slice.md`
- `spec_refs[]`: shared repository refs that justify the case
- `input_ref`: one stable input identifier from
  `tests/differential/first-widening-add-float64-slice.md`
- `comparison_mode`: the stable comparison-mode identifier from
  `tests/differential/first-widening-add-float64-slice.md`
- `projection_ref`: one stable projection identifier from
  `tests/differential/first-widening-add-float64-slice.md`

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
- `projection_ref`
- `outcome.kind` = `rows` or `error`

### Row Outcomes

When `outcome.kind = rows`, the adapter response must include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using canonical float64 string tokens plus `null`
- `row_count`

For this slice, `logical_type` should use shared semantic names such as
`float64`, not engine-native display strings.

For widening-success cases, adapters should still normalize the selected result
as logical type `float64`; engine-local arithmetic metadata must not silently
change that shared result-type identity.

For `float64` row values, adapters should normalize to the canonical tokens
already accepted for `first-float64-ordering-slice`:

- `-Infinity`
- `Infinity`
- `NaN`
- finite decimal strings with signed-zero distinction (`-0.0`, `0.0`)

If the selected `add<float64>` produces `NaN`, `Infinity`, `-Infinity`, or
signed zero, that outcome stays a `rows` result rather than becoming an
arithmetic error.

### Error Outcomes

When `outcome.kind = error`, the adapter response must include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

This first widening-float64 slice uses this normalized error-class vocabulary:

- `missing_column`: out-of-range `column(index)` reference
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
- `input_ref` and `projection_ref` should remain stable identifiers, not
  rendered SQL

## Follow-On Boundary

Later issues may extend this boundary to cover:

- executable TiDB/TiFlash adapter and harness coverage plus checked-in
  `inventory/` artifacts for this slice
- broader floating error normalization
- explicit precision-loss probes for admitted widening cases
- reusable session profiles or adapter capability advertisement
- TiKV single-engine and pairwise checkpoints for float64 add

Until then, this note fixes only the minimum request-and-response contract for
first-slice widening `add<float64>` differential comparison for the add-family
follow-on.
