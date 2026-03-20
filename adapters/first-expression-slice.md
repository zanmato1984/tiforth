# First Differential Expression Adapter Boundary

Status: issue #72 design checkpoint

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #72 `design: define first differential adapter request/response surface`

## Purpose

This note defines the minimal shared boundary between the first differential expression slice and the engine-specific adapters that execute it.

The goal is to give future harness work one small, explicit contract between:

- the shared differential slice in `tests/differential/first-expression-slice.md`
- the stable first-slice artifact carriers in `tests/differential/first-expression-slice-artifacts.md`
- the TiDB- and TiFlash-specific execution plumbing that should stay adapter-local

## Scope

This boundary applies only to the first differential expression slice:

- `slice_id = first-expression-slice`
- engines: `TiDB` and `TiFlash`
- case family: projection plus `column`, `literal<int32>`, and `add<int32>` comparisons already documented in `tests/differential/first-expression-slice.md`

It does **not** yet define:

- TiKV-specific adapter boundary details, which are handled separately in `adapters/first-expression-slice-tikv.md`
- connection management, authentication, or environment provisioning
- a generalized adapter API for later harness families
- engine-plan capture, timing, or runtime-event export

## Shared Ownership

Shared differential docs own:

- `slice_id`, `case_id`, `input_ref`, and `projection_ref`
- semantic meaning for each case
- normalized comparison rules and drift classification meanings
- the minimum normalized `case result` fields that inventory artifacts must carry

Adapters own:

- engine-native SQL or expression construction
- session setup, planner hints, and connection details
- execution against the target engine
- translation from engine-native rows or failures into the shared normalized carrier
- runtime orchestration, timeout or retry policy, cancellation transport, and diagnostics capture as bounded by `docs/design/adapter-runtime-orchestration-boundary.md`

This keeps shared docs focused on semantics while adapters stay thin and execution-oriented.

## Request Surface

The first executable differential harness should submit one documented case at a time to one engine-specific adapter.

The shared request surface is:

- `slice_id`: must be `first-expression-slice`
- `case_id`: one of the stable case IDs from `tests/differential/first-expression-slice.md`
- `spec_refs[]`: shared repository references that justify the case
- `input_ref`: one stable input identifier from `tests/differential/first-expression-slice.md`
- `projection_ref`: one stable projection or expression identifier from `tests/differential/first-expression-slice.md`

The shared request should **not** carry:

- raw SQL text
- engine-specific planner hints
- connection credentials or DSN details
- expected output rows

An adapter may derive engine-native setup and query text from those stable refs, but that derivation remains adapter-local rather than becoming shared semantic surface.

## Response Surface

Each adapter invocation should return one normalized `case result` record whose fields match the minimum carrier now defined in `tests/differential/first-expression-slice-artifacts.md`.

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
- `rows[]` using JSON scalars plus `null`
- `row_count`

For the first slice, `logical_type` should use shared semantic names such as `int32`, not engine-native display strings.

### Error Outcomes

When `outcome.kind = error`, the adapter response must include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

The first slice only needs a very small normalized error-class vocabulary:

- `arithmetic_overflow`: the documented overflow checkpoint for `add<int32>`
- `adapter_unavailable`: the adapter does not yet support executing a documented first-slice case
- `engine_error`: any other engine failure that the current slice does not normalize more specifically

If one side returns `adapter_unavailable`, the later drift aggregation step may classify the paired case as `unsupported` in the drift report. The adapter response itself should still remain a `case result` with `outcome.kind = error`.

## Field Notes

- `engine` should use stable lowercase engine names: `tidb` or `tiflash`
- `adapter` should identify the engine bridge, not the environment; a SQL-backed first implementation may use values such as `tidb-sql` and `tiflash-sql`
- `spec_refs[]` should be copied through unchanged from the shared case definition so drift artifacts can cite the same semantic sources
- `input_ref` and `projection_ref` should remain stable identifiers, not rendered SQL

## Follow-On Boundary

Later issues may extend this boundary to cover:

- executable TiKV pairwise drift-report generation beyond the first request/response checkpoint (policy fixed in `tests/differential/first-expression-slice-artifacts.md`)
- broader error normalization
- reusable session profiles or adapter capability advertisement
- live TiDB and TiFlash runner orchestration beyond the current deterministic harness fixtures

Until then, this note fixes only the minimum request-and-response contract needed to keep the first TiDB-versus-TiFlash differential slice coherent.
