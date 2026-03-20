# First Differential Unsigned Arithmetic Adapter Boundary

Status: issue #300 design checkpoint, issue #302 artifact-carrier checkpoint

Related issues:

- #141 `spec: define signed/unsigned interaction checkpoint for initial coercion lattice`
- #276 `spec: define overflow behavior checkpoint for follow-on operator families`
- #300 `design: define first unsigned arithmetic semantic slice boundary`
- #302 `docs: define first-unsigned-arithmetic-slice differential artifact carriers`

## Purpose

This note defines the minimal shared boundary between the first differential
unsigned arithmetic slice and the engine-specific adapters that execute it.

The goal is to give future harness work one explicit contract between:

- the shared differential unsigned arithmetic slice in
  `tests/differential/first-unsigned-arithmetic-slice.md`
- the shared conformance anchor in
  `tests/conformance/first-unsigned-arithmetic-slice.md`
- the shared artifact-carrier boundary in
  `tests/differential/first-unsigned-arithmetic-slice-artifacts.md`
- TiDB and TiFlash execution plumbing that should stay adapter-local

## Scope

This boundary applies only to the first unsigned arithmetic differential slice:

- `slice_id = first-unsigned-arithmetic-slice`
- engines: `TiDB` and `TiFlash`
- case families:
  - passthrough `column(index)` over `uint64`
  - `literal<uint64>(value)` projection probes
  - `add<uint64>(lhs, rhs)` arithmetic probes
  - `is_not_null(column(index))` over `uint64`

It does **not** yet define:

- TiKV participation in this slice
- unsigned families beyond `uint64`
- unsigned arithmetic beyond `add<uint64>`
- unsigned cast or coercion expansion
- connection management, authentication, or environment provisioning
- a generalized adapter API for later unsigned checkpoints
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

The first unsigned arithmetic differential harness should submit one documented
case at a time to one engine-specific adapter.

The shared request surface is:

- `slice_id`: must be `first-unsigned-arithmetic-slice`
- `case_id`: one of the stable case IDs from
  `tests/differential/first-unsigned-arithmetic-slice.md`
- `spec_refs[]`: shared repository refs that justify the case
- `input_ref`: one stable input identifier from
  `tests/differential/first-unsigned-arithmetic-slice.md`
- exactly one operation reference:
  - `projection_ref` for passthrough, literal, and add probes
  - `filter_ref` for `is_not_null(column(index))` probes

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
- `rows[]` using canonical `uint64` decimal-string tokens plus `null`
- `row_count`

For this slice, `logical_type` should use shared semantic names such as
`uint64`, not engine-native display strings.

For `uint64` row values, adapters should normalize non-null values as canonical
base-10 integer strings so full-range `uint64` values remain exact, while SQL
`NULL` rows remain carrier `null`.

### Error Outcomes

When `outcome.kind = error`, the adapter response must include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

This first unsigned arithmetic slice uses this normalized error-class
vocabulary:

- `missing_column`: out-of-range `column(index)` reference
- `unsigned_overflow`: row-wise overflow in `add<uint64>(lhs, rhs)`
- `mixed_signed_unsigned`: mixed signed and unsigned arithmetic request in this
  checkpoint
- `unsupported_unsigned_family`: unsigned logical type outside current `uint64`
  scope
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

- broader unsigned error normalization
- reusable session profiles or adapter capability advertisement
- executable harness wiring and checked-in inventory artifact refresh for this slice
- TiKV single-engine and pairwise checkpoints for this slice

Until then, this note fixes only the minimum request-and-response contract for
first-slice unsigned arithmetic differential comparison.
