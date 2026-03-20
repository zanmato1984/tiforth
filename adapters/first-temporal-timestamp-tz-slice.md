# First Differential Temporal `timestamp_tz(us)` Adapter Boundary

Status: issue #280 design checkpoint, issue #290 TiKV boundary checkpoint

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #178 `milestone-1: implement first executable temporal date32 slice in local kernel`
- #280 `design: define first timezone-aware timestamp semantic slice checkpoint`
- #290 `design: define TiKV adapter boundary for first-temporal-timestamp-tz-slice`

## Purpose

This note defines the minimal shared boundary between the first differential
timezone-aware timestamp slice and the engine-specific adapters that execute
it.

The goal is to give future harness work one explicit contract between:

- the shared differential timestamp-timezone slice in
  `tests/differential/first-temporal-timestamp-tz-slice.md`
- the shared conformance anchor in
  `tests/conformance/first-temporal-timestamp-tz-slice.md`
- TiDB and TiFlash execution plumbing that should stay adapter-local

## Scope

This boundary applies only to the first timestamp-timezone differential slice:

- `slice_id = first-temporal-timestamp-tz-slice`
- engines: `TiDB` and `TiFlash`
- case families:
  - passthrough `column(index)` over `timestamp_tz(us)`
  - `is_not_null(column(index))` over `timestamp_tz(us)`
  - ordering probe `order-by(column(index), asc, nulls_last)` over
    `timestamp_tz(us)`
  - unsupported timestamp-without-timezone and timestamp-unit probes

It does **not** yet define:

- TiKV-specific single-engine and pairwise checkpoint details, which are
  handled separately in `adapters/first-temporal-timestamp-tz-slice-tikv.md`
- temporal arithmetic, casts, extraction, truncation, or interval behavior
- timezone-name canonicalization or timezone-database negotiation
- connection management, authentication, or environment provisioning
- a generalized adapter API for later temporal families
- engine-plan capture, timing, or runtime-event export

## Shared Ownership

Shared differential docs own:

- `slice_id`, `case_id`, and `input_ref`
- semantic meaning for each projection, filter, and ordering case
- semantic meaning for unsupported temporal-type and temporal-unit probe cases
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

The first timestamp-timezone differential harness should submit one documented
case at a time to one engine-specific adapter.

The shared request surface is:

- `slice_id`: must be `first-temporal-timestamp-tz-slice`
- `case_id`: one of the stable case IDs from
  `tests/differential/first-temporal-timestamp-tz-slice.md`
- `spec_refs[]`: shared repository refs that justify the case
- `input_ref`: one stable input identifier from
  `tests/differential/first-temporal-timestamp-tz-slice.md`
- exactly one operation reference:
  - `projection_ref` for passthrough `column(index)` cases
  - `filter_ref` for `is_not_null(column(index))` cases
  - `ordering_ref` for ordering-probe cases

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
  (`projection_ref`, `filter_ref`, or `ordering_ref`)
- `outcome.kind` = `rows` or `error`

### Row Outcomes

When `outcome.kind = rows`, the adapter response must include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using JSON numbers plus `null`
- `row_count`

For this slice, `logical_type` should use shared semantic names such as
`timestamp_tz(us)`, not engine-native display strings.

For `timestamp_tz(us)` rows, non-null values should be normalized as signed UTC
epoch-microsecond integers so equivalent instants compare equal even when input
offsets differ.

### Error Outcomes

When `outcome.kind = error`, the adapter response must include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

This first timestamp-timezone slice uses this normalized error-class
vocabulary:

- `missing_column`: out-of-range `column(index)` reference
- `unsupported_temporal_type`: temporal logical family outside current
  `timestamp_tz(us)` scope (for example `timestamp` without timezone)
- `unsupported_temporal_unit`: timezone-aware timestamp unit outside current
  `us` scope
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

- reusable session profiles or adapter capability advertisement
- checked-in artifact carriers and live-runner wiring for this slice
- broader temporal error normalization beyond this checkpoint
- additional temporal families and timestamp unit coverage
- TiKV live-runner and pairwise refresh workflow details beyond the docs-first
  TiKV boundary

Issue #290 now defines the TiKV-specific request and response boundary for this
same slice in `adapters/first-temporal-timestamp-tz-slice-tikv.md`.

Until then, this note fixes only the minimum request-and-response contract for
first-slice timezone-aware timestamp differential comparison.
