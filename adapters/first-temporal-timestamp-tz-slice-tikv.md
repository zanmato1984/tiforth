# First TiKV Temporal `timestamp_tz(us)` Adapter Boundary

Status: issue #290 design checkpoint

Verified: 2026-03-20

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #280 `design: define first timezone-aware timestamp semantic slice checkpoint`
- #288 `kernel: execute first timestamp_tz(us) local conformance slice`
- #290 `design: define TiKV adapter boundary for first-temporal-timestamp-tz-slice`

## Purpose

This note defines the first TiKV-specific adapter boundary for the existing
`first-temporal-timestamp-tz-slice` semantic checkpoint.

The goal is to make TiKV request and response expectations explicit for harness
and follow-on adapter work without changing shared slice semantics or widening
temporal families.

## Scope

This boundary applies only to:

- `slice_id = first-temporal-timestamp-tz-slice`
- engine: `tikv`
- case families already documented in
  `tests/differential/first-temporal-timestamp-tz-slice.md`

It does not define:

- TiKV single-engine execution coverage or pairwise drift artifacts for this
  slice
- TiKV connection provisioning, cluster topology, or deployment assumptions
- planner, coprocessor, or pushdown strategy details
- temporal arithmetic, cast, extraction, truncation, or interval behavior
- timezone-name canonicalization or timezone-database negotiation

## Shared Ownership

Shared differential docs still own:

- stable `slice_id`, `case_id`, `input_ref`, and operation refs
- semantic meaning for each first-slice case
- normalized `case result` carrier fields and error vocabulary for this slice

The TiKV adapter boundary owns:

- TiKV-native request derivation from shared refs
- TiKV execution mechanics and session policy
- translation from TiKV-native rows or failures into normalized case results

This keeps shared semantics centralized while adapter execution remains
engine-local.

## Request Surface

Harnesses should submit one documented first timestamp-timezone case at a time
to the TiKV adapter with the same minimal request fields already used by TiDB
and TiFlash:

- `slice_id`
- `case_id`
- `spec_refs[]`
- `input_ref`
- exactly one operation reference:
  - `projection_ref` for passthrough `column(index)` cases
  - `filter_ref` for `is_not_null(column(index))` cases
  - `ordering_ref` for ordering-probe cases

The request should not carry engine-native query text, planner directives,
credentials, or expected rows.

## Response Surface

Each TiKV adapter invocation returns one normalized `case result` record with at
least:

- `slice_id`
- `engine = tikv`
- `adapter`
- `case_id`
- `spec_refs[]`
- `input_ref`
- exactly one operation reference matching the request
  (`projection_ref`, `filter_ref`, or `ordering_ref`)
- `outcome.kind = rows` or `error`

When `outcome.kind = rows`, the record includes:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` in normalized JSON numeric scalar form plus `null`
- `row_count`

For this slice, non-null `timestamp_tz(us)` row values should normalize to
signed UTC epoch-microsecond integers so equivalent instants compare equal
across engines.

When `outcome.kind = error`, the record includes:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this first TiKV timestamp-timezone checkpoint, the normalized error
vocabulary is unchanged:

- `missing_column`
- `unsupported_temporal_type`
- `unsupported_temporal_unit`
- `adapter_unavailable`
- `engine_error`

## First Checkpoint Expectations

- issue #290 defines docs-first TiKV request and response ownership for every
  documented first timestamp-timezone `case_id`
- until a follow-on executable issue lands, TiKV may return
  `adapter_unavailable` for documented cases
- normalized field meanings stay aligned with
  `tests/differential/first-temporal-timestamp-tz-slice.md`

## Follow-On Boundary

After this docs-first request/response checkpoint, follow-on issues may
separately define:

- deterministic TiKV single-engine timestamp-timezone adapter and harness
  execution
- deterministic TiKV pairwise drift rendering for `tidb-vs-tikv` and
  `tiflash-vs-tikv`
- live TiKV timestamp-timezone runner wiring and environment-backed refresh
  workflow

## Result

TiKV now has a concrete docs-first request and response boundary for
`first-temporal-timestamp-tz-slice` while the shared differential executable
checkpoint remains TiDB-versus-TiFlash until follow-on TiKV execution issues
land.
