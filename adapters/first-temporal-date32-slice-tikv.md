# First TiKV Temporal `date32` Adapter Boundary

Status: issue #264 design checkpoint, issue #266 executable checkpoint, issue #270 pairwise checkpoint

Verified: 2026-03-20

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #264 `design: define first TiKV temporal date32 adapter request/response surface`
- #266 `adapter: execute first-temporal-date32-slice through TiKV`
- #270 `harness: add first-temporal-date32-slice TiKV pairwise drift artifacts`

## Purpose

This note defines the first TiKV-specific adapter boundary for the existing
`first-temporal-date32-slice` semantic checkpoint.

The goal is to make TiKV request and response expectations explicit for harness
and follow-on adapter work without changing shared slice semantics or widening
temporal families.

## Scope

This boundary applies only to:

- `slice_id = first-temporal-date32-slice`
- engine: `tikv`
- case families already documented in
  `tests/differential/first-temporal-date32-slice.md`

It does not define:

- TiKV connection provisioning, cluster topology, or deployment assumptions
- planner, coprocessor, or pushdown strategy details
- temporal semantics beyond the existing first `date32` checkpoint

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

Harnesses should submit one documented first-temporal case at a time to the
TiKV adapter with the same minimal request fields already used by TiDB and
TiFlash:

- `slice_id`
- `case_id`
- `spec_refs[]`
- `input_ref`
- exactly one operation reference:
  - `projection_ref` for passthrough `column(index)` cases
  - `filter_ref` for `is_not_null(column(index))` cases

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
- exactly one operation reference matching the request (`projection_ref` or
  `filter_ref`)
- `outcome.kind = rows` or `error`

When `outcome.kind = rows`, the record includes:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` in normalized JSON scalar form plus `null`
- `row_count`

When `outcome.kind = error`, the record includes:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this first TiKV temporal checkpoint, the normalized error vocabulary is
unchanged:

- `missing_column`
- `unsupported_temporal_type`
- `adapter_unavailable`
- `engine_error`

## First Checkpoint Expectations

- issue #266 adds deterministic TiKV adapter-core coverage for every
  documented first-temporal `case_id`
- issue #266 adds a deterministic TiKV single-engine harness carrier at
  `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv.rs`
- issue #266 lands first TiKV temporal compatibility notes and normalized
  `case-results` inventory artifacts
- issue #270 adds deterministic TiKV pairwise drift rendering at
  `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv_pairwise.rs`
- issue #270 lands paired TiDB-versus-TiKV and TiFlash-versus-TiKV temporal
  drift-report artifacts under `inventory/`
- normalized field meanings stay aligned with
  `tests/differential/first-temporal-date32-slice-artifacts.md`

## Follow-On Boundary

After this request/response plus single-engine and pairwise executable
checkpoint, follow-on issues may separately define:

- live TiKV runner wiring and refresh workflow for this slice

## Result

TiKV now has a concrete docs-first request and response boundary plus
executable single-engine and pairwise first-temporal checkpoints while
preserving the shared first differential temporal checkpoint as
TiDB-versus-TiFlash.
