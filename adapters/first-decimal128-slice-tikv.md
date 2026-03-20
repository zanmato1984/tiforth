# First TiKV Decimal `decimal128` Adapter Boundary

Status: issue #278 design checkpoint

Verified: 2026-03-20

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #189 `design: define first decimal semantic slice boundary`
- #206 `harness: execute first-decimal128-slice differential artifacts for TiDB and TiFlash`
- #278 `design: define first TiKV decimal128 adapter request/response surface`

## Purpose

This note defines the first TiKV-specific adapter boundary for the existing
`first-decimal128-slice` semantic checkpoint.

The goal is to make TiKV request and response expectations explicit for harness
and follow-on adapter work without changing shared slice semantics or widening
decimal families.

## Scope

This boundary applies only to:

- `slice_id = first-decimal128-slice`
- engine: `tikv`
- case families already documented in
  `tests/differential/first-decimal128-slice.md`

It does not define:

- TiKV connection provisioning, cluster topology, or deployment assumptions
- planner, coprocessor, or pushdown strategy details
- decimal semantics beyond the existing first `decimal128` checkpoint
- executable TiKV decimal artifact refresh workflows

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

Harnesses should submit one documented first-decimal case at a time to the
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

For this first TiKV decimal checkpoint, the normalized error vocabulary is
unchanged:

- `missing_column`
- `unsupported_decimal_type`
- `invalid_decimal_metadata`
- `adapter_unavailable`
- `engine_error`

## First Checkpoint Expectations

- issue #278 defines docs-first TiKV request and response ownership for every
  documented first-decimal `case_id`
- normalized field meanings stay aligned with
  `tests/differential/first-decimal128-slice-artifacts.md`
- executable TiKV decimal adapter, harness, and inventory checkpoints remain
  follow-on scope

## Follow-On Boundary

After this docs-first request/response checkpoint, follow-on issues may
separately define:

- deterministic TiKV single-engine harness execution for
  `first-decimal128-slice`
- checked-in TiKV compatibility notes and single-engine `case-results`
  artifacts for this slice
- deterministic TiKV pairwise drift rendering and artifact refresh workflow
  against `tidb` and `tiflash`

## Result

TiKV now has a concrete docs-first request and response boundary for first
decimal `decimal128` checkpoints while preserving the shared first differential
decimal checkpoint as TiDB-versus-TiFlash.
