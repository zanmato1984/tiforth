# First TiKV Temporal `date32` Adapter Boundary

Status: issue #264 design checkpoint

Verified: 2026-03-20

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #264 `design: define first TiKV temporal date32 adapter request/response surface`

## Purpose

This note defines the first TiKV-specific adapter boundary for the existing
`first-temporal-date32-slice` semantic checkpoint.

The goal is to make TiKV request and response expectations explicit for future
harness and adapter work without changing shared slice semantics or widening
temporal families.

## Scope

This boundary applies only to:

- `slice_id = first-temporal-date32-slice`
- engine: `tikv`
- case families already documented in
  `tests/differential/first-temporal-date32-slice.md`

It does not define:

- executable TiKV temporal adapter behavior in crates
- TiKV temporal `case-results` or pairwise drift-report artifacts
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

Each TiKV adapter invocation should return one normalized `case result` record
with at least:

- `slice_id`
- `engine = tikv`
- `adapter`
- `case_id`
- `spec_refs[]`
- `input_ref`
- exactly one operation reference matching the request (`projection_ref` or
  `filter_ref`)
- `outcome.kind = rows` or `error`

When `outcome.kind = rows`, the record should include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` in normalized JSON scalar form plus `null`
- `row_count`

When `outcome.kind = error`, the record should include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this first TiKV temporal boundary, the normalized error vocabulary is
unchanged:

- `missing_column`
- `unsupported_temporal_type`
- `adapter_unavailable`
- `engine_error`

## First Checkpoint Expectations

- this issue is docs-first only; executable TiKV temporal behavior remains
  follow-on scope
- until executable support lands, documented cases may legitimately normalize to
  `adapter_unavailable` instead of being omitted
- normalized field meanings stay aligned with
  `tests/differential/first-temporal-date32-slice-artifacts.md`

## Follow-On Boundary

After this docs-first checkpoint, follow-on issues may separately define:

- executable TiKV single-engine temporal adapter coverage
- checked-in TiKV temporal compatibility-notes and `case-results` artifacts
- TiKV pairwise drift-report policy and checked-in artifacts for this slice

## Result

TiKV now has a concrete docs-first request and response boundary for
`first-temporal-date32-slice` while preserving the current executable temporal
differential checkpoint as TiDB-versus-TiFlash.
