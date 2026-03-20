# First TiKV Filter Adapter Boundary

Status: issue #247 design checkpoint

Verified: 2026-03-19

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #147 `design: define first differential filter slice and adapter boundary for is_not_null`
- #153 `harness: execute first-filter-is-not-null differential artifacts for TiDB and TiFlash`
- #247 `design: define first TiKV differential filter adapter request/response surface`

## Purpose

This note defines the first TiKV-specific adapter boundary for the existing
`first-filter-is-not-null-slice` semantic checkpoint.

The goal is to make TiKV request and response expectations explicit for harness
and follow-on adapter work without changing shared slice semantics or widening
predicate families.

## Scope

This boundary applies only to:

- `slice_id = first-filter-is-not-null-slice`
- engine: `tikv`
- case family already documented in
  `tests/differential/first-filter-is-not-null-slice.md`

It does not define:

- pairwise TiKV-versus-TiDB or TiKV-versus-TiFlash drift-report policy for this
  slice, which remains follow-on scope
- TiKV connection provisioning, cluster topology, or deployment assumptions
- planner, coprocessor, or pushdown strategy details
- generalized predicate execution beyond `is_not_null(column(index))`

## Shared Ownership

Shared differential docs still own:

- stable `slice_id`, `case_id`, `input_ref`, and `filter_ref`
- semantic meaning for each first-slice case
- normalized `case result` carrier fields and error vocabulary for this slice

The TiKV adapter boundary owns:

- TiKV-native request derivation from shared refs
- TiKV execution mechanics and session policy
- translation from TiKV-native rows or failures into normalized case results

This keeps shared semantics centralized while adapter execution remains
engine-local.

## Request Surface

Harnesses submit one documented first-filter case at a time to the TiKV
adapter with the same minimal request fields already used by TiDB and TiFlash:

- `slice_id`
- `case_id`
- `spec_refs[]`
- `input_ref`
- `filter_ref`

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
- `filter_ref`
- `outcome.kind = rows` or `error`

When `outcome.kind = rows`, the record includes:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` in normalized JSON scalar form plus `null`
- `row_count`

When `outcome.kind = error`, the record includes:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this first TiKV filter checkpoint, the normalized error vocabulary is
unchanged:

- `missing_column`
- `unsupported_predicate_type`
- `adapter_unavailable`
- `engine_error`

## First Checkpoint Expectations

- a TiKV adapter may return `adapter_unavailable` for documented first-filter
  cases while implementation is still incomplete
- unsupported cases should still emit explicit `case result` records, not
  silent omission
- normalized field meanings stay aligned with
  `tests/differential/first-filter-is-not-null-slice-artifacts.md`

## Follow-On Boundary

After this first request and response checkpoint is executable, follow-on
issues may separately define:

- TiKV single-engine executable adapter coverage for the documented first-filter
  case set
- TiKV first-filter compatibility-notes and normalized `case-results`
  inventory checkpoints
- TiKV pairwise drift-report policy and checked-in artifacts for this slice

## Result

TiKV now has a concrete docs-first request and response boundary for the
existing `first-filter-is-not-null-slice` harness surface while preserving the
shared first differential filter checkpoint as TiDB-versus-TiFlash.
