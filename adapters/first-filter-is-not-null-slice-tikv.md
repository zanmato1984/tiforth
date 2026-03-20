# First TiKV Filter Adapter Boundary

Status: issue #247 design checkpoint, issue #249 executable checkpoint

Verified: 2026-03-20

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #147 `design: define first differential filter slice and adapter boundary for is_not_null`
- #153 `harness: execute first-filter-is-not-null differential artifacts for TiDB and TiFlash`
- #247 `design: define first TiKV differential filter adapter request/response surface`
- #249 `adapter: execute first-filter-is-not-null-slice through TiKV`

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

- issue #249 adds deterministic TiKV adapter-core coverage for every
  documented first-filter `case_id`
- issue #249 adds a deterministic TiKV single-engine harness carrier at
  `crates/tiforth-harness-differential/src/first_filter_is_not_null_slice_tikv.rs`
- issue #249 lands first TiKV filter compatibility notes and normalized
  `case-results` inventory artifacts
- normalized field meanings stay aligned with
  `tests/differential/first-filter-is-not-null-slice-artifacts.md`

## Follow-On Boundary

After this first request/response plus single-engine executable checkpoint,
follow-on issues may separately define:

- TiKV pairwise drift-report policy and checked-in artifacts for this slice
- live TiKV runner wiring and refresh workflow for this slice

## Result

TiKV now has a concrete docs-first request and response boundary plus an
executable single-engine first-filter checkpoint while preserving the shared
first differential filter checkpoint as TiDB-versus-TiFlash.
