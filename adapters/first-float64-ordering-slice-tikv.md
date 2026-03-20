# First TiKV Float64 Ordering Adapter Boundary

Status: issue #286 design checkpoint, issue #292 executable single-engine checkpoint

Verified: 2026-03-20

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #194 `design: define first float64 NaN, infinity, and ordering checkpoint`
- #208 `harness: execute first-float64-ordering-slice differential artifacts for TiDB and TiFlash`
- #286 `design: define TiKV adapter boundary for first-float64-ordering-slice`
- #292 `harness: execute first-float64-ordering-slice TiKV single-engine artifacts`

## Purpose

This note defines the first TiKV-specific adapter boundary for the existing
`first-float64-ordering-slice` semantic checkpoint.

The goal is to make TiKV request and response expectations explicit for harness
and follow-on adapter work without changing shared slice semantics or widening
floating-point families.

## Scope

This boundary applies only to:

- `slice_id = first-float64-ordering-slice`
- engine: `tikv`
- case families already documented in
  `tests/differential/first-float64-ordering-slice.md`

It does not define:

- TiKV pairwise drift artifacts for this slice
- TiKV connection provisioning, cluster topology, or deployment assumptions
- planner, coprocessor, or pushdown strategy details
- float arithmetic, cast, coercion, or SQL `ORDER BY` semantics

## Shared Ownership

Shared differential docs still own:

- stable `slice_id`, `case_id`, `input_ref`, operation refs, and
  `comparison_mode`
- semantic meaning for each first-slice case
- normalized `case result` carrier fields and error vocabulary for this slice

The TiKV adapter boundary owns:

- TiKV-native request derivation from shared refs
- TiKV execution mechanics and session policy
- translation from TiKV-native rows or failures into normalized case results

This keeps shared semantics centralized while adapter execution remains
engine-local.

## Request Surface

Harnesses should submit one documented first-float64 case at a time to the
TiKV adapter with the same minimal request fields already used by TiDB and
TiFlash:

- `slice_id`
- `case_id`
- `spec_refs[]`
- `input_ref`
- `comparison_mode`
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
- `comparison_mode`
- exactly one operation reference matching the request (`projection_ref` or
  `filter_ref`)
- `outcome.kind = rows` or `error`

When `outcome.kind = rows`, the record includes:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` in canonical float64 token form plus `null`
- `row_count`

For this slice, `rows[]` should normalize float64 values to the same shared
tokens used by TiDB and TiFlash checkpoints:

- `-Infinity`
- `Infinity`
- `NaN`
- finite decimal strings, including `-0.0` and `0.0`

When `outcome.kind = error`, the record includes:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this first TiKV float64 checkpoint, the normalized error vocabulary is
unchanged:

- `missing_column`
- `unsupported_floating_type`
- `adapter_unavailable`
- `engine_error`

## First Checkpoint Expectations

- issue #286 defines docs-first TiKV request and response ownership for every
  documented first-float64 `case_id`
- normalized field meanings stay aligned with
  `tests/differential/first-float64-ordering-slice-artifacts.md`
- issue #292 adds executable TiKV float64 adapter, harness, and inventory
  single-engine checkpoints for this slice

## Follow-On Boundary

After the issue #292 executable single-engine checkpoint, follow-on issues may
separately define:

- deterministic TiKV pairwise drift rendering for `tidb-vs-tikv` and
  `tiflash-vs-tikv`
- live TiKV float64 runner wiring and environment-backed refresh workflow

## Result

TiKV now has a concrete docs-first request and response boundary for
`first-float64-ordering-slice` plus executable single-engine adapter and
inventory coverage. Pairwise TiKV float64 drift artifacts remain follow-on
scope.
