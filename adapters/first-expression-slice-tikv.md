# First TiKV Expression Adapter Boundary

Status: issue #218 design checkpoint

Verified: 2026-03-19

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #72 `design: define first differential adapter request/response surface`
- #88 `design: define adapter milestone breakdown for first differential slice`
- #113 `harness: compare first-expression-slice results for TiDB and TiFlash`
- #218 `design: define first TiKV differential adapter request/response surface`
- #220 `adapter: execute first-expression-slice through TiKV`
- #228 `inventory: add first-expression-slice TiKV compatibility notes checkpoint`

## Purpose

This note defines the first TiKV-specific adapter boundary for the existing
`first-expression-slice` semantic checkpoint.

The goal is to make TiKV request and response expectations explicit for harness
work without changing shared slice semantics or widening expression families.

## Scope

This boundary applies only to:

- `slice_id = first-expression-slice`
- engine: `tikv`
- case family already documented in
  `tests/differential/first-expression-slice.md`

It does not define:

- pairwise TiKV-versus-TiDB or TiKV-versus-TiFlash drift-report policy
- TiKV connection provisioning, cluster topology, or deployment assumptions
- planner, coprocessor, or pushdown strategy details
- expanded function or operator families beyond the current first slice

## Shared Ownership

Shared differential docs still own:

- stable `slice_id`, `case_id`, `input_ref`, and `projection_ref`
- semantic meaning for each first-slice case
- normalized `case result` carrier fields and drift vocabulary

The TiKV adapter boundary owns:

- TiKV-native request derivation from shared refs
- TiKV execution mechanics and session policy
- translation from TiKV-native rows or failures into normalized case results

This keeps shared semantics centralized while adapter execution remains
engine-local.

## Request Surface

Harnesses submit one documented first-slice case at a time to the TiKV adapter
with the same minimal request fields already used by TiDB and TiFlash:

- `slice_id`
- `case_id`
- `spec_refs[]`
- `input_ref`
- `projection_ref`

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
- `projection_ref`
- `outcome.kind = rows` or `error`

When `outcome.kind = rows`, the record includes:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` in normalized JSON scalar form plus `null`
- `row_count`

When `outcome.kind = error`, the record includes:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this first TiKV checkpoint, the normalized error vocabulary is unchanged:

- `arithmetic_overflow`
- `adapter_unavailable`
- `engine_error`

## First Checkpoint Expectations

- a TiKV adapter may return `adapter_unavailable` for documented first-slice
  cases while implementation is still incomplete
- unsupported cases should still emit explicit `case result` records, not
  silent omission
- normalized field meanings stay aligned with
  `tests/differential/first-expression-slice-artifacts.md`

## Follow-On Boundary

After this first request/response checkpoint is executable, follow-on issues may
separately define:

- TiKV pairwise drift aggregation rules
- checked-in TiKV single-engine `case-results` artifacts for
  `first-expression-slice`
- broader expression families or error normalization

Issue #228 now lands the first TiKV compatibility-note inventory checkpoint in
`inventory/first-expression-slice-tikv-compat-notes.md`.

## Result

TiKV now has a concrete docs-first request and response boundary for the
existing `first-expression-slice` harness surface, issue #220's first
executable single-engine adapter core in `crates/tiforth-adapter-tikv`, and
issue #228's first checked-in TiKV compatibility-notes artifact in
`inventory/first-expression-slice-tikv-compat-notes.md`.

Pairwise TiKV drift policy and checked-in TiKV `case-results` artifacts remain
follow-on work.
