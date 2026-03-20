# First TiKV Unsigned Arithmetic Adapter Boundary

Status: issue #324 design checkpoint

Verified: 2026-03-20

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #141 `spec: define signed/unsigned interaction checkpoint for initial coercion lattice`
- #276 `spec: define overflow behavior checkpoint for follow-on operator families`
- #300 `design: define first unsigned arithmetic semantic slice boundary`
- #302 `docs: define first-unsigned-arithmetic-slice differential artifact carriers`
- #310 `milestone-1: execute first unsigned arithmetic differential slice`
- #324 `design: define TiKV adapter boundary for first-unsigned-arithmetic-slice`

## Purpose

This note defines the first TiKV-specific adapter boundary for the existing
`first-unsigned-arithmetic-slice` semantic checkpoint.

The goal is to make TiKV request and response expectations explicit for
follow-on harness and adapter work without changing shared unsigned semantics
or widening unsigned-family coverage.

## Scope

This boundary applies only to:

- `slice_id = first-unsigned-arithmetic-slice`
- engine: `tikv`
- case families already documented in
  `tests/differential/first-unsigned-arithmetic-slice.md`

It does not define:

- TiKV connection provisioning, cluster topology, or deployment assumptions
- planner, coprocessor, or pushdown strategy details
- unsigned families beyond the first `uint64` checkpoint
- executable TiKV unsigned harness coverage or checked-in inventory artifacts
- signed/unsigned coercion expansion beyond the existing first-slice boundary

## Shared Ownership

Shared differential docs still own:

- stable `slice_id`, `case_id`, `input_ref`, operation refs, and
  `comparison_mode`
- semantic meaning for each first-slice unsigned case
- normalized `case result` carrier fields and error vocabulary for this slice

The TiKV adapter boundary owns:

- TiKV-native request derivation from shared refs
- TiKV execution mechanics and session policy
- translation from TiKV-native rows or failures into normalized case results

This keeps shared semantics centralized while adapter execution remains
engine-local.

## Request Surface

Harnesses should submit one documented first-unsigned case at a time to the
TiKV adapter with the same minimal request fields already used by TiDB and
TiFlash:

- `slice_id`
- `case_id`
- `spec_refs[]`
- `input_ref`
- `comparison_mode`
- exactly one operation reference:
  - `projection_ref` for `column(index)`, `literal<uint64>(value)`, and
    `add<uint64>(lhs, rhs)` cases
  - `filter_ref` for `is_not_null(column(index))` cases

The request should not carry engine-native query text, planner directives,
credentials, or expected rows.

## Response Surface

Each TiKV adapter invocation returns one normalized `case result` record with
at least:

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
- `rows[]` using canonical unsigned decimal-string tokens plus `null`
- `row_count`

For this slice, non-null `uint64` values should normalize to canonical base-10
integer strings in `rows[]` so full-range `uint64` values remain exact across
engines.

When `outcome.kind = error`, the record includes:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this first TiKV unsigned arithmetic checkpoint, the normalized error
vocabulary is unchanged:

- `missing_column`
- `unsigned_overflow`
- `mixed_signed_unsigned`
- `unsupported_unsigned_family`
- `adapter_unavailable`
- `engine_error`

## First Checkpoint Expectations

- issue #324 defines docs-first TiKV request and response ownership for every
  documented first-unsigned `case_id`
- normalized field meanings stay aligned with
  `tests/differential/first-unsigned-arithmetic-slice-artifacts.md`
- follow-on executable work may add TiKV adapter, harness, and inventory
  checkpoints for this slice without reopening shared unsigned semantics

## Follow-On Boundary

Follow-on issues may still separately define:

- executable TiKV single-engine unsigned adapter and harness coverage
- executable `tidb-vs-tikv` and `tiflash-vs-tikv` drift rendering for this
  slice
- live TiKV unsigned runner wiring and environment-backed refresh workflow

## Result

TiKV now has a concrete docs-first request and response boundary for
`first-unsigned-arithmetic-slice`, while executable TiKV unsigned checkpoints
remain follow-on scope.
