# First TiKV Temporal `date32` Adapter Boundary

Status: issue #264 design checkpoint, issue #266 executable checkpoint, issue #270 pairwise checkpoint, issue #380 live-runner executable checkpoint

Verified: 2026-03-21

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #264 `design: define first TiKV temporal date32 adapter request/response surface`
- #266 `adapter: execute first-temporal-date32-slice through TiKV`
- #270 `harness: add first-temporal-date32-slice TiKV pairwise drift artifacts`
- #376 `design: define live TiKV temporal and decimal runner refresh boundary`
- #380 `harness: implement first-temporal-date32-slice TiKV live runner refresh workflow`

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
- issue #380 lands executable live-runner wiring in
  `crates/tiforth-harness-differential/src/first_temporal_date32_slice_tikv_live.rs`,
  `crates/tiforth-harness-differential/src/bin/first_temporal_date32_slice_tikv_live.rs`,
  and `scripts/refresh-first-temporal-date32-tikv-live-artifacts.sh`
- normalized field meanings stay aligned with
  `tests/differential/first-temporal-date32-slice-artifacts.md`

## Follow-On Boundary

After issue #380, follow-on issues may separately define:

- shared-review environment refresh cadence for checked-in first-temporal-date32
  TiKV artifacts
- live-runner expansion to first-temporal-timestamp-tz checkpoints under
  `docs/design/first-temporal-decimal-slices-tikv-live-runner-boundary.md`
- broader temporal-family adapter coverage beyond first `date32` checkpoints

## Result

TiKV now has a concrete docs-first request and response boundary plus
executable single-engine and pairwise first-temporal checkpoints, and
executable live-runner wiring while preserving the shared first differential
temporal checkpoint semantics.
