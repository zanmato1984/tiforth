# First TiKV Union Adapter Boundary

Status: issue #368 executable checkpoint, issue #370 compatibility-notes checkpoint, issue #372 live-runner-boundary checkpoint, issue #374 live-runner executable checkpoint

Verified: 2026-03-21

Related issues:

- #241 `docs: define first union nested handoff slice checkpoint`
- #340 `docs: define first-union-slice differential artifact carriers`
- #366 `harness: execute first-union-slice differential artifacts`
- #368 `harness: add TiKV first-union-slice executable checkpoints`
- #370 `inventory: add first-union-slice TiKV compatibility notes checkpoint`
- #372 `design: define live TiKV runner boundary for first-union-slice`
- #374 `harness: implement first-union-slice live TiKV runner and refresh workflow`

## Purpose

This note defines the first TiKV-specific adapter boundary for the existing
`first-union-slice` semantic checkpoint.

The goal is to make TiKV request and response expectations explicit for harness
and follow-on adapter work without changing shared union slice semantics.

## Scope

This boundary applies only to:

- `slice_id = first-union-slice`
- engine: `tikv`
- case family already documented in `tests/differential/first-union-slice.md`

It does not define:

- TiKV connection provisioning, cluster topology, or deployment assumptions
- planner, coprocessor, or pushdown strategy details
- nested predicate behavior (`is_not_null(column(index))`) over union
- broader nested union modes (`sparse_union`) or nested-family expansion

## Shared Ownership

Shared differential docs still own:

- stable `slice_id`, `case_id`, `input_ref`, and operation refs
- semantic meaning for each first-union case
- normalized `case result` carrier fields and error vocabulary for this slice

The TiKV adapter boundary owns:

- TiKV-native request derivation from shared refs
- TiKV execution mechanics and session policy
- translation from TiKV-native rows or failures into normalized case results

This keeps shared semantics centralized while adapter execution remains
engine-local.

## Request Surface

Harnesses submit one documented first-union case at a time to the TiKV adapter
with the same minimal request fields used by other engines:

- `slice_id`
- `case_id`
- `spec_refs[]`
- `input_ref`
- exactly one operation reference:
  - `projection_ref` for passthrough `column(index)` probes

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
- `rows[]` using canonical union carrier objects with stable keys (`tag`, then
  `value`)
- `row_count`

When `outcome.kind = error`, the record includes:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this first TiKV union checkpoint, the normalized error vocabulary is
unchanged:

- `missing_column`
- `unsupported_nested_family`
- `adapter_unavailable`
- `engine_error`

## First Checkpoint Expectations

- issue #368 adds deterministic TiKV adapter-core coverage for every
  documented first-union `case_id`
- issue #368 adds a deterministic TiKV single-engine harness carrier at
  `crates/tiforth-harness-differential/src/first_union_slice_tikv.rs`
- issue #368 adds deterministic TiKV pairwise drift rendering at
  `crates/tiforth-harness-differential/src/first_union_slice_tikv_pairwise.rs`
- issue #368 lands checked-in TiKV first-union artifacts:
  - `inventory/first-union-slice-tikv-case-results.json`
  - `inventory/first-union-slice-tidb-vs-tikv-drift-report.md`
  - `inventory/first-union-slice-tidb-vs-tikv-drift-report.json`
  - `inventory/first-union-slice-tiflash-vs-tikv-drift-report.md`
  - `inventory/first-union-slice-tiflash-vs-tikv-drift-report.json`
- issue #370 lands the first TiKV union compatibility notes artifact at
  `inventory/first-union-slice-tikv-compat-notes.md`
- issue #372 defines the docs-first live TiKV runner boundary in
  `docs/design/first-union-slice-tikv-live-runner-boundary.md`
- issue #374 lands executable live-runner wiring in
  `crates/tiforth-harness-differential/src/first_union_slice_tikv_live.rs`,
  `crates/tiforth-harness-differential/src/bin/first_union_slice_tikv_live.rs`,
  and `scripts/refresh-first-union-tikv-live-artifacts.sh`
- normalized field meanings stay aligned with
  `tests/differential/first-union-slice-artifacts.md`

## Follow-On Boundary

After issue #374, follow-on issues may separately define:

- shared-review environment refresh cadence for checked-in first-union TiKV
  artifacts
- live-runner expansion to temporal, decimal, or later accepted slice families

## Result

TiKV now has a concrete docs-first request and response boundary, executable
single-engine and pairwise first-union checkpoints, and executable live-runner
wiring while preserving the shared first differential union checkpoint
semantics.
