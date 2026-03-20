# First TiKV Temporal `timestamp_tz(us)` Adapter Boundary

Status: issue #290 design checkpoint, issue #306 executable checkpoint

Verified: 2026-03-20

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #280 `design: define first timezone-aware timestamp semantic slice checkpoint`
- #288 `kernel: execute first timestamp_tz(us) local conformance slice`
- #290 `design: define TiKV adapter boundary for first-temporal-timestamp-tz-slice`
- #306 `checkpoint: implement TiKV first-temporal-timestamp-tz executable differential slice`

## Purpose

This note defines the TiKV-specific adapter boundary for the
`first-temporal-timestamp-tz-slice` semantic checkpoint and records the first
executable TiKV checkpoint that conforms to that boundary.

The goal is to keep shared slice semantics stable while making TiKV request,
normalization, and artifact expectations explicit.

## Scope

This boundary applies only to:

- `slice_id = first-temporal-timestamp-tz-slice`
- engine: `tikv`
- case families already documented in
  `tests/differential/first-temporal-timestamp-tz-slice.md`

It does not define:

- TiKV connection provisioning, cluster topology, or deployment assumptions
- planner, coprocessor, or pushdown strategy details
- temporal arithmetic, cast, extraction, truncation, or interval behavior
- timezone-name canonicalization or timezone-database negotiation
- live-runner orchestration for environment-backed refresh

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

Harnesses submit one documented first timestamp-timezone case at a time to the
TiKV adapter with the same minimal request fields used by TiDB and TiFlash:

- `slice_id`
- `case_id`
- `spec_refs[]`
- `input_ref`
- exactly one operation reference:
  - `projection_ref` for passthrough `column(index)` cases
  - `filter_ref` for `is_not_null(column(index))` cases
  - `ordering_ref` for ordering-probe cases

The request does not carry engine-native query text, planner directives,
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
- exactly one operation reference matching the request
  (`projection_ref`, `filter_ref`, or `ordering_ref`)
- `outcome.kind = rows` or `error`

When `outcome.kind = rows`, the record includes:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` in normalized JSON numeric scalar form plus `null`
- `row_count`

For this slice, non-null `timestamp_tz(us)` row values normalize to signed UTC
epoch-microsecond integers so equivalent instants compare equal across engines.

When `outcome.kind = error`, the record includes:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this checkpoint, the normalized error vocabulary stays:

- `missing_column`
- `unsupported_temporal_type`
- `unsupported_temporal_unit`
- `adapter_unavailable`
- `engine_error`

## Executable Checkpoint Evidence

Issue #306 adds deterministic TiKV executable coverage on top of this boundary:

- adapter implementation in
  `crates/tiforth-adapter-tikv/src/first_temporal_timestamp_tz_slice.rs`
- single-engine harness execution in
  `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv.rs`
- pairwise TiDB-vs-TiKV and TiFlash-vs-TiKV drift rendering in
  `crates/tiforth-harness-differential/src/first_temporal_timestamp_tz_slice_tikv_pairwise.rs`
- checked-in inventory artifacts:
  - `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json`
  - `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tikv-drift-report.md`
  - `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tikv-drift-report.json`
  - `inventory/first-temporal-timestamp-tz-slice-tiflash-vs-tikv-drift-report.md`
  - `inventory/first-temporal-timestamp-tz-slice-tiflash-vs-tikv-drift-report.json`
  - `inventory/first-temporal-timestamp-tz-slice-tikv-compat-notes.md`

## Follow-On Boundary

Follow-on issues may still separately define:

- live TiKV timestamp-timezone runner wiring and environment-backed refresh
  workflow
- broader temporal-family semantics beyond `timestamp_tz(us)`

## Result

TiKV now has both a stable docs-defined request/response boundary and the first
executable single-engine plus pairwise differential checkpoint for
`first-temporal-timestamp-tz-slice`, while broader temporal-family expansion
remains follow-on scope.
