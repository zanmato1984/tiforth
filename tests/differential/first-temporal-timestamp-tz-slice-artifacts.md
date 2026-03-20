# First Temporal `timestamp_tz(us)` Slice Artifact Carriers

Status: issue #280 design checkpoint, issue #290 TiKV boundary checkpoint, issue #298 artifact-carrier checkpoint, issue #304 harness checkpoint

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #280 `design: define first timezone-aware timestamp semantic slice checkpoint`
- #288 `kernel: execute first timestamp_tz(us) local conformance slice`
- #290 `design: define TiKV adapter boundary for first-temporal-timestamp-tz-slice`
- #298 `docs: define first-temporal-timestamp-tz differential artifact carriers`
- #304 `harness: execute first timestamp_tz(us) differential artifacts`

## Purpose

This note defines the stable differential artifact carriers for
`first-temporal-timestamp-tz-slice` from
`tests/differential/first-temporal-timestamp-tz-slice.md`.

The goal is to keep artifact shape under source-of-truth harness docs while
checked-in `inventory/` files remain reviewable evidence instead of schema
authority.

The minimal adapter request and response boundary that feeds these artifacts is
defined in `adapters/first-temporal-timestamp-tz-slice.md` plus
`adapters/first-temporal-timestamp-tz-slice-tikv.md`.

## Artifact Set

The first executable timestamp-timezone differential checkpoint should produce
four checked-in artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one aggregated TiDB-versus-TiFlash `drift-report`
4. one machine-readable TiDB-versus-TiFlash `drift-report` sidecar

Current artifact filenames for this slice:

- `inventory/first-temporal-timestamp-tz-slice-tidb-case-results.json`
- `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json`
- `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tiflash-drift-report.json`

## `case-results` Artifact Shape

Each per-engine artifact should record at least:

- top-level `slice_id`
- top-level `engine`
- top-level `adapter`
- `cases[]`, where each case entry includes:
  - `slice_id`
  - `engine`
  - `adapter`
  - `case_id`
  - `spec_refs[]`
  - `input_ref`
  - exactly one operation reference: `projection_ref`, `filter_ref`, or
    `ordering_ref`
  - `outcome.kind` = `rows` or `error`

When `outcome.kind = rows`, include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using normalized UTC epoch-microsecond integer scalars plus `null`
- `row_count`

For this slice, equivalent instants represented with different offsets should
normalize to the same UTC epoch-microsecond integer value.

When `outcome.kind = error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this slice, `error_class` must stay stable enough for:

- `missing_column`
- `unsupported_temporal_type`
- `unsupported_temporal_unit`
- `adapter_unavailable`
- `engine_error`

## `drift-report` Carrier For This Slice

The shared cross-slice drift-report carrier now lives in:

- `tests/differential/drift-report-carrier.md`

The first timestamp-timezone checkpoint should use that shared carrier without
adding extra status values.

For this slice, `comparison_dimensions[]` should only use dimensions that the
slice actually compares:

- `field_name`
- `field_nullability`
- `logical_type`
- `row_count`
- `row_values`
- `error_class`

For this slice, `unsupported` should stay limited to explicit adapter or
engine-path gaps for already-documented first timestamp-timezone cases, and
each `unsupported` record should include a concrete `follow_up`.

The machine-readable sidecar should mirror the shared carrier fields
(`slice_id`, `engines[]`, `spec_refs[]`, and `cases[]`) used by the paired
Markdown report.

## Inventory Refresh Boundary

Issue #304 adds executable fixture-runner wiring and checks in the first
`first-temporal-timestamp-tz-slice` differential artifacts listed above.

Follow-on PRs should refresh those artifacts when slice semantics, case IDs,
normalized fields, or drift conclusions change under
`docs/process/inventory-refresh.md`.

## Boundary For Now

The first timestamp-timezone artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the first TiDB-versus-TiFlash pair
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
- TiKV single-engine and pairwise timestamp-timezone artifact expansion
