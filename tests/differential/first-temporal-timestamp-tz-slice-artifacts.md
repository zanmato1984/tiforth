# First Temporal `timestamp_tz(us)` Slice Artifact Carriers

Status: issue #280 design checkpoint, issue #290 TiKV boundary checkpoint, issue #298 artifact-carrier checkpoint, issue #304 harness checkpoint, issue #306 TiKV executable artifact checkpoint

Related issues:

- #174 `design: define first temporal semantic slice boundary`
- #176 `docs: define first temporal date32 coverage and adapter checkpoints`
- #280 `design: define first timezone-aware timestamp semantic slice checkpoint`
- #288 `kernel: execute first timestamp_tz(us) local conformance slice`
- #290 `design: define TiKV adapter boundary for first-temporal-timestamp-tz-slice`
- #298 `docs: define first-temporal-timestamp-tz differential artifact carriers`
- #304 `harness: execute first timestamp_tz(us) differential artifacts`
- #306 `checkpoint: implement TiKV first-temporal-timestamp-tz executable differential slice`

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

The current executable timestamp-timezone checkpoint now produces nine
checked-in artifacts:

1. normalized TiDB `case-results`
2. normalized TiFlash `case-results`
3. normalized TiKV `case-results`
4. TiDB-versus-TiFlash `drift-report`
5. TiDB-versus-TiFlash machine-readable sidecar
6. TiDB-versus-TiKV `drift-report`
7. TiDB-versus-TiKV machine-readable sidecar
8. TiFlash-versus-TiKV `drift-report`
9. TiFlash-versus-TiKV machine-readable sidecar

Current artifact filenames for this slice:

- `inventory/first-temporal-timestamp-tz-slice-tidb-case-results.json`
- `inventory/first-temporal-timestamp-tz-slice-tiflash-case-results.json`
- `inventory/first-temporal-timestamp-tz-slice-tikv-case-results.json`
- `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tiflash-drift-report.json`
- `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tikv-drift-report.md`
- `inventory/first-temporal-timestamp-tz-slice-tidb-vs-tikv-drift-report.json`
- `inventory/first-temporal-timestamp-tz-slice-tiflash-vs-tikv-drift-report.md`
- `inventory/first-temporal-timestamp-tz-slice-tiflash-vs-tikv-drift-report.json`

## `case-results` Artifact Shape

Each per-engine artifact records at least:

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

For this slice, equivalent instants represented with different offsets
normalize to the same UTC epoch-microsecond integer value.

When `outcome.kind = error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this slice, `error_class` stays stable enough for:

- `missing_column`
- `unsupported_temporal_type`
- `unsupported_temporal_unit`
- `adapter_unavailable`
- `engine_error`

## `drift-report` Carrier For This Slice

The shared cross-slice drift-report carrier lives in:

- `tests/differential/drift-report-carrier.md`

This timestamp-timezone checkpoint uses that shared carrier without adding extra
status values.

For this slice, `comparison_dimensions[]` only uses dimensions that the slice
actually compares:

- `field_name`
- `field_nullability`
- `logical_type`
- `row_count`
- `row_values`
- `error_class`

For this slice, `unsupported` stays limited to explicit adapter or engine-path
gaps for already documented timestamp-timezone cases, and each `unsupported`
record includes a concrete `follow_up`.

The machine-readable sidecar mirrors the shared carrier fields (`slice_id`,
`engines[]`, `spec_refs[]`, and `cases[]`) used by the paired Markdown report.

## Inventory Refresh Boundary

Issue #304 adds executable fixture-runner wiring and checked-in TiDB-versus-
TiFlash artifacts for this slice.

Issue #306 extends that executable checkpoint with TiKV single-engine artifacts
plus TiDB-versus-TiKV and TiFlash-versus-TiKV pairwise drift artifacts.

Follow-on PRs should refresh these artifacts when slice semantics, case IDs,
normalized fields, or drift conclusions change under
`docs/process/inventory-refresh.md`.

## Boundary For Now

The first timestamp-timezone artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond explicit two-engine pairwise reports
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
