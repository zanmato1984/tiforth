# First Decimal `decimal128` Slice Artifact Carriers

Status: issue #206 artifact-carrier and harness checkpoint

Related issues:

- #189 `design: define first decimal semantic slice boundary`
- #206 `harness: execute first-decimal128-slice differential artifacts for TiDB and TiFlash`

## Purpose

This note defines the stable differential artifact carriers for
`first-decimal128-slice` from
`tests/differential/first-decimal128-slice.md`.

The goal is to keep artifact shape under source-of-truth harness docs while
checked-in `inventory/` files remain reviewable evidence instead of schema
authority.

The minimal adapter request and response boundary that feeds these artifacts is
defined in `adapters/first-decimal128-slice.md`.

## Artifact Set

The first executable decimal differential checkpoint produces four checked-in
artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one aggregated TiDB-versus-TiFlash `drift-report`
4. one machine-readable TiDB-versus-TiFlash `drift-report` sidecar

Current artifact filenames for this slice:

- `inventory/first-decimal128-slice-tidb-case-results.json`
- `inventory/first-decimal128-slice-tiflash-case-results.json`
- `inventory/first-decimal128-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-decimal128-slice-tidb-vs-tiflash-drift-report.json`

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
  - exactly one operation reference: `projection_ref` or `filter_ref`
  - `outcome.kind` = `rows` or `error`

When `outcome.kind = rows`, include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using JSON strings plus `null`
- `row_count`

For this slice, decimal rows should be normalized as canonical decimal strings
(for example, `"2.50"`) so cross-engine comparison does not rely on floating
conversion.

When `outcome.kind = error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this slice, `error_class` must stay stable enough for:

- `missing_column`
- `unsupported_decimal_type`
- `invalid_decimal_metadata`
- `adapter_unavailable`
- `engine_error`

## `drift-report` Carrier For This Slice

The shared cross-slice drift-report carrier now lives in:

- `tests/differential/drift-report-carrier.md`

The first-decimal checkpoint should use that shared carrier without adding
extra status values.

For this slice, `comparison_dimensions[]` should only use dimensions that the
slice actually compares:

- `field_name`
- `field_nullability`
- `logical_type`
- `row_count`
- `row_values`
- `error_class`

For this slice, `unsupported` should stay limited to explicit adapter or
engine-path gaps for already-documented first-decimal cases, and each
`unsupported` record should include a concrete `follow_up`.

The machine-readable sidecar should mirror the shared carrier fields
(`slice_id`, `engines[]`, `spec_refs[]`, and `cases[]`) used by the paired
Markdown report.

## Inventory Refresh Boundary

Issue #206 adds executable fixture-runner wiring and checks in the first
`first-decimal128-slice` artifacts listed above.

Follow-on PRs should refresh those artifacts when slice semantics, case IDs,
normalized fields, or drift conclusions change under
`docs/process/inventory-refresh.md`.

## Boundary For Now

The first decimal artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the first TiDB-versus-TiFlash pair
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
- decimal arithmetic, cast, coercion, or rounding evidence
