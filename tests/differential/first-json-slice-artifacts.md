# First JSON Slice Artifact Carriers

Status: issue #224 design checkpoint, issue #272 artifact-carrier checkpoint, issue #356 executable artifact checkpoint

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #224 `design: define first JSON semantic slice boundary`
- #272 `docs: define first-json-slice differential artifact carriers`
- #356 `harness: execute first-json-slice differential artifacts`

## Purpose

This note defines the stable differential artifact carriers for
`first-json-slice` from `tests/differential/first-json-slice.md`.

The goal is to keep artifact shape under source-of-truth harness docs while
checked-in `inventory/` files remain reviewable evidence instead of schema
authority.

The minimal adapter request and response boundary that feeds these artifacts is
defined in `adapters/first-json-slice.md`.

## Artifact Set

The first executable JSON differential checkpoint should produce four checked-in
artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one aggregated TiDB-versus-TiFlash `drift-report`
4. one machine-readable TiDB-versus-TiFlash `drift-report` sidecar

Artifact filenames for this slice:

- `inventory/first-json-slice-tidb-case-results.json`
- `inventory/first-json-slice-tiflash-case-results.json`
- `inventory/first-json-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-json-slice-tidb-vs-tiflash-drift-report.json`

Issue #272 is docs-first only and does not add or refresh those `inventory/`
files.

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
  - exactly one operation reference: `projection_ref`, `filter_ref`,
    `comparison_ref`, or `cast_ref`
  - `outcome.kind` = `rows` or `error`

When `outcome.kind = rows`, include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using canonical JSON value-token strings plus carrier `null`
- `row_count`

For this slice, non-null JSON values should be normalized to canonical JSON
value tokens carried as JSON strings. SQL `NULL` should remain carrier `null`.
This preserves the shared distinction between SQL `NULL` and JSON literal
`null`.

When `outcome.kind = error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this slice, `error_class` must stay stable enough for:

- `missing_column`
- `unsupported_json_comparison`
- `unsupported_json_cast`
- `adapter_unavailable`
- `engine_error`

## `drift-report` Carrier For This Slice

The shared cross-slice drift-report carrier now lives in:

- `tests/differential/drift-report-carrier.md`

The first JSON checkpoint should use that shared carrier without adding extra
status values.

For this slice, `comparison_dimensions[]` should only use dimensions that the
slice actually compares:

- `field_name`
- `field_nullability`
- `logical_type`
- `row_count`
- `row_values`
- `error_class`

For this slice, `unsupported` should stay limited to explicit adapter or
engine-path gaps for already-documented first-JSON cases, and each
`unsupported` record should include a concrete `follow_up`.

The machine-readable sidecar should mirror the shared carrier fields
(`slice_id`, `engines[]`, `spec_refs[]`, and `cases[]`) used by the paired
Markdown report.

## Inventory Refresh Boundary

Issue #356 now lands executable runner wiring for this slice through `crates/tiforth-adapter-tidb/src/first_json_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_json_slice.rs`, `crates/tiforth-harness-differential/src/first_json_slice.rs`, and `crates/tiforth-harness-differential/src/bin/first_json_slice.rs`, and refreshes the four checked-in `inventory/first-json-slice-*` artifacts listed above.

Follow-on PRs that change first-json semantics, case identifiers, adapter normalization, or drift-comparison policy should refresh those artifacts and declare `Inventory-Impact: updated`.

## Boundary For Now

The first JSON artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the first TiDB-versus-TiFlash pair
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
- successful explicit JSON cast semantics, JSON ordering semantics, or JSON path/operator artifact families
