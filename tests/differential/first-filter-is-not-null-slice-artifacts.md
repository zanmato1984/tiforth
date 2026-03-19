# First Filter Slice Artifact Carriers

Status: issue #147 design checkpoint, issue #153 harness checkpoint

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #147 `design: define first differential filter slice and adapter boundary for is_not_null`
- #153 `harness: execute first-filter-is-not-null differential artifacts for TiDB and TiFlash`

## Purpose

This note defines the stable checked-in artifact carriers for the first
differential filter slice from
`tests/differential/first-filter-is-not-null-slice.md`.

The goal is to keep the artifact shape under source-of-truth harness docs while
the checked-in `inventory/` files remain reviewable evidence rather than the
place where the schema is defined.

The minimal adapter request and response boundary that feeds these artifacts is
defined in `adapters/first-filter-is-not-null-slice.md`.

## Artifact Set

The first executable differential filter checkpoint produces three checked-in
artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one aggregated TiDB-versus-TiFlash `drift-report`

These carriers should stay simple and JSON-serializable at the record level
even when the drift report also renders a human-readable Markdown summary.

Current checked-in examples:

- `inventory/first-filter-is-not-null-slice-tidb-case-results.json`
- `inventory/first-filter-is-not-null-slice-tiflash-case-results.json`
- `inventory/first-filter-is-not-null-slice-tidb-vs-tiflash-drift-report.md`

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
  - `filter_ref`
  - `outcome.kind` = `rows` or `error`

When `outcome.kind = rows`, include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using JSON scalars plus `null`
- `row_count`

When `outcome.kind = error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this slice, `error_class` must stay stable enough for:

- `missing_column`
- `unsupported_predicate_type`
- `adapter_unavailable`
- `engine_error`

## `drift-report` Carrier For This Slice

The shared cross-slice drift-report carrier now lives in:

- `tests/differential/drift-report-carrier.md`

The first-filter checkpoint uses that shared carrier without adding extra status
values.

For this slice, `comparison_dimensions[]` should only use dimensions that the
slice actually compares:

- `field_name`
- `field_nullability`
- `logical_type`
- `row_count`
- `row_values`
- `error_class`

For this slice, `unsupported` should stay limited to explicit adapter or
engine-path gaps for already-documented first-slice cases, and each
`unsupported` record should include a concrete `follow_up`.

## Boundary For Now

The first filter artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the first TiDB-versus-TiFlash pair
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
