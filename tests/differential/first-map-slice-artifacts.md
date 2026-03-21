# First Map Slice Artifact Carriers

Status: issue #230 design checkpoint, issue #338 artifact-carrier checkpoint

Related issues:

- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #151 `design: define first nested-family shared handoff slice and claim-ownership boundary`
- #226 `design: define first struct nested handoff slice checkpoint`
- #230 `docs: define first map nested handoff slice checkpoint`
- #338 `docs: define first-map-slice differential artifact carriers`

## Purpose

This note defines the stable differential artifact carriers for
`first-map-slice` from `tests/differential/first-map-slice.md`.

The goal is to keep artifact shape under source-of-truth harness docs while
checked-in `inventory/` files remain reviewable evidence instead of schema
authority.

The minimal adapter request and response boundary that feeds these artifacts is
defined in `adapters/first-map-slice.md`.

## Artifact Set

The first executable map differential checkpoint should produce four
checked-in artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one aggregated TiDB-versus-TiFlash `drift-report`
4. one machine-readable TiDB-versus-TiFlash `drift-report` sidecar

Planned artifact filenames for this slice:

- `inventory/first-map-slice-tidb-case-results.json`
- `inventory/first-map-slice-tiflash-case-results.json`
- `inventory/first-map-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-map-slice-tidb-vs-tiflash-drift-report.json`

Issue #338 is docs-first only and does not add or refresh those `inventory/`
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
  - `projection_ref`
  - `outcome.kind` = `rows` or `error`

When `outcome.kind = rows`, include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using canonical JSON array carriers plus carrier `null`
- `row_count`

For this slice, each non-null map row should be normalized as one canonical
JSON array of entry objects with stable keys (`key`, then `value`). Entry keys
should use JSON numbers and must not be `null`. Entry values should use JSON
numbers or `null`. SQL `NULL` map rows should stay carrier `null`.

When `outcome.kind = error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this slice, `error_class` must stay stable enough for:

- `missing_column`
- `unsupported_nested_family`
- `adapter_unavailable`
- `engine_error`

## `drift-report` Carrier For This Slice

The shared cross-slice drift-report carrier now lives in:

- `tests/differential/drift-report-carrier.md`

The first map checkpoint should use that shared carrier without adding extra
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
engine-path gaps for already-documented first-map cases, and each
`unsupported` record should include a concrete `follow_up`.

The machine-readable sidecar should mirror the shared carrier fields
(`slice_id`, `engines[]`, `spec_refs[]`, and `cases[]`) used by the paired
Markdown report.

## Inventory Refresh Boundary

This checkpoint documents carriers only.

Follow-on harness work should add executable runner wiring for this slice and
then refresh or check in the planned `inventory/` files using the same carrier
shape defined here.

Until that executable checkpoint lands, PRs that touch this note may declare:

- `Inventory-Impact: none - map artifact carriers documented but no executable artifact refresh performed`

## Boundary For Now

The first map artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the first TiDB-versus-TiFlash pair
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
- executable map adapter or differential harness wiring
- nested predicate or compute semantics beyond passthrough `column(index)`
- broader nested-family artifact sets for `union` or nested combinations
- TiKV single-engine or pairwise map artifacts
