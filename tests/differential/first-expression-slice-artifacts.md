# First Expression Slice Artifact Carriers

Status: issue #68 design checkpoint, issue #113 harness checkpoint

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #72 `design: define first differential adapter request/response surface`
- #113 `harness: compare first-expression-slice results for TiDB and TiFlash`

## Purpose

This note defines the stable checked-in artifact carriers for the first differential expression slice from `tests/differential/first-expression-slice.md`.

The goal is to keep the artifact shape under source-of-truth harness docs while the checked-in `inventory/` files remain reviewable evidence rather than the place where the schema is defined.

The minimal adapter request and response boundary that feeds these artifacts is defined in `adapters/first-expression-slice.md`.

## Artifact Set

The first executable differential checkpoint produces three checked-in artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one aggregated TiDB-versus-TiFlash `drift-report`

These carriers should stay simple and JSON-serializable at the record level even when the drift report also renders a human-readable Markdown summary.

Current checked-in examples:

- `inventory/first-expression-slice-tidb-case-results.json`
- `inventory/first-expression-slice-tiflash-case-results.json`
- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md`

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
- `rows[]` using JSON scalars plus `null`
- `row_count`

When `outcome.kind = error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For the first slice, `error_class` must stay stable enough for:

- `arithmetic_overflow`
- `adapter_unavailable`
- `engine_error`

## `drift-report` Minimum Fields

The aggregated drift report should record at least:

- `slice_id`
- `engines[]`
- `spec_refs[]`
- `cases[]`, where each case entry includes:
  - `case_id`
  - `status`: `match`, `drift`, or `unsupported`
  - `comparison_dimensions[]`
  - `summary`
  - `evidence_refs[]`
  - optional `follow_up`

### Status Meanings

- `match`: compared engines produced the same normalized outcome for the fields this slice cares about
- `drift`: compared engines both produced evidence, but that evidence differs on compared semantics such as values, nullability, field names, or `error_class`
- `unsupported`: the case could not be executed on one side because the adapter or engine path for that slice does not exist yet; this should be used sparingly and called out explicitly

### Comparison Dimensions

For the first expression slice, `comparison_dimensions[]` should only use dimensions that the slice actually compares:

- `field_name`
- `field_nullability`
- `logical_type`
- `row_count`
- `row_values`
- `error_class`

## Boundary For Now

The first artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the first TiDB-versus-TiFlash pair
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
