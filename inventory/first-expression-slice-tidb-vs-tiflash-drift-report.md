# First Expression Slice TiDB-vs-TiFlash Drift Report Format

Status: issue #68 design checkpoint

Related issues:

- #68 `design: define first differential expression slice and drift report format`

## Purpose

This note defines the minimum artifact set for the first differential expression slice from `tests/differential/first-expression-slice.md`.

The goal is to make future TiDB-versus-TiFlash comparison output reviewable and check-in friendly before broader inventory or harness tooling exists.

Its filename follows `docs/process/inventory-artifact-naming.md` and serves as the first concrete example of that convention.

The minimal adapter request and response boundary that feeds these artifacts is defined in `adapters/first-expression-slice.md`.

## Artifact Set

The first differential checkpoint should produce two artifact kinds:

1. one normalized `case result` capture per engine and case
2. one aggregated `drift report` that compares those captures case by case

These artifacts should be simple JSON-serializable records even if the first implementation also renders a human-readable summary.

## `case result` Minimum Fields

Each per-engine case capture should record at least:

- `slice_id`
- `engine`
- `adapter`
- `case_id`
- `spec_refs[]`
- `input_ref`
- `projection_ref` or equivalent expression descriptor
- `outcome.kind` = `rows` or `error`

When `outcome.kind` is `rows`, include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using JSON scalars plus `null`
- `row_count`

When `outcome.kind` is `error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For the first slice, `error_class` must be stable enough for the overflow checkpoint. Exact engine wording remains evidence only.

## `drift report` Minimum Fields

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

## Inventory Boundary

The first drift report format is intentionally narrow.

It does not yet define:

- artifact families beyond the current `case-results` and `drift-report` examples
- performance result formats
- merged multi-engine summaries beyond the first pairwise slice
- adapter-internal traces or engine plan captures

Those remain follow-on inventory and harness issues.
