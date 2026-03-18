# Differential Drift Report Carrier

Status: issue #133 design checkpoint

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #113 `harness: compare first-expression-slice results for TiDB and TiFlash`
- #133 `design: define reusable differential drift-report carrier guidance`

## Purpose

This note defines the reusable minimum carrier for checked-in differential
`drift-report` artifacts under `inventory/`.

The goal is to keep shared drift-report structure stable across slices while
still allowing each slice to define its own compared dimensions.

## Applicability

This guidance applies to aggregated differential `drift-report` artifacts, such
as:

- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md`

Slice-specific artifact docs should reference this shared carrier and define
only their additional constraints.

## Minimum Report Fields

Each drift report should record at least:

- `slice_id`
- `engines[]`
- `spec_refs[]`
- status counts in a summary section for every status used in that report
- `cases[]`, where each case entry includes:
  - `case_id`
  - `status`
  - `comparison_dimensions[]`
  - `summary`
  - `evidence_refs[]`
  - optional `follow_up`

`evidence_refs[]` should point to normalized per-engine `case-results` records
for the same `case_id`, so reviewers can trace one comparison back to concrete
engine-side evidence.

## Status Vocabulary

The shared baseline status vocabulary is:

- `match`: compared engines produced the same normalized outcome for the
  compared dimensions in this slice
- `drift`: compared engines both produced evidence, but differ on one or more
  compared dimensions
- `unsupported`: at least one side could not execute the case for the current
  slice boundary (for example, an adapter path is intentionally missing)

If a later slice needs an additional status, introduce it through a docs-first
issue and update this file before using that status in checked-in artifacts.

## Comparison Dimensions

`comparison_dimensions[]` should use short stable identifiers that name what was
actually compared for the case.

Shared examples from the first executable slice are:

- `field_name`
- `field_nullability`
- `logical_type`
- `row_count`
- `row_values`
- `error_class`

Each slice-specific artifact note should explicitly list which dimensions are
allowed for that slice.

## Format Boundary

Milestone 1 keeps drift reports as review-first Markdown artifacts.

This carrier does not yet define:

- a required machine-readable drift-report sidecar format
- merged summaries across more than one engine pair in one artifact
- adapter-internal traces, engine plans, or local orchestration metadata
