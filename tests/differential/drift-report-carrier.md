# Differential Drift Report Carrier

Status: issue #133 design checkpoint, issue #159 sidecar-policy checkpoint, issue #161 first-sidecar checkpoint

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #113 `harness: compare first-expression-slice results for TiDB and TiFlash`
- #133 `design: define reusable differential drift-report carrier guidance`
- #159 `docs: define machine-readable sidecar policy for differential drift reports`
- #161 `harness: add machine-readable drift-report sidecars for first differential slices`

## Purpose

This note defines the reusable minimum carrier for checked-in differential
`drift-report` artifacts under `inventory/`.

The goal is to keep shared drift-report structure stable across slices while
still allowing each slice to define its own compared dimensions.

## Applicability

This guidance applies to aggregated differential `drift-report` artifacts, such
as:

- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.json`

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

For milestone 1, this means:

- each differential slice checks in one Markdown `drift-report` artifact
- a machine-readable `drift-report` sidecar is optional and not required for
  slice completion
- milestone-1 slices may additionally check in one JSON sidecar that mirrors
  the same `slice_id`, engine pair, status vocabulary, and `cases[]` entries as
  the paired Markdown report

If a follow-on slice adds a machine-readable sidecar, it should:

- derive that sidecar from the same normalized `case-results` evidence used by
  the Markdown report
- use the same `slice_id`, `engines[]`, status vocabulary, and `cases[]` case
  identity as the paired Markdown report
- update this shared carrier and the slice-specific artifact doc before adding
  sidecar-only fields

This carrier still does not yet define:

- one required machine-readable drift-report schema for all slices
- merged summaries across more than one engine pair in one artifact
- adapter-internal traces, engine plans, or local orchestration metadata
