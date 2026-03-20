# First Expression Slice Artifact Carriers

Status: issue #68 design checkpoint, issue #113 harness checkpoint, issue #133 drift-report-carrier checkpoint, issue #159 sidecar-policy checkpoint, issue #161 first-sidecar checkpoint, issue #235 TiKV single-engine case-results checkpoint

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #72 `design: define first differential adapter request/response surface`
- #113 `harness: compare first-expression-slice results for TiDB and TiFlash`
- #133 `design: define reusable differential drift-report carrier guidance`
- #159 `docs: define machine-readable sidecar policy for differential drift reports`
- #161 `harness: add machine-readable drift-report sidecars for first differential slices`
- #235 `inventory: add first-expression-slice TiKV case-results artifact checkpoint`

## Purpose

This note defines the stable checked-in artifact carriers for the first differential expression slice from `tests/differential/first-expression-slice.md`.

The goal is to keep the artifact shape under source-of-truth harness docs while the checked-in `inventory/` files remain reviewable evidence rather than the place where the schema is defined.

The minimal adapter request and response boundary that feeds these artifacts is defined in `adapters/first-expression-slice.md`.

## Artifact Set

The first executable differential checkpoint produces four checked-in artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one aggregated TiDB-versus-TiFlash `drift-report`
4. one machine-readable TiDB-versus-TiFlash `drift-report` sidecar

Issue #235 also adds one checked-in TiKV single-engine `case-results` artifact that reuses the same `case-results` carrier shape below, without adding a TiKV pairwise `drift-report` requirement in this checkpoint.

These carriers should stay simple and JSON-serializable at the record level even when the drift report also renders a human-readable Markdown summary.

Current checked-in examples:

- `inventory/first-expression-slice-tidb-case-results.json`
- `inventory/first-expression-slice-tiflash-case-results.json`
- `inventory/first-expression-slice-tikv-case-results.json`
- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.json`

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

## `drift-report` Carrier For This Slice

The shared cross-slice drift-report carrier now lives in:

- `tests/differential/drift-report-carrier.md`

The first-expression-slice checkpoint uses that shared carrier without adding
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
engine-path gaps for already-documented first-slice cases, and each
`unsupported` record should include a concrete `follow_up`.

For the issue #161 first-sidecar checkpoint, this slice now also checks in a
machine-readable `drift-report` sidecar that mirrors the shared carrier fields
(`slice_id`, `engines[]`, `spec_refs[]`, and `cases[]`) used by the paired
Markdown report.

## Boundary For Now

The first artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the first TiDB-versus-TiFlash pair
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
