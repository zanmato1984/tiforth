# First Float64 Ordering Slice Artifact Carriers

Status: issue #208 artifact-carrier and harness checkpoint, issue #292 TiKV single-engine artifact expansion checkpoint, issue #294 TiKV pairwise artifact expansion checkpoint

Related issues:

- #194 `design: define first float64 NaN, infinity, and ordering checkpoint`
- #208 `harness: execute first-float64-ordering-slice differential artifacts for TiDB and TiFlash`
- #286 `design: define TiKV adapter boundary for first-float64-ordering-slice`
- #292 `harness: execute first-float64-ordering-slice TiKV single-engine artifacts`
- #294 `harness: execute first-float64-ordering-slice TiKV pairwise drift artifacts`

## Purpose

This note defines the stable differential artifact carriers for
`first-float64-ordering-slice` from
`tests/differential/first-float64-ordering-slice.md`.

The goal is to keep artifact shape under source-of-truth harness docs while
checked-in `inventory/` files remain reviewable evidence instead of schema
authority.

The minimal adapter request and response boundary that feeds these artifacts is
defined in `adapters/first-float64-ordering-slice.md`.

## Artifact Set

The current executable float64 checkpoint produces nine checked-in artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one normalized TiKV `case-results` artifact
4. one aggregated TiDB-versus-TiFlash `drift-report`
5. one machine-readable TiDB-versus-TiFlash `drift-report` sidecar
6. one aggregated TiDB-versus-TiKV `drift-report`
7. one machine-readable TiDB-versus-TiKV `drift-report` sidecar
8. one aggregated TiFlash-versus-TiKV `drift-report`
9. one machine-readable TiFlash-versus-TiKV `drift-report` sidecar

Current artifact filenames for this slice:

- `inventory/first-float64-ordering-slice-tidb-case-results.json`
- `inventory/first-float64-ordering-slice-tiflash-case-results.json`
- `inventory/first-float64-ordering-slice-tikv-case-results.json`
- `inventory/first-float64-ordering-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-float64-ordering-slice-tidb-vs-tiflash-drift-report.json`
- `inventory/first-float64-ordering-slice-tidb-vs-tikv-drift-report.md`
- `inventory/first-float64-ordering-slice-tidb-vs-tikv-drift-report.json`
- `inventory/first-float64-ordering-slice-tiflash-vs-tikv-drift-report.md`
- `inventory/first-float64-ordering-slice-tiflash-vs-tikv-drift-report.json`

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
  - `comparison_mode`
  - exactly one operation reference: `projection_ref` or `filter_ref`
  - `outcome.kind` = `rows` or `error`

When `outcome.kind = rows`, include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using canonical float64 string tokens plus `null`
- `row_count`

For this slice, float64 rows should normalize special values as:

- `-Infinity`
- `Infinity`
- `NaN`
- finite decimal strings, including signed-zero distinction (`-0.0`, `0.0`)

When `outcome.kind = error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this slice, `error_class` must stay stable enough for:

- `missing_column`
- `unsupported_floating_type`
- `adapter_unavailable`
- `engine_error`

## `drift-report` Carrier For This Slice

The shared cross-slice drift-report carrier now lives in:

- `tests/differential/drift-report-carrier.md`

The first-float64 checkpoint should use that shared carrier without adding
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
engine-path gaps for already-documented first-float64 cases, and each
`unsupported` record should include a concrete `follow_up`.

The machine-readable sidecar should mirror the shared carrier fields
(`slice_id`, `engines[]`, `spec_refs[]`, and `cases[]`) used by the paired
Markdown report.

## Comparison-Mode Rule For This Slice

The first-float64 checkpoint uses two `comparison_mode` values:

- `row-order-preserved`: compare normalized `rows[]` in output order
- `float64-multiset-canonical`: compare normalized `rows[]` after canonical
  sorting by `-Infinity < finite < Infinity < NaN`, with `-0.0` before `0.0`
  only as deterministic tie-break

This comparison-mode behavior is a differential harness convention for this
slice; it is not shared SQL `ORDER BY` policy.

## Inventory Refresh Boundary

Issue #208 adds executable fixture-runner wiring and checks in the first
`first-float64-ordering-slice` artifacts listed above.

Issue #292 extends the same artifact carrier with one checked-in TiKV
single-engine `case-results` artifact.

Issue #294 extends the same carrier with checked-in TiKV pairwise drift-report
Markdown and JSON sidecars for `tidb-vs-tikv` and `tiflash-vs-tikv`.

Follow-on PRs should refresh those artifacts when slice semantics, case IDs,
comparison modes, normalized fields, or drift conclusions change under
`docs/process/inventory-refresh.md`.

## Boundary For Now

The first float64 artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond per-pair TiDB/TiFlash/TiKV reports
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
- float arithmetic, cast, or coercion differential evidence
- SQL ordering policy beyond this slice's canonical comparison-mode convention
