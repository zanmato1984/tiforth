# First Signed-Widening `add<int64>` Slice Artifact Carriers

Status: issue #426 design checkpoint, issue #434 executable checkpoint

Related issues:

- #276 `spec: define overflow behavior checkpoint for follow-on operator families`
- #409 `epic: complete function-family program`
- #422 `spec: complete the numeric add/plus family boundary`
- #426 `design: define first signed-widening add/int64 slice for the numeric add/plus family`
- #434 `milestone-1: implement first executable signed-widening add/int64 TiDB/TiFlash differential slice`

## Purpose

This note defines the stable differential artifact carriers for
`first-signed-widening-add-int64-slice` from
`tests/differential/first-signed-widening-add-int64-slice.md`.

The goal is to keep artifact shape under source-of-truth harness docs while
checked-in `inventory/` files remain reviewable evidence instead of schema
authority.

The minimal adapter request and response boundary that feeds these artifacts is
defined in `adapters/first-signed-widening-add-int64-slice.md`.

## Artifact Set

The first executable signed-widening differential checkpoint should produce
four checked-in artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one aggregated TiDB-versus-TiFlash `drift-report`
4. one machine-readable TiDB-versus-TiFlash `drift-report` sidecar

Artifact filenames for this slice:

- `inventory/first-signed-widening-add-int64-slice-tidb-case-results.json`
- `inventory/first-signed-widening-add-int64-slice-tiflash-case-results.json`
- `inventory/first-signed-widening-add-int64-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-signed-widening-add-int64-slice-tidb-vs-tiflash-drift-report.json`

Issue #434 adds those checked-in `inventory/` files using the stable
filenames and carrier shape fixed here.

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
  - `projection_ref`
  - `outcome.kind` = `rows` or `error`

When `outcome.kind = rows`, include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using JSON numeric scalars plus `null`
- `row_count`

For this slice, non-null `int64` row values should normalize as JSON numeric
scalars, while SQL `NULL` rows remain carrier `null`.

When `outcome.kind = error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this slice, `error_class` must stay stable enough for:

- `missing_column`
- `arithmetic_overflow`
- `adapter_unavailable`
- `engine_error`

## `drift-report` Carrier For This Slice

The shared cross-slice drift-report carrier now lives in:

- `tests/differential/drift-report-carrier.md`

The first signed-widening checkpoint should use that shared carrier without
adding extra status values.

For this slice, `comparison_dimensions[]` should only use dimensions that the
slice actually compares:

- `field_name`
- `field_nullability`
- `logical_type`
- `row_count`
- `row_values`
- `error_class`

For this slice, `unsupported` should stay limited to explicit adapter or
engine-path gaps for already-documented first-signed cases, and each
`unsupported` record should include a concrete `follow_up`.

The machine-readable sidecar should mirror the shared carrier fields
(`slice_id`, `engines[]`, `spec_refs[]`, and `cases[]`) used by the paired
Markdown report.

## Comparison-Mode Rule For This Slice

The first signed-widening checkpoint currently uses one `comparison_mode`
value:

- `row-order-preserved`: compare normalized `rows[]` in output order

This comparison-mode behavior is a differential harness convention for this
slice; it is not shared SQL `ORDER BY` policy.

## Inventory Refresh Boundary

Issue #434 adds the first checked-in artifacts under the filenames above.
Future harness work should refresh those `inventory/` files whenever the
documented case set, normalization rules, or shared carrier fields change.

## Boundary For Now

The first signed-widening artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the first TiDB-versus-TiFlash pair
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
- broader signed-family or mixed signed/unsigned artifact coverage
