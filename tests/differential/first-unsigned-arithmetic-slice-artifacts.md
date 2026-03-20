# First Unsigned Arithmetic Slice Artifact Carriers

Status: issue #300 design checkpoint, issue #302 artifact-carrier checkpoint

Related issues:

- #141 `spec: define signed/unsigned interaction checkpoint for initial coercion lattice`
- #276 `spec: define overflow behavior checkpoint for follow-on operator families`
- #300 `design: define first unsigned arithmetic semantic slice boundary`
- #302 `docs: define first-unsigned-arithmetic-slice differential artifact carriers`

## Purpose

This note defines the stable differential artifact carriers for
`first-unsigned-arithmetic-slice` from
`tests/differential/first-unsigned-arithmetic-slice.md`.

The goal is to keep artifact shape under source-of-truth harness docs while
checked-in `inventory/` files remain reviewable evidence instead of schema
authority.

The minimal adapter request and response boundary that feeds these artifacts is
defined in `adapters/first-unsigned-arithmetic-slice.md`.

## Artifact Set

The first executable unsigned arithmetic differential checkpoint should produce
four checked-in artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one aggregated TiDB-versus-TiFlash `drift-report`
4. one machine-readable TiDB-versus-TiFlash `drift-report` sidecar

Planned artifact filenames for this slice:

- `inventory/first-unsigned-arithmetic-slice-tidb-case-results.json`
- `inventory/first-unsigned-arithmetic-slice-tiflash-case-results.json`
- `inventory/first-unsigned-arithmetic-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-unsigned-arithmetic-slice-tidb-vs-tiflash-drift-report.json`

Issue #302 is docs-first only and does not add or refresh those `inventory/`
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
  - `comparison_mode`
  - exactly one operation reference: `projection_ref` or `filter_ref`
  - `outcome.kind` = `rows` or `error`

When `outcome.kind = rows`, include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using canonical unsigned decimal-string tokens plus `null`
- `row_count`

For this slice, non-null `uint64` values should be normalized to canonical
base-10 integer strings in `rows[]` so full-range `uint64` values remain exact.
SQL `NULL` rows should remain carrier `null`.

When `outcome.kind = error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this slice, `error_class` must stay stable enough for:

- `missing_column`
- `unsigned_overflow`
- `mixed_signed_unsigned`
- `unsupported_unsigned_family`
- `adapter_unavailable`
- `engine_error`

## `drift-report` Carrier For This Slice

The shared cross-slice drift-report carrier now lives in:

- `tests/differential/drift-report-carrier.md`

The first unsigned arithmetic checkpoint should use that shared carrier without
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
engine-path gaps for already-documented first-unsigned cases, and each
`unsupported` record should include a concrete `follow_up`.

The machine-readable sidecar should mirror the shared carrier fields
(`slice_id`, `engines[]`, `spec_refs[]`, and `cases[]`) used by the paired
Markdown report.

## Comparison-Mode Rule For This Slice

The first unsigned checkpoint currently uses one `comparison_mode` value:

- `row-order-preserved`: compare normalized `rows[]` in output order

This comparison-mode behavior is a differential harness convention for this
slice; it is not shared SQL `ORDER BY` policy.

## Inventory Refresh Boundary

This checkpoint documents carriers only.

Follow-on harness work should add executable runner wiring for this slice and
then refresh or check in the planned `inventory/` files using the same carrier
shape defined here.

Until that executable checkpoint lands, PRs that touch this note may declare:

- `Inventory-Impact: none - unsigned artifact carriers documented but no executable artifact refresh performed`

## Boundary For Now

The first unsigned arithmetic artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the first TiDB-versus-TiFlash pair
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
- executable unsigned adapter or differential harness wiring
- unsigned cast/coercion or wider unsigned-family artifact coverage
