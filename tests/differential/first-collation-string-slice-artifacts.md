# First Collation-Sensitive String Slice Artifact Carriers

Status: issue #233 design checkpoint, issue #342 artifact-carrier checkpoint, issue #358 executable artifact checkpoint

Related issues:

- #143 `docs: define initial collation scope and ownership boundary`
- #233 `design: define first string collation semantic slice`
- #342 `docs: define first-collation-string-slice differential artifact carriers`
- #358 `harness: execute first-collation-string-slice differential artifacts`

## Purpose

This note defines the stable differential artifact carriers for
`first-collation-string-slice` from
`tests/differential/first-collation-string-slice.md`.

The goal is to keep artifact shape under source-of-truth harness docs while
checked-in `inventory/` files remain reviewable evidence instead of schema
authority.

The minimal adapter request and response boundary that feeds these artifacts is
defined in `adapters/first-collation-string-slice.md`.

## Artifact Set

The first executable collation differential checkpoint should produce four
checked-in artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one aggregated TiDB-versus-TiFlash `drift-report`
4. one machine-readable TiDB-versus-TiFlash `drift-report` sidecar

Artifact filenames for this slice:

- `inventory/first-collation-string-slice-tidb-case-results.json`
- `inventory/first-collation-string-slice-tiflash-case-results.json`
- `inventory/first-collation-string-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-collation-string-slice-tidb-vs-tiflash-drift-report.json`

Issue #358 refreshes those four checked-in `inventory/first-collation-string-slice-*` files.

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
  - `collation_ref`
  - exactly one operation reference: `projection_ref`, `filter_ref`,
    `comparison_ref`, or `ordering_ref`
  - `outcome.kind` = `rows` or `error`

When `outcome.kind = rows`, include:

- `schema[]` with `name`, `logical_type`, and `nullable`
- `rows[]` using UTF-8 strings, booleans, and carrier `null`
- `row_count`

For this slice, adapters should preserve row order exactly as produced by the
documented operation and `collation_ref`, so canonical ordering cases can be
reviewed directly from the normalized `rows[]` carrier.

When `outcome.kind = error`, include:

- `error_class`
- optional `engine_code`
- optional `engine_message`

For this slice, `error_class` must stay stable enough for:

- `missing_column`
- `unknown_collation`
- `unsupported_collation_type`
- `adapter_unavailable`
- `engine_error`

## `drift-report` Carrier For This Slice

The shared cross-slice drift-report carrier now lives in:

- `tests/differential/drift-report-carrier.md`

The first collation checkpoint should use that shared carrier without adding
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
engine-path gaps for already-documented first-collation cases, and each
`unsupported` record should include a concrete `follow_up`.

The machine-readable sidecar should mirror the shared carrier fields
(`slice_id`, `engines[]`, `spec_refs[]`, and `cases[]`) used by the paired
Markdown report.

## Inventory Refresh Boundary

Issue #358 now lands executable runner wiring for this slice through `crates/tiforth-adapter-tidb/src/first_collation_string_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_collation_string_slice.rs`, `crates/tiforth-harness-differential/src/first_collation_string_slice.rs`, and `crates/tiforth-harness-differential/src/bin/first_collation_string_slice.rs`, and refreshes the four checked-in `inventory/first-collation-string-slice-*` artifacts listed above.

Follow-on PRs that change first-collation semantics, case identifiers, adapter normalization, or drift-comparison policy should refresh those artifacts and declare `Inventory-Impact: updated`.

## Boundary For Now

The first collation artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the first TiDB-versus-TiFlash pair
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
- locale-specific collation families beyond `binary` and `unicode_ci`
- string-function or binary-family artifact sets beyond the first comparison
  and ordering probes
- TiKV single-engine or pairwise collation artifacts
