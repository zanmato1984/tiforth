# First Struct Slice Artifact Carriers

Status: issue #226 design checkpoint, issue #331 artifact-carrier checkpoint, issue #360 executable artifact checkpoint

Related issues:

- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #151 `design: define first nested-family shared handoff slice and claim-ownership boundary`
- #226 `design: define first struct nested handoff slice checkpoint`
- #331 `docs: define first-struct-slice differential artifact carriers`
- #360 `harness: execute first-struct-slice differential artifacts`

## Purpose

This note defines the stable differential artifact carriers for
`first-struct-slice` from `tests/differential/first-struct-slice.md`.

The goal is to keep artifact shape under source-of-truth harness docs while
checked-in `inventory/` files remain reviewable evidence instead of schema
authority.

The minimal adapter request and response boundary that feeds these artifacts is
defined in `adapters/first-struct-slice.md`.

## Artifact Set

The first executable struct differential checkpoint should produce four
checked-in artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one aggregated TiDB-versus-TiFlash `drift-report`
4. one machine-readable TiDB-versus-TiFlash `drift-report` sidecar

Artifact filenames for this slice:

- `inventory/first-struct-slice-tidb-case-results.json`
- `inventory/first-struct-slice-tiflash-case-results.json`
- `inventory/first-struct-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-struct-slice-tidb-vs-tiflash-drift-report.json`

Issue #360 refreshes those four checked-in `inventory/first-struct-slice-*` files.

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
- `rows[]` using canonical JSON object carriers plus carrier `null`
- `row_count`

For this slice, each non-null struct row should be normalized as one canonical
JSON object whose key order matches the shared struct field order (`a`, then
`b`). Child values should use JSON numbers or `null`. SQL `NULL` struct rows
should stay carrier `null`.

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

The first struct checkpoint should use that shared carrier without adding extra
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
engine-path gaps for already-documented first-struct cases, and each
`unsupported` record should include a concrete `follow_up`.

The machine-readable sidecar should mirror the shared carrier fields
(`slice_id`, `engines[]`, `spec_refs[]`, and `cases[]`) used by the paired
Markdown report.

## Inventory Refresh Boundary

Issue #360 now lands executable runner wiring for this slice through `crates/tiforth-adapter-tidb/src/first_struct_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_struct_slice.rs`, `crates/tiforth-harness-differential/src/first_struct_slice.rs`, and `crates/tiforth-harness-differential/src/bin/first_struct_slice.rs`, and refreshes the four checked-in `inventory/first-struct-slice-*` artifacts listed above.

Follow-on PRs that change first-struct semantics, case identifiers, adapter normalization, or drift-comparison policy should refresh those artifacts and declare `Inventory-Impact: updated`.

## Boundary For Now

The first struct artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the first TiDB-versus-TiFlash pair
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
- nested predicate or compute semantics beyond passthrough `column(index)`
- broader nested-family artifact sets for `map`, `union`, or nested combinations
- TiKV single-engine or pairwise struct artifacts
