# First Union Slice Artifact Carriers

Status: issue #241 design checkpoint, issue #340 artifact-carrier checkpoint, issue #366 executable artifact checkpoint, issue #368 TiKV artifact checkpoint

Related issues:

- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #151 `design: define first nested-family shared handoff slice and claim-ownership boundary`
- #226 `design: define first struct nested handoff slice checkpoint`
- #230 `docs: define first map nested handoff slice checkpoint`
- #241 `docs: define first union nested handoff slice checkpoint`
- #340 `docs: define first-union-slice differential artifact carriers`
- #366 `harness: execute first-union-slice differential artifacts`
- #368 `harness: add TiKV first-union-slice executable checkpoints`

## Purpose

This note defines the stable differential artifact carriers for
`first-union-slice` from `tests/differential/first-union-slice.md`.

The goal is to keep artifact shape under source-of-truth harness docs while
checked-in `inventory/` files remain reviewable evidence instead of schema
authority.

The minimal shared adapter request and response boundary that feeds these artifacts is
defined in `adapters/first-union-slice.md`, and the TiKV-specific follow-on
boundary is defined in `adapters/first-union-slice-tikv.md`.

## Artifact Set

The current executable union checkpoint now produces nine checked-in artifacts:

1. one normalized TiDB `case-results` artifact
2. one normalized TiFlash `case-results` artifact
3. one normalized TiKV `case-results` artifact
4. one aggregated TiDB-versus-TiFlash `drift-report`
5. one machine-readable TiDB-versus-TiFlash `drift-report` sidecar
6. one aggregated TiDB-versus-TiKV `drift-report`
7. one machine-readable TiDB-versus-TiKV `drift-report` sidecar
8. one aggregated TiFlash-versus-TiKV `drift-report`
9. one machine-readable TiFlash-versus-TiKV `drift-report` sidecar

Artifact filenames for this slice:

- `inventory/first-union-slice-tidb-case-results.json`
- `inventory/first-union-slice-tiflash-case-results.json`
- `inventory/first-union-slice-tikv-case-results.json`
- `inventory/first-union-slice-tidb-vs-tiflash-drift-report.md`
- `inventory/first-union-slice-tidb-vs-tiflash-drift-report.json`
- `inventory/first-union-slice-tidb-vs-tikv-drift-report.md`
- `inventory/first-union-slice-tidb-vs-tikv-drift-report.json`
- `inventory/first-union-slice-tiflash-vs-tikv-drift-report.md`
- `inventory/first-union-slice-tiflash-vs-tikv-drift-report.json`

Issue #366 refreshes the TiDB/TiFlash artifact set, and issue #368 adds and refreshes the TiKV single-engine plus pairwise first-union artifacts.

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
- `rows[]` using canonical union carrier objects
- `row_count`

For this slice, each union row should be normalized as one canonical JSON
object with stable keys (`tag`, then `value`). The `tag` field should use the
stable variant names `i` or `n`, and the `value` field should use a JSON
number or `null` according to the selected variant payload.

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

The first union checkpoint should use that shared carrier without adding extra
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
engine-path gaps for already-documented first-union cases, and each
`unsupported` record should include a concrete `follow_up`.

The machine-readable sidecar should mirror the shared carrier fields
(`slice_id`, `engines[]`, `spec_refs[]`, and `cases[]`) used by the paired
Markdown report.

## Inventory Refresh Boundary

Issue #366 lands executable runner wiring for this slice through `crates/tiforth-adapter-tidb/src/first_union_slice.rs`, `crates/tiforth-adapter-tiflash/src/first_union_slice.rs`, `crates/tiforth-harness-differential/src/first_union_slice.rs`, and `crates/tiforth-harness-differential/src/bin/first_union_slice.rs` for the TiDB-versus-TiFlash checkpoint. Issue #368 extends executable wiring through `crates/tiforth-adapter-tikv/src/first_union_slice.rs`, `crates/tiforth-harness-differential/src/first_union_slice_tikv.rs`, `crates/tiforth-harness-differential/src/first_union_slice_tikv_pairwise.rs`, and `crates/tiforth-harness-differential/src/bin/first_union_slice_tikv_pairwise.rs`, and refreshes the TiKV `inventory/first-union-slice-*` artifacts listed above.

Follow-on PRs that change first-union semantics, case identifiers, adapter normalization, or drift-comparison policy should refresh those artifacts and declare `Inventory-Impact: updated`.

## Boundary For Now

The first union artifact carriers are intentionally narrow.

They do not yet define:

- performance result formats
- merged multi-engine summaries beyond the current pairwise reports
- adapter-internal traces or engine plan captures
- live engine orchestration metadata beyond the normalized first-slice carriers
- nested predicate or compute semantics beyond passthrough `column(index)`
- broader union-family artifact sets for `sparse_union` or nested combinations
- TiKV compatibility-note artifacts for this slice
