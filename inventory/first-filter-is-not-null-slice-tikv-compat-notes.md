# First Filter Slice TiKV Compatibility Notes

Status: issue #249 adapter and inventory checkpoint

Verified: 2026-03-20

Related issues:

- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #147 `design: define first differential filter slice and adapter boundary for is_not_null`
- #247 `design: define first TiKV differential filter adapter request/response surface`
- #249 `adapter: execute first-filter-is-not-null-slice through TiKV`

## Purpose

This note records TiKV-side compatibility evidence for the first
`first-filter-is-not-null-slice` TiKV adapter checkpoint.

It scopes only to the shared first-filter surface:

- `is_not_null(column(index))`
- stable case IDs and refs from `tests/differential/first-filter-is-not-null-slice.md`
- normalized `case-results` carrier fields from
  `tests/differential/first-filter-is-not-null-slice-artifacts.md`

This artifact treats adapter boundary docs and executable adapter-core tests as
source evidence, not as shared design authority.

## TiKV Snapshot

- tiforth repository base commit reviewed: `b22bf9be7a75423944b55dbe4853e9c271cf0bcb`
- artifact baseline: deterministic TiKV adapter-core checkpoint from issue #249
  with checked-in single-engine `case-results` evidence
- paired TiKV filter drift artifacts now cover `tidb-vs-tikv` and `tiflash-vs-tikv`
- live TiKV runner artifacts remain follow-on scope

## Shared Slice Anchors

This note stays anchored to first-filter shared vocabulary:

- `slice_id = first-filter-is-not-null-slice`
- `filter_ref = is-not-null-column-0`
- `filter_ref = is-not-null-column-1`
- `case_id = all-rows-kept`
- `case_id = all-rows-dropped`
- `case_id = mixed-keep-drop`
- `case_id = missing-column-error`
- `case_id = unsupported-predicate-type-error`

## Reviewed Sources

- `adapters/first-filter-is-not-null-slice-tikv.md`
- `tests/differential/first-filter-is-not-null-slice.md`
- `tests/differential/first-filter-is-not-null-slice-artifacts.md`
- `crates/tiforth-adapter-tikv/src/first_filter_is_not_null_slice.rs`
- `crates/tiforth-harness-differential/src/first_filter_is_not_null_slice_tikv.rs`
- `crates/tiforth-harness-differential/src/first_filter_is_not_null_slice_tikv_pairwise.rs`
- `inventory/first-filter-is-not-null-slice-tikv-case-results.json`
- `inventory/first-filter-is-not-null-slice-tidb-vs-tikv-drift-report.md`
- `inventory/first-filter-is-not-null-slice-tiflash-vs-tikv-drift-report.md`

## Compatibility Notes

### Filter SQL Lowering And Case Mapping

#### TiKV Adapter Surface

- TiKV first-filter lowering renders stable SQL for each documented case with:
  - deterministic inline input rows per shared `input_ref`
  - one filter condition per shared `filter_ref`
  - missing-column probes routed through `input_rows.__missing_column_1`

#### Recorded TiKV Facts

- all five shared case IDs have deterministic TiKV lowering and execution
  normalization paths in adapter-core tests
- `mixed-keep-drop` preserves row order and full-row passthrough shape
- the normalized carrier keeps the shared `input_ref` and `filter_ref` stable

#### Evidence Gaps

- this checkpoint does not include live TiKV planner or execution captures
- live TiKV pairwise runner captures remain follow-on scope

### Error Normalization

#### TiKV Adapter Surface

- missing-column failures normalize to `missing_column` when code `1054` is
  present or message text indicates unknown or missing columns
- unsupported predicate input normalizes to `unsupported_predicate_type` when
  code `1105` is present or message text includes unsupported-predicate wording
- unavailable runner paths normalize to `adapter_unavailable`
- all remaining failures normalize to `engine_error`

#### Recorded TiKV Facts

- deterministic adapter-core tests cover normalization for:
  - `missing-column-error`
  - `unsupported-predicate-type-error`
  - explicit `adapter_unavailable` behavior

#### Evidence Gaps

- this checkpoint does not yet include live TiKV error payload captures

## Boundary For This Artifact

- this note records TiKV-side compatibility evidence only for the first
  `is_not_null(column(index))` differential checkpoint
- it does not redefine shared filter semantics or shared request/response fields
- checked-in evidence for this checkpoint includes the TiKV single-engine
  `case-results` artifact plus paired TiDB-vs-TiKV and TiFlash-vs-TiKV
  `drift-report` artifacts
- live TiKV runner evidence remains follow-on work
