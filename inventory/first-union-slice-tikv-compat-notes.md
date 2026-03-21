# First Union Slice TiKV Compatibility Notes

Status: issue #368 adapter, harness, and inventory checkpoint; issue #370 compatibility-notes checkpoint

Verified: 2026-03-21

Related issues:

- #241 `docs: define first union nested handoff slice checkpoint`
- #340 `docs: define first-union-slice differential artifact carriers`
- #366 `harness: execute first-union-slice differential artifacts`
- #368 `harness: add TiKV first-union-slice executable checkpoints`
- #370 `inventory: add first-union-slice TiKV compatibility notes checkpoint`

## Purpose

This note records TiKV-side compatibility evidence for the
`first-union-slice` checkpoint.

It scopes only to the shared first-union surface:

- passthrough projection `column(index)` over
  `dense_union<i:int32, n:int32?>`
- stable first-union case IDs and refs from
  `tests/differential/first-union-slice.md`
- normalized `case-results` and pairwise `drift-report` carriers from
  `tests/differential/first-union-slice-artifacts.md`

This artifact treats checked-in single-engine and pairwise evidence as source
input, not as shared design authority.

## TiKV Snapshot

- tiforth repository base commit reviewed:
  `26945e6870f1500f803b311610bb4a0a4ff94d35`
- artifact baseline: deterministic TiKV adapter-core single-engine plus pairwise
  checkpoint from issue #368
- paired union TiKV drift artifacts now cover `tidb-vs-tikv` and
  `tiflash-vs-tikv`
- live TiKV runner refresh artifacts remain follow-on scope for this slice

## Shared Slice Anchors

This note stays anchored to stable first-union vocabulary:

- `slice_id = first-union-slice`
- `projection_ref = column-0`
- `projection_ref = column-1`
- `case_id = union-column-passthrough`
- `case_id = union-variant-switch-preserve`
- `case_id = union-variant-null-preserve`
- `case_id = union-missing-column-error`
- `case_id = unsupported-nested-family-error`

## Reviewed Sources

- `docs/design/first-union-aware-handoff-slice.md`
- `adapters/first-union-slice-tikv.md`
- `tests/conformance/first-union-slice.md`
- `tests/differential/first-union-slice.md`
- `tests/differential/first-union-slice-artifacts.md`
- `crates/tiforth-adapter-tikv/src/first_union_slice.rs`
- `crates/tiforth-harness-differential/src/first_union_slice_tikv.rs`
- `crates/tiforth-harness-differential/src/first_union_slice_tikv_pairwise.rs`
- `inventory/first-union-slice-tikv-case-results.json`
- `inventory/first-union-slice-tidb-vs-tikv-drift-report.md`
- `inventory/first-union-slice-tiflash-vs-tikv-drift-report.md`

## Compatibility Notes

### Union Passthrough And Variant Preservation

#### TiKV Adapter Surface

- the TiKV adapter lowers first-union projection probes into deterministic SQL
  over adapter-owned aliases
- union row normalization emits canonical carriers with stable keys (`tag`, then
  `value`) for both single-engine and pairwise comparisons

#### Recorded TiKV Facts

- `union-column-passthrough` returns 3 rows preserving the documented variant
  sequence `{i:1}`, `{n:2}`, `{i:3}`
- `union-variant-switch-preserve` returns the same variant sequence as the
  shared first-union basic input probe
- `union-variant-null-preserve` keeps the nullable variant payload row
  `{tag:n, value:null}` plus `{tag:i, value:4}` and `{tag:n, value:5}`
- normalized schema is `u:dense_union<i:int32,n:int32?>, nullable=false`

### Missing Column Error Normalization

#### TiKV Adapter Surface

- the adapter emits a deliberate missing-column SQL reference
  (`__missing_column_1`) for the documented out-of-range projection probe
- error normalization maps TiKV missing-column signals into shared
  `error_class = missing_column`

#### Recorded TiKV Facts

- `union-missing-column-error` normalizes to
  `error_class = missing_column`
- checked-in TiKV evidence reports `engine_code = 1054` with message
  `Unknown column '__missing_column_1' in 'field list'`

### Unsupported Nested Family Normalization

#### TiKV Adapter Surface

- the first-union checkpoint keeps broader nested-family behavior out of scope
  and uses a documented unsupported probe via `first-map-basic`
- error normalization maps this path into shared
  `error_class = unsupported_nested_family`

#### Recorded TiKV Facts

- `unsupported-nested-family-error` normalizes to
  `error_class = unsupported_nested_family`
- checked-in TiKV evidence reports `engine_code = 1105` with message
  `unsupported nested expression input at column 0`

## Differential Summary Link

- paired TiKV union drift reports record `match = 5`, `drift = 0`, and
  `unsupported = 0` for both `tidb-vs-tikv` and `tiflash-vs-tikv`

## Boundary For This Artifact

- this note records TiKV-side compatibility evidence only for the first union
  slice
- it does not redefine shared request/response fields or drift-carrier schema
- checked-in evidence for this checkpoint includes the TiKV single-engine
  `case-results` artifact plus paired TiDB-vs-TiKV and TiFlash-vs-TiKV
  `drift-report` artifacts
- broader union modes, nested predicates, and live-runner TiKV refresh captures
  remain follow-on work
