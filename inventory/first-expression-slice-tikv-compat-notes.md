# First Expression Slice TiKV Compatibility Notes

Status: issue #228 inventory checkpoint, issue #235 case-results checkpoint, issue #245 pairwise drift-artifact checkpoint

Verified: 2026-03-19

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #72 `design: define first differential adapter request/response surface`
- #218 `design: define first TiKV differential adapter request/response surface`
- #220 `adapter: execute first-expression-slice through TiKV`
- #228 `inventory: add first-expression-slice TiKV compatibility notes checkpoint`
- #235 `inventory: add first-expression-slice TiKV case-results artifact checkpoint`
- #245 `harness: add first-expression-slice TiKV pairwise drift artifacts`

## Purpose

This note records TiKV-side compatibility evidence for the first
`first-expression-slice` TiKV adapter checkpoint.

It scopes only to the shared first-expression surface:

- the single-input projection operator
- `column(index)` as normalized by shared slice refs
- `literal<int32>(value)`
- `add<int32>(lhs, rhs)`

This artifact treats local adapter boundary docs and executable adapter-core
tests as source evidence, not as shared design authority.

## TiKV Snapshot

- tiforth repository base commit reviewed: `7b8369c8c782c0b14d5046fb1ad3c779c0cd6ba5`
- artifact baseline: deterministic TiKV adapter-core checkpoint from issue #220 plus issue #235 checked-in single-engine case-results evidence and issue #245 checked-in pairwise drift-report evidence
- live TiKV runner artifacts are not yet checked in for this slice

## Shared Slice Anchors

This note stays anchored to the stable first-expression vocabulary already
defined in `tests/differential/first-expression-slice.md`.

- `slice_id = first-expression-slice`
- `projection_ref = column-a`
- `projection_ref = literal-int32-seven`
- `projection_ref = literal-int32-null`
- `projection_ref = add-a-plus-one`
- `case_id = column-passthrough`
- `case_id = literal-int32-seven`
- `case_id = literal-int32-null`
- `case_id = add-int32-literal`
- `case_id = add-int32-null-propagation`
- `case_id = add-int32-overflow-error`

## Reviewed Sources

- `adapters/first-expression-slice-tikv.md`
- `tests/differential/first-expression-slice.md`
- `tests/differential/first-expression-slice-artifacts.md`
- `crates/tiforth-adapter-tikv/src/first_expression_slice.rs`
- `crates/tiforth-harness-differential/src/first_expression_slice_tikv.rs`
- `crates/tiforth-harness-differential/src/first_expression_slice_tikv_pairwise.rs`
- `inventory/first-expression-slice-tikv-case-results.json`
- `inventory/first-expression-slice-tidb-vs-tikv-drift-report.md`
- `inventory/first-expression-slice-tiflash-vs-tikv-drift-report.md`

## Compatibility Notes

### Projection Output Shape And Naming

#### TiKV Adapter Surface

- TiKV first-slice lowering emits explicit aliases for each documented
  projection shape:
  - `column-passthrough`: `... AS a`
  - `literal-int32-*`: `... AS lit`
  - `add-a-plus-one`: `... AS a_plus_one`
- normalized case results preserve adapter-lowered field names directly in the
  emitted `schema[]`.

#### Recorded TiKV Facts

- stable shared output names are adapter-owned for this checkpoint, not inferred
  from engine-default unaliased expressions
- current executable adapter-core tests assert the same lowerings and normalized
  schema names for all six first-expression case IDs

#### Evidence Gaps

- this note is based on deterministic adapter-core evidence and the checked-in normalized TiKV single-engine `case-results` artifact for this slice

### `column(index)` / Passthrough Projection

#### TiKV Adapter Surface

- shared `column(index)` normalization lowers to named SQL field selection:
  `SELECT input_rows.a AS a FROM (...) AS input_rows`
- first-slice input refs map to deterministic `CAST(... AS SIGNED)` row sources
  before projection lowering.

#### Recorded TiKV Facts

- `column-passthrough` remains anchored to
  `input_ref = first-expression-slice-int32-basic`
- normalized engine type handling maps integer-style engine metadata to shared
  logical `int32` for passthrough projections

#### Evidence Gaps

- this checkpoint does not include live TiKV planner or execution captures for
  passthrough projection paths

### `literal<int32>(value)`

#### TiKV Adapter Surface

- literal checkpoints lower to:
  - `SELECT CAST(7 AS SIGNED) AS lit ...`
  - `SELECT CAST(NULL AS SIGNED) AS lit ...`
- literal case normalization explicitly narrows engine-reported `bigint`
  metadata back to shared `int32` for the two literal case IDs.

#### Recorded TiKV Facts

- literal output naming is stable through explicit aliasing (`lit`)
- literal case logical typing is adapter-normalized to shared `int32` even when
  the engine metadata family reports `bigint`
- nullability still flows from engine result metadata (`nullable = true` for the
  null-literal path)

#### Evidence Gaps

- checked-in inventory does not yet include live TiKV metadata captures for
  literal cases; this note records adapter-normalized checkpoint intent

### `add<int32>(lhs, rhs)`

#### TiKV Adapter Surface

- add checkpoints lower to:
  `SELECT CAST(input_rows.a + CAST(1 AS SIGNED) AS SIGNED) AS a_plus_one ...`
- overflow normalization maps TiKV failures to `arithmetic_overflow` when code
  `1690` is present or error text includes `out of range` or `overflow`
- non-overflow engine failures normalize to `engine_error`; unavailable runner
  paths normalize to `adapter_unavailable`.

#### Recorded TiKV Facts

- deterministic adapter-core tests cover:
  - row-returning `add-int32-literal` normalization
  - overflow error normalization for `add-int32-overflow-error`
  - explicit `adapter_unavailable` normalization
- non-literal `bigint` metadata remains `int64` after normalization, while
  literal-int32 cases keep the narrow `int32` override

#### Evidence Gaps

- TiKV pairwise drift aggregation is now checked in through `inventory/first-expression-slice-tidb-vs-tikv-drift-report.{md,json}` and `inventory/first-expression-slice-tiflash-vs-tikv-drift-report.{md,json}`; live-runner evidence remains follow-on work

## Boundary For This Artifact

- this note records TiKV-side compatibility evidence only for the
  first-expression projection, `column`, `literal<int32>`, and `add<int32>`
  family
- it does not redefine the shared adapter request or response contract
- checked-in TiKV differential pairwise drift reports now live in `inventory/first-expression-slice-tidb-vs-tikv-drift-report.{md,json}` and `inventory/first-expression-slice-tiflash-vs-tikv-drift-report.{md,json}`
- broader TiKV slice coverage remains follow-on work
