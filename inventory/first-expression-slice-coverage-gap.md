# First Expression Slice Coverage Gap

Status: issue #129 inventory checkpoint

Verified: 2026-03-18

Related issues:

- #68 `design: define first differential expression slice and drift report format`
- #84 `design: define first inventoried function and operator families`
- #113 `harness: compare first-expression-slice results for TiDB and TiFlash`
- #129 `inventory: add first-expression-slice coverage-gap artifact`

## Purpose

This note records deferred and unsupported semantic edges for `first-expression-slice` that remain after the current executable differential checkpoint.

It complements the checked-in `case-results` and `drift-report` artifacts by making unresolved coverage boundaries explicit and reviewable in one place.

## Shared Slice Anchors

This artifact stays anchored to the stable first-slice vocabulary from `tests/differential/first-expression-slice.md`.

- `slice_id = first-expression-slice`
- `input_ref = first-expression-slice-int32-basic`
- `input_ref = first-expression-slice-int32-nullable`
- `input_ref = first-expression-slice-int32-overflow`
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

## Gap Summary

- open coverage gaps: `4`
- unsupported compared case: `add-int32-overflow-error`
- deferred error-class expansion and deferred case families remain outside the first executable differential slice

## Coverage Gaps

### `literal-int32-output-narrowing`

- anchor refs: `case_id = literal-int32-seven`, `case_id = literal-int32-null`, `projection_ref = literal-int32-seven`, `projection_ref = literal-int32-null`
- current evidence:
  - `inventory/first-expression-slice-tidb-case-results.json` and `inventory/first-expression-slice-tiflash-case-results.json` both currently report `logical_type = int64` for these literal projection outputs
  - `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md` classifies both literal cases as `match`
- gap:
  - shared milestone-1 semantics define these as `literal<int32>(value)` outputs, but current adapter-normalized metadata is not narrowed back to shared `int32`
- follow-up direction:
  - define whether adapters should enforce explicit narrowing for first-slice literal outputs or whether shared first-slice metadata should accept widened literal result typing

### `tidb-int32-overflow-parity`

- anchor refs: `case_id = add-int32-overflow-error`, `input_ref = first-expression-slice-int32-overflow`, `projection_ref = add-a-plus-one`
- current evidence:
  - `inventory/first-expression-slice-tidb-vs-tiflash-drift-report.md` reports `status = unsupported` for this case
  - current normalized outcomes are `adapter_unavailable` on TiDB and `arithmetic_overflow` on TiFlash
- gap:
  - the first differential slice does not yet provide paired overflow behavior evidence for `add<int32>` at the shared `2147483647 + 1` boundary
- follow-up direction:
  - either add a TiDB-side narrowing strategy that can produce shared overflow behavior or keep this case explicitly unsupported with a documented contract decision

### `deferred-error-normalization-cases`

- anchor refs: first-slice defer list in `tests/differential/first-expression-slice.md`
- current evidence:
  - the first differential slice explicitly defers schema-binding and type-rejection cases such as `missing column` and `unsupported arithmetic type`
  - the same note ties that deferment to a later error-normalization pass beyond the current overflow-focused vocabulary
- gap:
  - cross-engine drift evidence for those error families is not yet collected under `first-expression-slice`
- follow-up direction:
  - extend normalized `error_class` handling and then add one narrow follow-on differential checkpoint for deferred schema-binding and type-rejection families

### `deferred-runtime-admission-ownership-differential`

- anchor refs: first-slice defer list in `tests/differential/first-expression-slice.md`
- current evidence:
  - local runtime and admission outcomes (claim handoff, shrink, release, cancellation, ownership violations) are intentionally out of scope for first-slice differential artifacts
  - those behaviors remain covered by local conformance fixtures rather than paired TiDB-versus-TiFlash differential artifacts
- gap:
  - no cross-engine differential coverage exists for runtime/admission/ownership families, and no adapter-visible differential carrier is defined for that family in this slice
- follow-up direction:
  - keep this family local-only unless a later accepted slice defines a shared adapter-visible runtime-event comparison surface

## Boundary For This Artifact

- this note records first-slice deferred and unsupported coverage only
- it does not change the first-slice shared semantics or current adapter request/response boundary
- it does not introduce new executable harness behavior by itself
