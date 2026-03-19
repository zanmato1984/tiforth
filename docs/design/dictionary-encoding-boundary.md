# Dictionary Encoding Boundary

Status: issue #90 design checkpoint

Verified: 2026-03-18

Related issues:

- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #68 `design: define first differential expression slice and drift report format`
- #74 `spec: define milestone-1 int32 type-system boundary`
- #90 `design: define dictionary-encoding boundary in shared data contract`
- #145 `design: define first dictionary-aware shared handoff slice`

## Question

How should the shared Arrow-oriented data contract treat dictionary-encoded arrays, and what does milestone 1 require today?

## Inputs Considered

- `README.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/contracts/data.md`
- `docs/spec/type-system.md`
- `docs/spec/milestone-1-expression-projection.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/differential/first-expression-slice.md`
- `adapters/first-expression-slice.md`
- issue #90

## Design Summary

Dictionary encoding is a physical Arrow representation detail, not a separate shared semantic type family.

For milestone 1:

- stage handoff inside the current shared slice is defined only for decoded logical arrays
- adapters or sources that observe dictionary-backed engine data must normalize it before the current shared slice or checked-in differential evidence uses it
- current milestone-1 projection outputs must not emit dictionary-encoded arrays
- later direct dictionary-aware handoff is gated by naming the affected slice, supported logical families, and ownership-accounting rules; issue #145 is the first accepted checkpoint under that gate

## Why This Boundary

- the current milestone-1 families are `column`, `literal<int32>`, and `add<int32>` over `int32` arrays; none requires dictionary layout to express the documented semantics
- keeping dictionary layout out of current handoff prevents low-cardinality physical choices from leaking into the shared semantic type system or the first differential artifacts
- normalizing engine-native dictionary evidence to logical values keeps cross-engine comparison aligned with docs that already compare type, nullability, row count, and row values rather than physical plans or storage details
- leaving a later explicit opt-in path preserves the Arrow-native direction without forcing premature support promises

## Milestone-1 Boundary

### Shared Semantics

- type identity continues to be the underlying logical value type
- `dictionary<index, values>` is not a new shared semantic family by itself
- nullability, overflow, and row-wise semantics continue to follow the decoded logical type

### Current Stage Handoff

- current milestone-1 stage boundaries inside `tiforth` exchange decoded logical arrays only
- a milestone-1 producer that materializes new output must emit ordinary decoded Arrow arrays, not dictionary arrays
- live claim accounting in the current slice therefore tracks only the reachable decoded output buffers attached to the emitted batch

### Adapter And Harness Normalization

- if a future adapter or source sees dictionary-backed engine-native data for a documented milestone-1 case, it should decode or otherwise normalize that data before building the shared input or normalized case-result carrier
- checked-in differential or inventory evidence for current slices should record the logical outcome, not make dictionary layout itself part of the match surface
- raw engine-native evidence may still mention dictionary layout in issue or PR discussion when it helps explain drift, but that detail is not yet shared-contract output

## Follow-On Boundary

Issue #145 defines the first direct dictionary-aware handoff checkpoint in
`docs/design/first-dictionary-aware-handoff-slice.md`.

That follow-on keeps scope narrow:

- passthrough `dictionary<int32, int32>` through `column(index)` is allowed
- newly materialized projection outputs stay decoded
- dictionary index storage and dictionary values storage use separate claim
  ownership units when their lifetimes can diverge

## Result

For now, milestone 1 keeps dictionary encoding as an adapter-local or engine-local representation detail that must be normalized away before data crosses the current shared contract.
