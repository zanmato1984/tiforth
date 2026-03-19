# First Nested-Aware Handoff Slice

Status: issue #151 design checkpoint

Verified: 2026-03-18

Related issues:

- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #151 `design: define first nested-family shared handoff slice and claim-ownership boundary`

## Question

What is the first shared slice where nested arrays may cross stage boundaries
directly, and how should ownership claims attach in that slice?

## Inputs Considered

- `README.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/spec/type-system.md`
- `docs/design/milestone-1-nested-decimal-temporal-boundary.md`
- `docs/design/first-dictionary-aware-handoff-slice.md`
- `docs/spec/milestone-1-expression-projection.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/differential/first-expression-slice.md`
- issue #127
- issue #151

## Design Summary

The first nested-aware shared handoff slice is a narrow passthrough path:

- the source -> projection -> sink runtime path may hand off incoming
  `list<int32>` columns without prior flattening when those columns are
  forwarded through `column(index)` projection expressions
- newly materialized projection outputs (`literal<int32>`, `add<int32>`) remain
  scalar decoded arrays and must not emit nested output arrays
- adapters and harnesses may carry nested layout in runtime handoff evidence,
  while semantic comparison continues to focus on row count, logical element
  values, and nullability

This keeps the first direct nested-aware boundary small while preserving the
current milestone-1 expression and filter semantic core.

## Why This Slice

- it reuses an existing semantic family (`column(index)`) rather than adding a
  new nested operator family
- `list<int32>` is the smallest nested shape with independently meaningful
  parent-domain and child-domain buffer ownership
- it resolves first-pass nested claim ownership without widening arithmetic,
  predicate, decimal, temporal, or collation scope
- it allows later nested families to build on one explicit claim-splitting
  precedent

## Ownership Claim Rules For `list<int32>` Handoff

For each admitted `list<int32>` column in this slice:

- claim tracking keeps parent list storage and child values storage in separate
  ownership units because their lifetimes may diverge
- parent-domain claims cover list offsets buffers plus parent-level validity
  bitmaps
- child-domain claims cover child `int32` values buffers plus child-level
  validity bitmaps
- if multiple columns or batches share one child values payload, that payload
  should be tracked as one shared child-domain claim rather than duplicated
  byte accounting
- a stage may drop a parent-domain claim only when no outgoing column or
  retained state still references the corresponding parent list buffers
- a stage may drop a child-domain claim only when no outgoing column or
  retained state still references the corresponding child values payload

All other claim lifecycle rules from `docs/contracts/data.md` remain unchanged:
live forwarded claims are not shrunk in place, and final release is terminal.

## Out Of Scope For This Checkpoint

- nested families beyond `list<int32>` (`large_list`, `fixed_size_list`,
  `struct`, `map`, `union`, and nested combinations)
- nested compute semantics beyond passthrough `column(index)`
- dictionary-encoded nested payload boundaries
- decimal and temporal metadata interpretation
- new runtime states, scheduler behavior, or kernel execution expansion

## Result

The shared data contract now has one concrete post-milestone-1 nested-aware
handoff checkpoint: passthrough of `list<int32>` through `column(index)` while
keeping ownership claims split between parent-domain and child-domain bytes.
