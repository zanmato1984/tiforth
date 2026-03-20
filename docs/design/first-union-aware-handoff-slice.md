# First Union-Aware Handoff Slice

Status: issue #241 design checkpoint

Verified: 2026-03-19

Related issues:

- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #151 `design: define first nested-family shared handoff slice and claim-ownership boundary`
- #226 `design: define first struct nested handoff slice checkpoint`
- #230 `docs: define first map nested handoff slice checkpoint`
- #241 `docs: define first union nested handoff slice checkpoint`

## Question

What is the first shared slice where `union` arrays may cross stage boundaries
directly, and how should ownership claims attach in that slice?

## Inputs Considered

- `README.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/spec/type-system.md`
- `docs/design/milestone-1-nested-decimal-temporal-boundary.md`
- `docs/design/first-nested-aware-handoff-slice.md`
- `docs/design/first-struct-aware-handoff-slice.md`
- `docs/design/first-map-aware-handoff-slice.md`
- `tests/conformance/first-union-slice.md`
- `tests/differential/first-union-slice.md`
- `adapters/first-union-slice.md`
- issue #127
- issue #151
- issue #226
- issue #230
- issue #241

## Design Summary

The first union-aware shared handoff slice is a narrow passthrough path:

- the source -> projection -> sink runtime path may hand off incoming
  `dense_union<i:int32, n:int32?>` columns without prior flattening when those
  columns are forwarded through `column(index)` projection expressions
- newly materialized projection outputs (`literal<int32>`, `add<int32>`) remain
  scalar decoded arrays and must not emit `union` output arrays in this slice
- predicate expansion over `union` is out of scope in this checkpoint;
  `is_not_null(column(index))` over `union` remains follow-on work
- adapters and harnesses may carry nested union values in normalized
  differential rows while semantic comparison continues to focus on row count,
  variant-tag preservation, and per-variant value plus nullability
  preservation

This keeps the first direct union-aware boundary small while preserving the
current milestone-1 expression and filter semantic core.

## Why This Slice

- it reuses an existing semantic family (`column(index)`) rather than adding a
  new nested compute family
- `dense_union<i:int32, n:int32?>` is the smallest union shape that requires
  explicit type-id-domain and dense-offset-domain ownership alongside
  independently meaningful child-domain ownership
- it extends nested coverage beyond the earlier `list`, `struct`, and `map`
  checkpoints without widening decimal, temporal, collation, or runtime-state
  scope
- it gives follow-on nested work (`sparse_union`, nested compute, nested
  predicates, nested casts) one explicit union-oriented claim-splitting
  precedent

## Ownership Claim Rules For `dense_union<i:int32, n:int32?>` Handoff

For each admitted `dense_union<i:int32, n:int32?>` column in this slice:

- claim tracking keeps type-id-domain storage, dense-offset-domain storage, and
  each child-variant payload in separate ownership units because their
  lifetimes may diverge
- type-id-domain claims cover the union type-id buffer
- dense-offset-domain claims cover the union dense-offset buffer
- child-domain claims cover each variant payload independently:
  `i:int32` values plus `n:int32?` values and nullability state
- if multiple columns or batches share one child payload, that payload should
  be tracked as one shared child-domain claim rather than duplicated byte
  accounting
- a stage may drop type-id-domain or dense-offset-domain claims only when no
  outgoing column or retained state still references the corresponding buffers
- a stage may drop a child-domain claim only when no outgoing column or
  retained state still references the corresponding child payload

All other claim lifecycle rules from `docs/contracts/data.md` remain unchanged:
live forwarded claims are not shrunk in place, and final release is terminal.

## Coverage Anchors For This Checkpoint

Issue #241 defines the first docs-first coverage anchor set for this union
handoff checkpoint:

- conformance checkpoint doc: `tests/conformance/first-union-slice.md`
- differential checkpoint doc: `tests/differential/first-union-slice.md`
- adapter boundary doc: `adapters/first-union-slice.md`

## Out Of Scope For This Checkpoint

- union modes beyond this checkpoint (`sparse_union` and wider child sets)
- nested compute semantics beyond passthrough `column(index)`
- nested predicate, cast, or ordering expansion over `union` values
- dictionary-encoded nested payload boundaries
- decimal and temporal metadata interpretation
- new runtime states, scheduler behavior, or kernel execution expansion

## Result

The shared data contract now has one additional post-milestone nested-aware
handoff checkpoint: passthrough of `dense_union<i:int32, n:int32?>` through
`column(index)` while keeping ownership claims split across type-id, dense
offset, and per-child payload domains.
