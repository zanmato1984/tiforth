# First Map-Aware Handoff Slice

Status: issue #230 design checkpoint

Verified: 2026-03-19

Related issues:

- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #151 `design: define first nested-family shared handoff slice and claim-ownership boundary`
- #226 `design: define first struct nested handoff slice checkpoint`
- #230 `docs: define first map nested handoff slice checkpoint`

## Question

What is the first shared slice where `map` arrays may cross stage boundaries
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
- `tests/conformance/first-map-slice.md`
- `tests/differential/first-map-slice.md`
- `adapters/first-map-slice.md`
- issue #127
- issue #151
- issue #226
- issue #230

## Design Summary

The first map-aware shared handoff slice is a narrow passthrough path:

- the source -> projection -> sink runtime path may hand off incoming
  `map<int32, int32?>` columns without prior flattening when those columns are
  forwarded through `column(index)` projection expressions
- newly materialized projection outputs (`literal<int32>`, `add<int32>`) remain
  scalar decoded arrays and must not emit `map` output arrays in this slice
- predicate expansion over `map` is out of scope in this checkpoint;
  `is_not_null(column(index))` over `map` remains follow-on work
- adapters and harnesses may carry nested map values in normalized
  differential rows while semantic comparison continues to focus on row count,
  map-entry preservation, key and value nullability rules, and stable carrier
  structure

This keeps the first direct map-aware boundary small while preserving the
current milestone-1 expression and filter semantic core.

## Why This Slice

- it reuses an existing semantic family (`column(index)`) rather than adding a
  new nested compute family
- `map<int32, int32?>` is the smallest map shape that adds explicit key-domain
  and value-domain ownership expectations beyond the prior `list` and `struct`
  checkpoints
- it extends nested coverage without widening decimal, temporal, collation, or
  runtime-state scope
- it gives follow-on nested work (`union`, nested compute, nested predicates,
  nested casts) one explicit map-oriented claim-splitting precedent

## Ownership Claim Rules For `map<int32, int32?>` Handoff

For each admitted `map<int32, int32?>` column in this slice:

- claim tracking keeps parent map storage, entry-domain storage, key-domain
  storage, and value-domain storage in separate ownership units because their
  lifetimes may diverge
- parent-domain claims cover top-level map offsets buffers plus top-level
  validity bitmaps
- entry-domain claims cover map-entry structural buffers when those buffers are
  materialized separately from parent offsets
- key-domain claims cover map key `int32` payloads; keys remain non-null in
  this checkpoint
- value-domain claims cover map value `int32?` payloads plus value validity
  state
- if multiple columns or batches share one entry, key, or value payload, that
  payload should be tracked as one shared domain claim rather than duplicated
  byte accounting
- a stage may drop a parent-domain claim only when no outgoing column or
  retained state still references the corresponding top-level map buffers
- a stage may drop an entry, key, or value domain claim only when no outgoing
  column or retained state still references the corresponding payload

All other claim lifecycle rules from `docs/contracts/data.md` remain unchanged:
live forwarded claims are not shrunk in place, and final release is terminal.

## Coverage Anchors For This Checkpoint

Issue #230 defines the first docs-first coverage anchor set for this map
handoff checkpoint:

- conformance checkpoint doc: `tests/conformance/first-map-slice.md`
- differential checkpoint doc: `tests/differential/first-map-slice.md`
- adapter boundary doc: `adapters/first-map-slice.md`

## Out Of Scope For This Checkpoint

- nested families beyond this map checkpoint (`union` and nested combinations
  beyond current list, struct, and map passthrough checkpoints)
- nested compute semantics beyond passthrough `column(index)`
- nested predicate, cast, or ordering expansion over `map` values
- dictionary-encoded nested payload boundaries
- decimal and temporal metadata interpretation
- new runtime states, scheduler behavior, or kernel execution expansion

## Result

The shared data contract now has one additional post-milestone nested-aware
handoff checkpoint: passthrough of `map<int32, int32?>` through `column(index)`
while keeping ownership claims split across parent, entry, key, and value
domains.
