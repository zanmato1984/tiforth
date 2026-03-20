# First Struct-Aware Handoff Slice

Status: issue #226 design checkpoint, issue #329 local executable kernel checkpoint

Verified: 2026-03-20

Related issues:

- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #151 `design: define first nested-family shared handoff slice and claim-ownership boundary`
- #226 `design: define first struct nested handoff slice checkpoint`
- #329 `milestone-1: implement first executable struct slice in local kernel`

## Question

What is the first shared slice where `struct` arrays may cross stage boundaries
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
- `tests/conformance/first-struct-slice.md`
- `tests/differential/first-struct-slice.md`
- `adapters/first-struct-slice.md`
- issue #127
- issue #151
- issue #226
- issue #329

## Design Summary

The first struct-aware shared handoff slice is a narrow passthrough path:

- the source -> projection -> sink runtime path may hand off incoming
  `struct<a:int32, b:int32?>` columns without prior flattening when those
  columns are forwarded through `column(index)` projection expressions
- newly materialized projection outputs (`literal<int32>`, `add<int32>`) remain
  scalar decoded arrays and must not emit `struct` output arrays in this slice
- predicate expansion over `struct` is out of scope in this checkpoint;
  `is_not_null(column(index))` over `struct` remains follow-on work
- adapters and harnesses may carry nested struct values in normalized
  differential rows while semantic comparison continues to focus on row count,
  field order, nullability, and child-value preservation

This keeps the first direct struct-aware boundary small while preserving the
current milestone-1 expression and filter semantic core.

## Why This Slice

- it reuses an existing semantic family (`column(index)`) rather than adding a
  new nested compute family
- `struct<a:int32, b:int32?>` is the smallest named-field nested shape with
  independently meaningful parent-domain and per-child claim ownership
- it extends nested coverage beyond the earlier `list<int32>` checkpoint without
  widening decimal, temporal, collation, or runtime-state scope
- it gives follow-on nested work (`map`, `union`, nested compute) one explicit
  field-oriented claim-splitting precedent

## Ownership Claim Rules For `struct<a:int32, b:int32?>` Handoff

For each admitted `struct<a:int32, b:int32?>` column in this slice:

- claim tracking keeps parent struct storage and each child-field storage in
  separate ownership units because their lifetimes may diverge
- parent-domain claims cover top-level struct validity bitmaps
- child-domain claims cover each child field independently:
  `a:int32` values plus validity state, and `b:int32?` values plus validity
  state
- if multiple columns or batches share one child payload, that payload should
  be tracked as one shared child-domain claim rather than duplicated byte
  accounting
- a stage may drop a parent-domain claim only when no outgoing column or
  retained state still references the corresponding top-level struct buffers
- a stage may drop a child-domain claim only when no outgoing column or
  retained state still references the corresponding child payload

All other claim lifecycle rules from `docs/contracts/data.md` remain unchanged:
live forwarded claims are not shrunk in place, and final release is terminal.

## Coverage Anchors For This Checkpoint

Issue #226 defines the first docs-first coverage anchor set for this struct
handoff checkpoint:

- conformance checkpoint doc: `tests/conformance/first-struct-slice.md`
- differential checkpoint doc: `tests/differential/first-struct-slice.md`
- adapter boundary doc: `adapters/first-struct-slice.md`

Issue #329 adds the first executable local shared-kernel coverage for the same
checkpoint in:

- `crates/tiforth-kernel/tests/struct_slice.rs`

## Out Of Scope For This Checkpoint

- nested families beyond this struct checkpoint (`union` and nested
  combinations beyond current list, struct, and map passthrough checkpoints)
- nested compute semantics beyond passthrough `column(index)`
- filter predicate, cast, or ordering expansion over nested struct values
- dictionary-encoded nested payload boundaries
- decimal and temporal metadata interpretation
- new runtime states, scheduler behavior, or kernel execution expansion

## Result

The shared data contract now has one additional post-milestone nested-aware
handoff checkpoint: passthrough of `struct<a:int32, b:int32?>` through
`column(index)` while keeping ownership claims split between parent-domain and
per-child-domain bytes. Local executable shared-kernel conformance coverage for
this checkpoint now exists in `crates/tiforth-kernel/tests/struct_slice.rs`.
