# First Dictionary-Aware Handoff Slice

Status: issue #145 design checkpoint

Verified: 2026-03-18

Related issues:

- #90 `design: define dictionary-encoding boundary in shared data contract`
- #145 `design: define first dictionary-aware shared handoff slice`

## Question

What is the first shared slice where dictionary-encoded arrays may cross
stage boundaries without prior normalization, and how do ownership claims
attach in that slice?

## Inputs Considered

- `README.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/spec/type-system.md`
- `docs/spec/milestone-1-expression-projection.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/differential/first-expression-slice.md`
- issue #90
- issue #145

## Design Summary

The first dictionary-aware shared handoff slice is a narrow passthrough path:

- the source -> projection -> sink runtime path may hand off incoming
  `dictionary<int32, int32>` columns without prior decoding when those columns
  are forwarded through `column(index)` projection expressions
- newly materialized projection outputs (`literal<int32>`, `add<int32>`) remain
  ordinary decoded arrays and must not emit dictionary-encoded output
- adapters and harnesses may carry dictionary layout in runtime handoff
  evidence, but first-slice differential comparison remains based on logical
  type, nullability, row count, and row values

This keeps the first direct dictionary-aware boundary small while preserving
the current semantic comparison surface.

## Why This Slice

- it reuses an existing semantic family (`column(index)`) instead of adding a
  new operator or function family
- it allows direct dictionary-aware handoff only where zero-copy forwarding is
  already expected
- it avoids widening milestone-1 compute typing, arithmetic, or result-shape
  semantics
- it resolves claim-ownership ambiguity for dictionary values and index buffers
  before wider family support

## Ownership Claim Rules For Dictionary Handoff

For each dictionary-backed column admitted into this first slice:

- claim tracking keeps dictionary index storage and dictionary values storage in
  separate ownership units because their lifetimes may diverge
- index-domain claims cover dictionary index values plus index validity bits
- values-domain claims cover dictionary values buffers and any values-level
  validity bits
- if multiple columns share one dictionary values payload, that shared values
  payload should be tracked as one shared values-domain claim rather than
  duplicated byte accounting
- a stage may drop an index-domain claim only when the outgoing batch and
  retained state no longer reference that dictionary index buffer
- a stage may drop a values-domain claim only when no outgoing column or
  retained state still references that dictionary values payload

All other claim lifecycle rules from `docs/contracts/data.md` remain unchanged:
live forwarded claims are not shrunk in place, and final release is terminal.

## Out Of Scope For This Checkpoint

- dictionary value families other than logical `int32`
- dictionary materialization or re-encoding by projection operators
- implicit decoding for arithmetic over dictionary-backed operands inside the
  current executable milestone-1 kernel slice
- direct nested, decimal, or temporal dictionary-value support
- new runtime states or scheduler behavior

## Result

The shared data contract now has one concrete post-milestone-1 dictionary-aware
handoff checkpoint: passthrough of `dictionary<int32, int32>` through
`column(index)` while keeping computed outputs decoded and claim ownership split
between index-domain and values-domain bytes.
