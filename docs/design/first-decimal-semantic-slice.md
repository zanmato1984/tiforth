# First Decimal Semantic Slice

Status: issue #189 design checkpoint

Verified: 2026-03-19

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #189 `design: define first decimal semantic slice boundary`

## Question

What is the first shared decimal semantic slice that can be specified and
tested without widening decimal arithmetic, coercion, or runtime-state policy
too early?

## Inputs Considered

- `docs/spec/type-system.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/milestone-1-nested-decimal-temporal-boundary.md`
- `docs/spec/first-filter-is-not-null.md`
- `tests/conformance/first-filter-is-not-null-slice.md`
- `tests/differential/first-filter-is-not-null-slice.md`
- `tests/conformance/first-decimal128-slice.md`
- `tests/differential/first-decimal128-slice.md`
- `adapters/first-decimal128-slice.md`
- issue #189

## Design Summary

The first decimal semantic slice is a narrow `decimal128` checkpoint:

- admitted decimal logical family: `decimal128(precision, scale)` with
  precision `1..=38` and scale `0..=precision`
- admitted expression family for this slice: passthrough `column(index)` over
  `decimal128` input columns
- admitted predicate family for this slice:
  `is_not_null(column(index))` with `decimal128` input
- no decimal arithmetic, casts, rescaling, or rounding policy in this
  checkpoint

This keeps the first decimal step aligned with existing expression and
predicate families while preserving current runtime and ownership contracts.

## Type And Nullability Semantics

For this first decimal slice:

- `column(index)` preserves logical type `decimal128`, source precision and
  scale metadata, and source nullability
- `is_not_null(column(index))` derives logical type `boolean` with
  `nullable = false`
- out-of-range `index` remains an execution error
- unsupported decimal logical families (for example `decimal256`) remain
  execution errors rather than implicit coercions
- invalid `decimal128` precision or scale metadata remains an execution error

## Data-Contract Boundary For The Slice

The shared data handoff boundary for this first decimal slice is:

- incoming and outgoing decimal arrays in shared execution are limited to
  logical `decimal128`
- decimal field precision and scale metadata must stay explicit in schema
  fields and must be preserved across passthrough and filter output in this
  slice
- this slice does not require implicit rescaling or reinterpretation across
  stage boundaries
- adapters may normalize engine-native decimal renderings, but shared
  differential comparison for this slice is over canonical decimal string
  values plus nullability
- reserve-before-allocate admission and claim-carrying handoff behavior remain
  unchanged from `docs/contracts/data.md`

## Coverage Anchor Docs

Issue #189 provides the first docs-first coverage-anchor set for this
checkpoint:

- conformance checkpoint doc:
  `tests/conformance/first-decimal128-slice.md`
- differential checkpoint doc:
  `tests/differential/first-decimal128-slice.md`
- adapter boundary doc: `adapters/first-decimal128-slice.md`
- local executable conformance coverage:
  `crates/tiforth-kernel/tests/decimal128_slice.rs`

Before kernel or adapter expansion claims broader decimal support, follow-on
issues should preserve these anchors or replace them explicitly.

## Out Of Scope For This Checkpoint

- decimal arithmetic (`add`, `sub`, `mul`, `div`, aggregate math)
- decimal cast or coercion rules
- decimal rounding and rescale policy
- decimal logical families beyond `decimal128`
- new runtime states or scheduler behavior

## Result

The first decimal checkpoint is now explicit and narrow: `decimal128`
column-passthrough plus `is_not_null` predicate semantics, with existing
runtime and ownership contracts unchanged and broader decimal policy deferred
to follow-on issues. Coverage anchors are fixed under
`tests/conformance/`, `tests/differential/`, and `adapters/`, and local
executable conformance now exists under `crates/tiforth-kernel/tests/`.
