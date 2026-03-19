# First Temporal Semantic Slice

Status: issue #174 design checkpoint

Verified: 2026-03-19

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #149 `milestone-1: implement first executable is_not_null filter slice`
- #174 `design: define first temporal semantic slice boundary`

## Question

What is the first shared temporal semantic slice that can be specified and
tested without widening the current runtime-state model or introducing broad
timestamp and timezone policy too early?

## Inputs Considered

- `docs/spec/type-system.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/milestone-1-nested-decimal-temporal-boundary.md`
- `docs/spec/first-filter-is-not-null.md`
- `tests/conformance/first-filter-is-not-null-slice.md`
- `tests/differential/first-filter-is-not-null-slice.md`
- issue #174

## Design Summary

The first temporal semantic slice is a narrow `date32` checkpoint:

- admitted temporal logical family: `date32` only
- admitted expression family for this slice: passthrough `column(index)` over
  `date32` input columns
- admitted predicate family for this slice: `is_not_null(column(index))` with
  `date32` input
- no temporal arithmetic, comparisons, casts, or truncation/extract families
  in this checkpoint

This keeps the first temporal step aligned with existing operator families and
existing runtime and ownership rules.

## Type And Nullability Semantics

For this first temporal slice:

- `column(index)` preserves logical type `date32` and source nullability
- `is_not_null(column(index))` derives logical type `boolean` with
  `nullable = false`
- out-of-range `index` remains an execution error
- unsupported temporal logical families remain execution errors rather than
  implicit coercions

## Data-Contract Boundary For The Slice

The shared data handoff boundary for this first temporal slice is:

- incoming or outgoing temporal arrays in shared execution are limited to
  logical `date32`
- this slice does not require batch-carried timezone metadata
- adapters may keep engine-local date or timezone session settings, but shared
  differential comparison for this slice is over normalized `date32` day-domain
  values plus nullability
- reserve-before-allocate admission and claim-carrying handoff behavior remain
  unchanged from `docs/contracts/data.md`

## Required Coverage Anchors

Before kernel or adapter expansion claims broader temporal support, follow-on
issues should define and exercise coverage anchored to:

- conformance checkpoint doc path:
  `tests/conformance/first-temporal-date32-slice.md`
- differential checkpoint doc path:
  `tests/differential/first-temporal-date32-slice.md`
- adapter boundary doc path: `adapters/first-temporal-date32-slice.md`

## Out Of Scope For This Checkpoint

- temporal logical families other than `date32` (`time32`, `time64`,
  `timestamp`, `duration`, `interval`)
- timezone-aware timestamp normalization or ordering policy
- temporal arithmetic, comparison, cast, extraction, or truncation semantics
- decimal-family semantics
- new runtime states or scheduler behavior

## Result

The first temporal checkpoint is now explicit and narrow: `date32`
column-passthrough plus `is_not_null` predicate semantics, with existing
runtime and ownership contracts unchanged and broader temporal policy deferred
to follow-on issues.
