# First Float64 NaN/Infinity Ordering Slice

Status: issue #194 design checkpoint

Verified: 2026-03-19

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #194 `design: define first float64 NaN, infinity, and ordering checkpoint`

## Question

What is the first shared `float64` special-value and ordering checkpoint that
can be specified for conformance and differential coverage without widening
float arithmetic, cast, and coercion policy too early?

## Inputs Considered

- `docs/spec/type-system.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/spec/first-filter-is-not-null.md`
- `tests/conformance/first-filter-is-not-null-slice.md`
- `tests/conformance/first-float64-ordering-slice.md`
- `tests/differential/first-float64-ordering-slice.md`
- `adapters/first-float64-ordering-slice.md`
- issue #194

## Design Summary

The first float checkpoint is a narrow `float64` special-value and ordering
boundary:

- admitted floating logical family for this checkpoint: `float64`
- admitted expression family for this checkpoint: passthrough `column(index)`
  over `float64` input
- admitted predicate family for this checkpoint:
  `is_not_null(column(index))` over `float64` input
- this checkpoint defines shared NaN, infinity, signed-zero, and canonical
  ordering intent for harness comparison without adding float arithmetic or
  cast/coercion semantics

This keeps the first float step aligned with existing expression and predicate
families while making special-value comparison intent explicit.

## Type And Nullability Semantics

For this first float checkpoint:

- `column(index)` preserves logical type `float64` and source nullability
- `is_not_null(column(index))` derives logical type `boolean` with
  `nullable = false`
- `NaN`, `Infinity`, `-Infinity`, `0.0`, and `-0.0` are all non-null
  `float64` values for predicate evaluation
- equality intent for this checkpoint follows IEEE-style value semantics:
  `NaN` is not equal to any value (including `NaN`), while `0.0` and `-0.0`
  compare equal
- ordered-comparison intent for this checkpoint treats any comparison involving
  `NaN` as false; non-NaN values follow numeric order with
  `-Infinity < finite < Infinity`
- canonical differential ordering intent for this checkpoint is:
  `-Infinity < finite values < Infinity < NaN`, with `-0.0` ordered before
  `0.0` only as a stable tie-break during row canonicalization
- out-of-range `index` remains an execution error
- unsupported floating families (for example `float32`) remain execution errors
  rather than implicit widening

## Data-Contract Boundary For The Slice

The shared data handoff and comparison boundary for this first float checkpoint
is:

- incoming and outgoing floating arrays in the shared slice are limited to
  logical `float64`
- adapters may use engine-native float rendering internally, but normalized
  differential `rows[]` for this slice should use canonical value tokens:
  `-Infinity`, `Infinity`, `NaN`, and canonical finite decimal strings,
  including signed-zero distinction (`-0.0`, `0.0`)
- reserve-before-allocate admission and claim-carrying handoff behavior remain
  unchanged from `docs/contracts/data.md`

## Coverage Anchor Docs

Issue #194 defines the first docs-first coverage-anchor set for this
checkpoint:

- conformance checkpoint doc:
  `tests/conformance/first-float64-ordering-slice.md`
- differential checkpoint doc:
  `tests/differential/first-float64-ordering-slice.md`
- adapter boundary doc: `adapters/first-float64-ordering-slice.md`

Before kernel or adapter expansion claims broader float support, follow-on
issues should preserve these anchors or replace them explicitly.

## Out Of Scope For This Checkpoint

- float arithmetic semantics (`add`, `sub`, `mul`, `div`, aggregates)
- float cast and coercion policy beyond existing initial lattice checkpoints
- NaN payload preservation rules
- cross-family float/decimal or float/integer coercion expansion
- new runtime states or scheduler behavior

## Result

The first float checkpoint is now explicit and narrow: `float64`
column-passthrough plus `is_not_null` predicate coverage, with shared NaN,
infinity, signed-zero, and canonical ordering intent documented for harness
comparison. Broader floating semantics remain follow-on work.
