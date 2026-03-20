# First JSON Semantic Slice

Status: issue #224 design checkpoint

Verified: 2026-03-19

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #224 `design: define first JSON semantic slice boundary`

## Question

What is the first shared JSON semantic slice that can be specified for
conformance and differential coverage without prematurely freezing broad JSON
function, ordering, or cast behavior?

## Inputs Considered

- `docs/spec/type-system.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/spec/first-filter-is-not-null.md`
- `tests/conformance/first-json-slice.md`
- `tests/differential/first-json-slice.md`
- `adapters/first-json-slice.md`
- issue #224

## Design Summary

The first JSON semantic slice is a narrow docs-first checkpoint:

- admitted JSON logical family: `json`
- admitted expression family for this slice: passthrough `column(index)` over
  `json` input columns
- admitted predicate family for this slice:
  `is_not_null(column(index))` over `json` input
- initial comparability boundary: harness-level equality over canonical JSON
  value tokens; no shared ordering semantics
- initial cast boundary: no implicit casts involving `json`, and explicit
  non-identity casts involving `json` are out of scope for this checkpoint

This keeps first JSON coverage aligned with existing expression and predicate
families while making comparability and cast boundaries explicit.

## Type And Nullability Semantics

For this first JSON slice:

- `column(index)` preserves logical type `json` and source nullability
- `is_not_null(column(index))` derives logical type `boolean` with
  `nullable = false`
- SQL `NULL` remains a nullable-row outcome; JSON literal `null` remains a
  non-null `json` value
- out-of-range `index` remains an execution error
- unsupported JSON comparison operators and unsupported JSON cast requests are
  execution errors in this checkpoint

## Comparability Boundary For The Slice

For this first JSON checkpoint, shared comparability is intentionally narrow:

- cross-engine row comparison for non-null JSON values uses canonical JSON
  value tokens
- canonical JSON tokens are minified JSON text with deterministic object-key
  ordering, stable scalar spellings, and preserved array element order
- two non-null JSON values compare equal for this checkpoint only when their
  canonical JSON value tokens match exactly
- ordered comparisons (`<`, `<=`, `>`, `>=`) over `json` are out of scope and
  normalize as `unsupported_json_comparison`

This comparability boundary is for shared conformance and differential
coverage. It does not claim engine-level SQL JSON comparison parity beyond that
normalized carrier.

## Cast Boundary For The Slice

For this first JSON checkpoint:

- no implicit cast edges are added to or from `json`
- explicit casts involving `json` and a different logical family are out of
  scope for shared semantics in this checkpoint
- unsupported explicit cast requests normalize as `unsupported_json_cast`

This resolves the first JSON cast boundary without choosing broad parse,
coercion, or serialization policy yet.

## Data-Contract Boundary For The Slice

The shared handoff and comparison boundary for this first JSON slice is:

- shared normalized schema uses logical type token `json` for JSON columns
- differential `rows[]` encode non-null JSON values as canonical JSON value
  tokens carried as JSON strings
- SQL `NULL` remains `null` in normalized `rows[]`, so SQL `NULL` and JSON
  literal `null` stay distinguishable
- reserve-before-allocate admission and claim-carrying handoff behavior remain
  unchanged from `docs/contracts/data.md`

This checkpoint does not freeze one new Arrow physical JSON encoding
requirement for local kernel execution.

## Coverage Anchor Docs

Issue #224 provides the first docs-first coverage-anchor set for this
checkpoint:

- conformance checkpoint doc: `tests/conformance/first-json-slice.md`
- differential checkpoint doc: `tests/differential/first-json-slice.md`
- adapter boundary doc: `adapters/first-json-slice.md`

Before kernel or adapter expansion claims broader JSON support, follow-on
issues should preserve these anchors or replace them explicitly.

## Out Of Scope For This Checkpoint

- JSON path extraction or mutation families
- JSON containment and existence operators
- JSON ordering semantics beyond normalized equality-style comparison
- JSON arithmetic or aggregation semantics
- explicit JSON cast success semantics (for example `utf8 -> json` parse rules)
- new runtime states or scheduler behavior

## Result

The first JSON checkpoint is now explicit and narrow: `json`
column-passthrough plus `is_not_null` predicate coverage, with canonical
value-token comparability and unsupported-cast boundary semantics defined for
docs-first conformance and differential coverage. Broader JSON comparison,
cast, and function-family behavior remains follow-on work.
