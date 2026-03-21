# First Collation-Sensitive String Slice

Status: issue #233 design checkpoint

Verified: 2026-03-21

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #139 `spec: define first filter semantic slice for is_not_null(column(index))`
- #143 `docs: define initial collation scope and ownership boundary`
- #233 `design: define first string collation semantic slice`
- #352 `kernel: add first-collation-string-slice local executable checkpoint`

## Question

What is the first shared collation-sensitive string checkpoint that can be
specified for conformance and differential coverage without prematurely
freezing broad string, binary, or locale policy?

## Inputs Considered

- `docs/spec/type-system.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/collation-scope-ownership.md`
- `tests/conformance/first-collation-string-slice.md`
- `tests/differential/first-collation-string-slice.md`
- `adapters/first-collation-string-slice.md`
- issue #233
- issue #352

## Design Summary

The first collation-sensitive string checkpoint is a narrow `utf8` boundary:

- admitted string logical family for this checkpoint: `utf8`
- admitted collation identifiers for this checkpoint:
  - `binary`: bytewise UTF-8 comparison and ordering
  - `unicode_ci`: Unicode case-insensitive comparison and ordering intent
- admitted expression family for this checkpoint: passthrough `column(index)`
  over `utf8` input columns
- admitted predicate family for this checkpoint:
  `is_not_null(column(index))` over `utf8` input
- admitted comparison and ordering probes for this checkpoint:
  `collation_eq(lhs, rhs, collation_ref)`,
  `collation_lt(lhs, rhs, collation_ref)`, and canonical
  `order_by(column(index), collation_ref, asc)` row normalization

This keeps the first collation step aligned with existing expression and
predicate families while making comparison, ordering, and collation identifiers
explicit.

## Type And Nullability Semantics

For this first collation checkpoint:

- `column(index)` preserves logical type `utf8` and source nullability
- `is_not_null(column(index))` derives logical type `boolean` with
  `nullable = false`
- `collation_eq(...)` and `collation_lt(...)` derive logical type `boolean`
- comparison nullability follows SQL null propagation: if either operand is SQL
  `NULL`, the comparison result is SQL `NULL`
- out-of-range `index` remains an execution error
- unknown `collation_ref` values are execution errors
- non-`utf8` comparison operands are execution errors for this checkpoint

## Collation Semantics For The Slice

For this first collation checkpoint:

- `binary` compares UTF-8 byte sequences directly for both equality and
  ordering intent
- `unicode_ci` compares strings using shared canonical collation keys:
  Unicode simple case-folded text normalized to NFC
- ordered comparisons under `unicode_ci` use those canonical keys; when keys
  are equal, comparison intent treats values as compare-equal
- canonical differential row ordering for `unicode_ci` uses deterministic
  tie-breakers only for carrier stability: original UTF-8 byte sequence, then
  original row position
- no implicit collation derivation or coercion is introduced; every
  collation-sensitive case must provide an explicit `collation_ref`

This checkpoint defines shared harness intent for collation-sensitive behavior
without claiming engine-level parity for every locale rule.

## Data-Contract Boundary For The Slice

The shared handoff and comparison boundary for this first collation slice is:

- shared normalized schema uses logical type token `utf8` for string columns
- differential request and response carriers include explicit `collation_ref`
  values for collation-sensitive cases
- normalized `rows[]` carry UTF-8 strings and SQL `NULL` as `null`
- reserve-before-allocate admission and claim-carrying handoff behavior remain
  unchanged from `docs/contracts/data.md`

This checkpoint does not require batch-level collation metadata in milestone-1
runtime carriers.

## Coverage Anchor Docs

Issue #233 defines the first docs-first coverage-anchor set for this
checkpoint. Issue #352 adds first executable local shared-kernel coverage
through `crates/tiforth-kernel/tests/collation_string_slice.rs` while
adapter and differential wiring remain follow-on scope:

- conformance checkpoint doc:
  `tests/conformance/first-collation-string-slice.md`
- differential checkpoint doc:
  `tests/differential/first-collation-string-slice.md`
- adapter boundary doc: `adapters/first-collation-string-slice.md`

Before kernel or adapter expansion claims broader collation support, follow-on
issues should preserve these anchors or replace them explicitly.

## Out Of Scope For This Checkpoint

- locale-specific collation families beyond `binary` and `unicode_ci`
- accent-insensitive, kana-sensitive, or width-sensitive policy expansion
- `binary` logical-type (`bytes`) collation semantics
- string function families beyond the comparison and ordering probes above
- new runtime states, scheduler behavior, or batch-carried collation metadata
- live differential artifact refresh for this slice

## Result

The first collation-sensitive string checkpoint is now explicit and narrow:
`utf8` passthrough plus `is_not_null` predicate coverage, with shared
`binary` and `unicode_ci` comparison and ordering intent fixed for conformance and differential coverage, and first local executable shared-kernel coverage now exists in `crates/tiforth-kernel/tests/collation_string_slice.rs`. Broader adapter wiring, differential harness execution, and wider collation or string-family behavior remain follow-on work.
