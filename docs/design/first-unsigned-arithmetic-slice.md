# First Unsigned Arithmetic Slice

Status: issue #300 design checkpoint, issue #308 local executable kernel checkpoint

Verified: 2026-03-20

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #141 `spec: define signed/unsigned interaction checkpoint for initial coercion lattice`
- #276 `spec: define overflow behavior checkpoint for follow-on operator families`
- #300 `design: define first unsigned arithmetic semantic slice boundary`
- #308 `milestone-1: implement first executable unsigned arithmetic slice in local kernel`

## Question

What is the first shared unsigned arithmetic checkpoint that can be specified
for conformance and differential coverage without broad unsigned-family
expansion or implicit signed/unsigned coercion?

## Inputs Considered

- `docs/spec/type-system.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `tests/conformance/signed-unsigned-interaction-checkpoint.md`
- `tests/conformance/first-unsigned-arithmetic-slice.md`
- `tests/differential/first-unsigned-arithmetic-slice.md`
- `adapters/first-unsigned-arithmetic-slice.md`
- issue #300
- issue #308

## Design Summary

The first unsigned arithmetic checkpoint is a narrow docs-first boundary:

- admitted unsigned arithmetic logical type for this checkpoint: `uint64`
- admitted expression and predicate families for this checkpoint:
  `column(index)`, `literal<uint64>(value)`, `add<uint64>(lhs, rhs)`, and
  `is_not_null(column(index))` over `uint64`
- no implicit signed/unsigned coercion is introduced
- overflow in `add<uint64>` is an execution error

This keeps the first unsigned step aligned with existing milestone-1 families
while making unsigned arithmetic behavior explicit.

## Type And Nullability Semantics

For this first unsigned arithmetic slice:

- `column(index)` preserves logical type `uint64` and source nullability
- `literal<uint64>(value)` derives logical type `uint64`; non-null literals are
  `nullable = false`, and `NULL` literals are `nullable = true`
- `add<uint64>(lhs, rhs)` requires both operands to resolve to `uint64`
- `add<uint64>(lhs, rhs)` derives logical type `uint64`
- `add<uint64>(lhs, rhs)` derives result nullability as
  `lhs.nullable OR rhs.nullable`
- row-wise overflow in `add<uint64>` is an execution error; this checkpoint does
  not wrap, saturate, widen, or coerce overflowed results
- mixed signed/unsigned arithmetic requests (for example `int64` with `uint64`)
  are execution errors in this checkpoint
- out-of-range `index` remains an execution error

## Coercion And Signature Boundary For The Slice

For this first unsigned checkpoint:

- exact unsigned signature matches are allowed when a family defines them
- no implicit cast edges are added between signed and unsigned families
- no implicit cast edges are added from `uint64` to `float64` for arithmetic
  signature rescue
- untyped `NULL` may adopt a selected `uint64` signature type after signature
  selection, but does not choose between signed and unsigned candidates

This keeps signed/unsigned behavior aligned with
`tests/conformance/signed-unsigned-interaction-checkpoint.md`.

## Data-Contract Boundary For The Slice

The shared handoff and comparison boundary for this first unsigned slice is:

- shared normalized schema uses logical type token `uint64`
- differential `rows[]` encode non-null `uint64` values as canonical base-10
  integer strings so values above JavaScript-safe integer range remain exact
- SQL `NULL` remains `null` in normalized `rows[]`
- reserve-before-allocate admission and claim-carrying handoff behavior remain
  unchanged from `docs/contracts/data.md`

This checkpoint does not freeze a new Arrow physical encoding requirement beyond
shared logical `uint64` semantics.

## Coverage Anchor Docs

Issue #300 provides the first docs-first coverage-anchor set for this
checkpoint:

- conformance checkpoint doc:
  `tests/conformance/first-unsigned-arithmetic-slice.md`
- differential checkpoint doc:
  `tests/differential/first-unsigned-arithmetic-slice.md`
- adapter boundary doc: `adapters/first-unsigned-arithmetic-slice.md`

Issue #308 adds the first executable local shared-kernel coverage for this
checkpoint in:

- `crates/tiforth-kernel/tests/unsigned_arithmetic_slice.rs`

Before kernel or adapter expansion claims broader unsigned support, follow-on
issues should preserve these anchors or replace them explicitly.

## Out Of Scope For This Checkpoint

- unsigned families beyond `uint64`
- unsigned arithmetic families beyond `add<uint64>`
- unsigned cast, coercion, or rounding policy beyond this narrow checkpoint
- mixed signed/unsigned success semantics
- TiKV-specific adapter boundary and harness checkpoints
- new runtime states or scheduler behavior

## Result

The first unsigned arithmetic checkpoint is now explicit and narrow: `uint64`
column passthrough, `literal<uint64>`, `add<uint64>` overflow-as-error, and
`is_not_null` coverage anchors are fixed for docs-first conformance and
differential work, and local executable shared-kernel coverage now exists in
`crates/tiforth-kernel/tests/unsigned_arithmetic_slice.rs`. Broader unsigned
family semantics plus adapter and differential execution remain follow-on work.
