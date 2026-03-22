# Numeric `add/plus` Family Completion Boundary

Status: issue #422 docs-first checkpoint

Verified: 2026-03-22

Related issues:

- #409 `epic: complete function-family program`
- #414 `spec: pin the first complete function family as numeric add/plus`
- #418 `design: fix TiDB-to-Arrow mapping for the numeric add/plus family`
- #420 `design: map the numeric add/plus family to tipb/kvproto enums`
- #422 `spec: complete the numeric add/plus family boundary`

## Question

What exact shared boundary must exist before `tiforth` can claim that the
first numeric `add/plus` family is complete under the active function-family
epic?

## Inputs Considered

- `docs/spec/functions/README.md`
- `docs/spec/functions/numeric-add-family.md`
- `docs/spec/type-system.md`
- `docs/contracts/data.md`
- `docs/design/first-tidb-arrow-type-mapping-boundary.md`
- `docs/design/first-add-family-tipb-kvproto-enum-reuse.md`
- `docs/design/first-unsigned-arithmetic-slice.md`
- `docs/design/first-float64-ordering-slice.md`
- `docs/design/first-decimal-semantic-slice.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/conformance/first-unsigned-arithmetic-slice.md`
- `tests/differential/first-expression-slice.md`
- `tests/differential/first-unsigned-arithmetic-slice.md`
- `adapters/first-expression-slice.md`
- `adapters/first-unsigned-arithmetic-slice.md`

## Design Summary

The numeric `add/plus` family is not complete just because the repo has one
`add<int32>` checkpoint and one `add<uint64>` checkpoint.

For the active epic, family completion means all of these are true:

- shared docs expose one canonical `add` family entry rather than a pile of
  unrelated per-type arithmetic specs
- exact numeric overload groups reuse one family identity and one
  generic-first implementation shape
- type-system docs make signature selection, coercion, result-type derivation,
  nullability, and error boundaries explicit for the admitted overload groups
- adapter and harness docs know which narrow executable slices must exist
  before the family can be claimed complete
- checked-in evidence proves the admitted overload groups through the existing
  slice-carrier model rather than through one family-global ad hoc artifact

This document fixes that completion checklist. It does not itself claim that
every required overload group is already executable today.

## Shared Spec Entry

This file is the canonical shared spec entry for the numeric `add/plus`
family-completion program.

The surrounding doc ownership is:

- `docs/spec/functions/numeric-add-family.md`: why this family is the first
  one to complete
- this file: what must be true before that family can be claimed complete
- `docs/spec/type-system.md`: family-wide signature selection, coercion,
  nullability, overflow, and result-derivation consequences
- `docs/design/first-tidb-arrow-type-mapping-boundary.md`: Arrow-native versus
  metadata-carrying mapping policy for the admitted overload groups
- `docs/design/first-add-family-tipb-kvproto-enum-reuse.md`: protocol-facing
  enum reuse for the admitted overload groups

Shared docs should keep using `add` as the family name. `plus` and SQL `+`
remain accepted external spellings for the same family, not separate family
entries.

## Required Overload Groups

For the current epic, the first complete numeric family is the first admitted
cross-type checkpoint set, not every numeric width or every engine-local SQL
arithmetic spelling ever supported by TiDB.

The completion boundary therefore requires these overload groups:

- exact signed integer add:
  `add<int32>(lhs, rhs)` and `add<int64>(lhs, rhs)`
- exact unsigned integer add:
  `add<uint64>(lhs, rhs)`
- exact floating-point add:
  `add<float64>(lhs, rhs)`
- exact decimal add:
  `add<decimal128(precision, scale)>(lhs, rhs)` once decimal result-derivation
  policy is accepted

Current repo evidence already anchors two of those groups:

- `add<int32>(lhs, rhs)` through `first-expression-slice`
- `add<uint64>(lhs, rhs)` through `first-unsigned-arithmetic-slice`

The remaining required groups for a completion claim are:

- signed widening through `add<int64>`
- floating arithmetic through `add<float64>`
- decimal arithmetic through `add<decimal128(..., ...)>`

## Generic-First Overload Reuse Boundary

Family completion requires one shared add-family implementation shape with
type-specific policy hooks, not separate shared families such as
`add_int32`, `add_uint64`, `add_float64`, or `add_decimal`.

For the admitted overload groups:

- operand dispatch first selects one shared `add` family candidate set
- exact signed integer overloads reuse the same family shape with width-specific
  overflow checking and result typing
- exact unsigned integer overloads reuse that same shape, but keep explicit
  unsigned overflow and signedness boundaries
- exact floating-point overloads reuse that same shape, but follow `float64`
  value semantics and normalized special-value comparison rules instead of
  integer overflow-as-error behavior
- exact decimal overloads reuse that same shape, but need explicit precision,
  scale, overflow, and rescale hooks before they are admitted

Shared docs, adapters, and harnesses should therefore talk about one numeric
`add/plus` family whose overload identity is carried by logical operand and
result types, not by separate family names.

## Type-System And Coercion Boundary

The completion boundary reuses the current shared selection order from
`docs/spec/type-system.md` and makes the add-family consequences explicit.

### Candidate Selection

For the admitted completion set:

1. choose an exact overload when one exists
2. otherwise apply only the currently admitted widening ladder
   `int32 < int64 < float64`
3. do not form candidates through signed-to-unsigned, unsigned-to-signed,
   decimal-to-non-decimal, or non-decimal-to-decimal implicit coercions
4. let untyped `NULL` adopt the selected overload type after signature
   selection, but do not let `NULL` choose between overload groups by itself

That yields these family-level outcomes:

- `int32 + int32 -> add<int32>`
- `int32 + int64`, `int64 + int32`, and `int64 + int64 -> add<int64>`
- `int32 + float64`, `int64 + float64`, `float64 + int32`,
  `float64 + int64`, and `float64 + float64 -> add<float64>`
- `uint64 + uint64 -> add<uint64>`
- mixed signed and unsigned integer inputs remain execution errors
- decimal inputs do not rescue or absorb integer, unsigned, or float inputs
- `float32` remains outside the admitted completion set for this first family

### Result Derivation

For every admitted overload group:

- result logical type is the selected overload's logical result type
- result nullability is `lhs.nullable OR rhs.nullable`
- null propagation stays row-wise

For the currently admitted or required groups:

- `add<int32>` derives `int32`
- `add<int64>` derives `int64`
- `add<uint64>` derives `uint64`
- `add<float64>` derives `float64`

Integer-family overflow remains an execution error:

- signed integer add does not wrap, saturate, or widen after selection
- unsigned integer add does not wrap, saturate, or coerce into signed or
  floating output

`add<float64>` does not reuse the integer overflow rule. It reuses the shared
`float64` special-value boundary from
`docs/design/first-float64-ordering-slice.md`, so `NaN`, `Infinity`,
`-Infinity`, and signed zero stay ordinary non-null `float64` row outcomes
when produced by the selected overload.

### Decimal Blocker Boundary

Decimal add is part of the required completion set, but it is not admitted by
default just because `decimal128` passthrough and predicate checkpoints exist.

Before `add<decimal128(precision, scale)>` is admitted, the same epic still
needs one accepted checkpoint that fixes all of these together:

- decimal precision and scale result derivation
- whether only same-scale decimal add is admitted first
- overflow-as-error versus any narrower decimal-specific rescale policy
- the exact boundary between `decimal128`-in-range add and deferred
  `decimal256` or wider decimal paths

Until that checkpoint exists, decimal add remains a required blocker for a full
family-completion claim rather than an implicit inherited case.

## Adapter, Harness, And Evidence Expectations

Family completion should reuse the repo's existing narrow-slice carrier model.
It should not introduce one giant family-global adapter protocol or one
monolithic inventory matrix.

Before the family can be claimed complete:

- every newly admitted overload group needs one docs-first conformance anchor
  under `tests/conformance/`
- every newly admitted overload group needs one differential slice doc under
  `tests/differential/`
- every newly admitted overload group needs one shared adapter-boundary doc
  under `adapters/`
- executable claims need checked-in `inventory/` evidence through the current
  `case-results`, compatibility-notes when relevant, and drift-report carriers

Existing anchors already count toward the family:

- `first-expression-slice` for `add<int32>`
- `first-unsigned-arithmetic-slice` for `add<uint64>`

The remaining admitted overload groups should follow the same pattern rather
than redefining the harness model:

- one narrow signed-widening or `int64` add slice
- one narrow `float64` add slice that reuses canonical float normalization
- one narrow decimal add slice after the decimal blocker boundary above is
  accepted

Normalized evidence for those follow-ons should keep using shared logical type
tokens and current carrier rules:

- signed integer rows use JSON numeric scalar form plus `null`
- `uint64` rows use canonical base-10 string tokens plus `null`
- `float64` rows reuse canonical float64 tokens from the existing float64
  checkpoint
- decimal rows use canonical decimal strings with preserved scale plus `null`

`adapter_unavailable` is acceptable as an interim executable outcome while a
required slice is still being built, but the family cannot be claimed complete
while a required overload group still depends on that interim outcome for the
admitted engine pair.

## Non-Goals

This completion boundary does not include:

- unary plus
- subtraction, multiplication, division, modulo, bitwise, comparison, or
  aggregate families
- every signed or unsigned width beyond the admitted `int32`, `int64`, and
  `uint64` checkpoint set
- `float32` arithmetic in this first family-completion pass
- `decimal256`, wider decimal carriers, or decimal/int/float mixed arithmetic
- mixed signed/unsigned success semantics
- temporal, interval, duration, JSON, string, or collation-sensitive `+`
  behavior
- a family-global runtime API, family-global artifact format, or plan-capture
  requirement

## Result

`tiforth` now has one explicit completion boundary for the first numeric
`add/plus` family.

That boundary says the family is complete only when shared docs, type-system
rules, adapter and harness anchors, and checked-in evidence cover one generic
`add` family across the admitted `int32`, `int64`, `uint64`, `float64`, and
decimal add checkpoints, with decimal result derivation called out as an
explicit same-epic blocker instead of a hidden assumption.
