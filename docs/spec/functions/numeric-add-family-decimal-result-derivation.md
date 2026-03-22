# Numeric `add/plus` Decimal Result-Derivation Boundary

Status: issue #423 docs-first checkpoint

Verified: 2026-03-22

Related issues:

- #189 `design: define first decimal semantic slice boundary`
- #409 `epic: complete function-family program`
- #414 `spec: pin the first complete function family as numeric add/plus`
- #418 `design: fix TiDB-to-Arrow mapping for the numeric add/plus family`
- #420 `design: map the numeric add/plus family to tipb/kvproto enums`
- #422 `spec: complete the numeric add/plus family boundary`
- #423 `spec: fix decimal add result derivation for the numeric add/plus family`

## Question

What exact shared result-derivation rule admits decimal add into the first
numeric `add/plus` family without widening the family prematurely beyond
`decimal128`?

## Inputs Considered

- `docs/spec/functions/numeric-add-family.md`
- `docs/spec/functions/numeric-add-family-completion.md`
- `docs/spec/type-system.md`
- `docs/design/first-decimal-semantic-slice.md`
- `docs/design/first-tidb-arrow-type-mapping-boundary.md`
- `docs/contracts/data.md`
- issue #423

## Design Summary

The first admitted exact decimal add boundary for the numeric `add/plus` family
uses the TiDB/MySQL-compatible `PlusDecimal` result-derivation rule.

For `lhs: decimal128(p1, s1)` and `rhs: decimal128(p2, s2)`:

- `result.scale = max(s1, s2)`
- `result.precision = max(p1 - s1, p2 - s2) + max(s1, s2) + 1`

This boundary is admitted only when the derived result precision stays within
Arrow `decimal128` range (`<= 38`). Shared docs do not narrow, round, or
rescale a wider derived result back into `decimal128`.

## Admitted Scope

The first admitted decimal add scope is intentionally narrow:

- both operands must already resolve to exact `decimal128(p, s)` logical types
- operand metadata must satisfy `1 <= p <= 38` and `0 <= s <= p`
- decimal add does not absorb or rescue `int32`, `int64`, `uint64`, `float64`,
  or any other non-decimal input
- result nullability is `lhs.nullable OR rhs.nullable`
- the selected overload result is `decimal128(result.precision, result.scale)`
  only when the derived precision is `<= 38`

This admits same-scale decimal add and mixed-scale decimal add that can be
aligned through exact zero-extension only.

## Mixed-Scale Exact Zero-Extension Boundary

Mixed-scale decimal add is admitted when operand alignment is exact:

- the operand with the smaller scale is aligned to `result.scale` by appending
  fractional trailing zeroes only
- that alignment is exact value preservation, not rounding, trimming, or a
  value-dependent cast
- the result-derivation formula above still applies after that logical
  alignment; there is no separate same-scale-only rule

For this checkpoint, mixed-scale admission is therefore narrower than a general
decimal rescale policy. It permits only exact zero-extension needed to align
the operands for add.

## Deferred `decimal256` Boundary

When the derived result precision is greater than `38`:

- the add request is outside the current admitted `decimal128` boundary
- shared docs do not silently reduce scale, clip digits, round, saturate, or
  force the result back into `decimal128`
- those cases are deferred to a same-family `decimal256` follow-on issue plus
  the required data-contract and adapter checkpoints

This keeps the current family aligned with Arrow-native `decimal128` while
leaving wider exact decimal add as explicit follow-on work.

## Non-Goals

This checkpoint does not admit:

- `decimal256` execution or shared `decimal256` carrier policy
- decimal subtraction, multiplication, division, modulo, or aggregate math
- decimal and integer, unsigned, floating-point, JSON, string, or temporal
  mixed arithmetic
- decimal cast or coercion rules outside exact zero-extension for add alignment
- a value-dependent narrowing rule for forcing wide results back into
  `decimal128`

## Result

`tiforth` now has one accepted decimal add result-derivation boundary for the
first numeric `add/plus` family: exact `decimal128` operands use the
TiDB/MySQL-compatible precision and scale formula above, mixed-scale alignment
is limited to exact zero-extension, and any derived result requiring precision
greater than `38` stays deferred to a `decimal256` follow-on instead of being
silently narrowed.
