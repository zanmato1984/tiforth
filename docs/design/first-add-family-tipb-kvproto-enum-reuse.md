# First Numeric Add Family `tipb`/`kvproto` Enum Reuse Boundary

Status: issue #420 design checkpoint

Verified: 2026-03-22

Related issues:

- #409 `epic: complete function-family program`
- #412 `sub-epic: reuse tipb/kvproto function enums for the active family`
- #414 `spec: pin the first complete function family as numeric add/plus`
- #418 `design: fix TiDB-to-Arrow mapping for the numeric add/plus family`
- #420 `design: map the numeric add/plus family to tipb/kvproto enums`

## Question

How should the scalar numeric `add/plus` family reuse existing `tipb` and
`kvproto` enum surfaces before `tiforth` invents any new family IDs?

## Inputs Considered

- `docs/spec/functions/numeric-add-family.md`
- `docs/spec/functions/README.md`
- `docs/spec/type-system.md`
- `https://github.com/pingcap/tipb/blob/master/proto/expression.proto`
- `https://github.com/pingcap/kvproto/blob/master/proto/mpp.proto`
- `https://github.com/pingcap/kvproto/blob/master/proto/disaggregated.proto`

## Design Summary

The numeric `add/plus` family should reuse existing `tipb` arithmetic enum
entries directly.

For the current family:

- `tipb` is the source of reusable function identifiers
- `kvproto` does not define a parallel numeric add function enum in the reviewed
  sources; it transports `tipb` payloads where relevant
- `tiforth` therefore reuses `tipb` enum identity first and treats `kvproto` as
  the transport boundary for those reused `tipb` payloads rather than as a
  second function-ID namespace

No `tiforth`-only function ID is required for the currently admitted numeric
`add/plus` family.

## Reviewed Upstream Enum Surface

In `pingcap/tipb` `proto/expression.proto`, the reviewed arithmetic scalar
function signatures include:

- `PlusReal = 200`
- `PlusDecimal = 201`
- `PlusInt = 203`
- `PlusIntUnsignedUnsigned = 219`
- `PlusIntUnsignedSigned = 220`
- `PlusIntSignedUnsigned = 221`
- `PlusIntSignedSigned = 222`

The same reviewed file also carries the enclosing expression-type enum for
scalar functions.

Inference from reviewed `pingcap/kvproto` sources:

- the repository search did not surface `ScalarFuncSig`, `PlusInt`, or another
  duplicate numeric-add function enum in `kvproto`
- reviewed `kvproto` files such as `proto/mpp.proto` and
  `proto/disaggregated.proto` reference `tipb` payloads instead
- for the current family, `kvproto` contributes transport compatibility, not a
  second add-family enum mapping table

## Reuse Policy For The Current Family

For `tiforth` shared docs:

- the family name remains `add`, with `plus` and SQL `+` treated as accepted
  external spellings
- when `tiforth` needs a protocol-facing function identifier for the admitted
  numeric family, it should reuse the matching `tipb` arithmetic signature
  instead of inventing a new one

Current mapping policy:

- exact floating-point add overloads reuse `tipb::ScalarFuncSig::PlusReal`
- exact decimal add overloads reuse `tipb::ScalarFuncSig::PlusDecimal`
- exact integer add overloads reuse the `tipb` integer-plus family
- when operand signedness is already explicit at the boundary, prefer the more
  specific `PlusIntUnsignedUnsigned`, `PlusIntUnsignedSigned`,
  `PlusIntSignedUnsigned`, or `PlusIntSignedSigned`
- when only the coarse integer-add family identity is needed, `PlusInt` remains
  the acceptable shared fallback

This keeps enum reuse aligned with the existing upstream arithmetic surface
without collapsing `tiforth` overload identity into one coarse protocol token.
Exact overload identity still lives in `tiforth` specs, types, and schema
metadata.

## Deferred Or Rejected Cases

This checkpoint does not claim enum reuse for:

- unary plus
- subtraction or other arithmetic families
- temporal or interval addition spelled with `+`
- non-numeric `+` semantics such as string concatenation

If a later numeric add overload needs a protocol-facing ID that the existing
reviewed `tipb` surface does not provide, that mismatch should be handled in a
follow-on issue explicitly instead of silently inventing a new `tiforth` ID.

## Result

`tiforth` now has a first explicit enum-reuse boundary for the numeric
`add/plus` family: reuse existing `tipb` arithmetic signatures directly, treat
`kvproto` as the transport layer for those `tipb` payloads rather than as a
second function-enum namespace, and keep `tiforth` overload identity in shared
spec and type-system docs.
