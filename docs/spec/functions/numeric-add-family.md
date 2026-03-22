# First Complete Numeric Add Family

Status: issue #414 docs-first checkpoint

Verified: 2026-03-22

Related issues:

- #84 `design: define first inventoried function and operator families`
- #94 `inventory: add first-expression-slice legacy function catalog`
- #300 `design: define first unsigned arithmetic semantic slice boundary`
- #409 `epic: complete function-family program`
- #412 `sub-epic: reuse tipb/kvproto function enums for the active family`
- #413 `sub-epic: TiDB-to-Arrow mapping for the active function family`
- #414 `spec: pin the first complete function family as numeric add/plus`
- #418 `design: fix TiDB-to-Arrow mapping for the numeric add/plus family`
- #420 `design: map the numeric add/plus family to tipb/kvproto enums`
- #422 `spec: complete the numeric add/plus family boundary`
- #423 `spec: fix decimal add result derivation for the numeric add/plus family`

## Question

Which exact function family should `tiforth` complete first under the active
function-family program?

## Inputs Considered

- `docs/design/first-inventory-wave.md`
- `docs/spec/functions/README.md`
- `docs/spec/type-system.md`
- `docs/spec/milestone-1-expression-projection.md`
- `docs/design/first-unsigned-arithmetic-slice.md`
- `inventory/first-expression-slice-legacy-function-catalog.md`
- `inventory/first-expression-slice-tidb-compat-notes.md`
- `inventory/first-expression-slice-tiflash-compat-notes.md`
- `inventory/first-expression-slice-tikv-compat-notes.md`
- `inventory/first-unsigned-arithmetic-slice-tidb-compat-notes.md`
- `inventory/first-unsigned-arithmetic-slice-tiflash-compat-notes.md`
- `inventory/first-unsigned-arithmetic-slice-tikv-compat-notes.md`

## Design Summary

The first complete function family is the scalar numeric add family.

For shared docs:

- the shared semantic family name is `add`
- `plus` is an accepted donor or protocol spelling for the same family
- SQL `+` is an accepted engine-local spelling for the same family
- shared docs may therefore refer to this as the numeric `add/plus` family when
  external naming matters

This first family-completion program is about one coherent family only:
same-name scalar numeric addition across exact-type overloads, generic-first
reuse, and explicit type-system rules.

## Why This Family First

- the repository already has donor and engine inventory anchored to
  `add<int32>(lhs, rhs)` through `first-expression-slice`
- the repository already has a second executable arithmetic checkpoint for the
  same family through `add<uint64>(lhs, rhs)` in
  `docs/design/first-unsigned-arithmetic-slice.md`
- `add` already forces the repo to answer the hard follow-on questions that a
  complete family needs: overload identity, result typing, signed versus
  unsigned interaction, type mapping, and protocol-ID reuse
- `literal<T>` is a useful operand-support family, but it is not the best first
  complete same-name overload program
- `is_not_null` already has broad predicate reuse, but it does not drive the
  same generic-first overload, type-mapping, and protocol-identity questions as
  numeric addition

## Current Evidence Boundary

Current admitted family members already present in repo evidence are:

- `add<int32>(lhs, rhs)` in the milestone-1 expression slice
- `add<uint64>(lhs, rhs)` in the first unsigned arithmetic slice

Those two checkpoints do not complete the family. They justify the family
choice.

The operand-support families used by those checkpoints:

- `column(index)`
- `literal<int32>(value)`
- `literal<uint64>(value)`

remain prerequisites and companions to the add family, not replacements for it
as the first complete function-family target.

## Family Identity Boundary

This first family-completion program includes:

- binary scalar addition over numeric operands
- one shared family identity across exact-type overloads
- explicit result-type and nullability derivation rules per overload group
- generic-first implementation reuse across overloads with type-specific policy
  hooks only where semantics require them

This first family-completion program does not include:

- unary plus
- subtraction, multiplication, division, modulo, bitwise, or comparison
  families
- aggregate `sum` or window-function semantics
- temporal, interval, or duration addition
- string concatenation through `+`
- mixed-type rescue through undocumented implicit casts

## Planned Completion Scope

The family-completion program should stay inside scalar numeric addition.

In scope for follow-on completion work:

- exact signed integer add overloads
- exact unsigned integer add overloads
- exact floating-point add overloads
- exact decimal add overloads inside the admitted `decimal128` boundary fixed
  in `docs/spec/functions/numeric-add-family-decimal-result-derivation.md`

Still deferred even after the family is fixed:

- mixed signed and unsigned success semantics beyond already-documented exact
  matches
- temporal or interval add spellings that engines may also lower with `+`
- non-numeric overloads or operator aliases that do not share numeric-add
  semantics cleanly

## Mapping Boundary

Issue #418 now fixes the family-specific TiDB-to-Arrow mapping boundary in
`docs/design/first-tidb-arrow-type-mapping-boundary.md`.

That mapping keeps the current family Arrow-native by default:

- exact signed and unsigned integer overloads preserve their exact Arrow integer
  result types even when engine-local arithmetic surfaces wider metadata
- exact floating-point overloads stay Arrow-native by width
- decimal add remains Arrow-native through Arrow decimal types with explicit
  precision/scale result derivation; derived precision `> 38` stays deferred to
  a `decimal256` follow-on rather than a custom numeric carrier now

## Enum-Reuse Boundary

Issue #420 now fixes the family-specific enum-reuse boundary in
`docs/design/first-add-family-tipb-kvproto-enum-reuse.md`.

That boundary reuses the existing upstream arithmetic IDs directly:

- `tipb::ScalarFuncSig::PlusReal` for floating-point add
- `tipb::ScalarFuncSig::PlusDecimal` for decimal add
- `tipb::ScalarFuncSig::PlusInt` plus the signedness-specific `PlusInt*`
  variants for exact integer add overloads
- no separate numeric-add function enum from `kvproto`; `kvproto` remains the
  transport boundary for `tipb` payloads

## Decimal Result-Derivation Boundary

Issue #423 now fixes the family-specific decimal add result-derivation boundary
in `docs/spec/functions/numeric-add-family-decimal-result-derivation.md`.

That boundary now makes all of these explicit for exact decimal add:

- `result.scale = max(s1, s2)`
- `result.precision = max(p1 - s1, p2 - s2) + max(s1, s2) + 1`
- mixed-scale decimal add is admitted only through exact zero-extension of the
  smaller-scale operand
- derived precision `> 38` is deferred to a same-family `decimal256`
  follow-on instead of being narrowed back into `decimal128`

## Completion Boundary

Issue #422 now fixes the family-completion boundary in
`docs/spec/functions/numeric-add-family-completion.md`.

That doc is the canonical shared entry for:

- generic-first overload reuse across the admitted numeric add overload groups
- add-family type-system and result-derivation consequences
- required adapter, harness, and checked-in evidence expectations before the
  family can be claimed complete

## Result

`tiforth` now has one exact first complete function-family target: the scalar
numeric `add/plus` family. Future family work should stay inside that family
until its mapping, protocol identity, and completion boundaries are resolved
through the linked shared docs.
