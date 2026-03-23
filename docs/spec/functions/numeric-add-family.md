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
- exact decimal add overloads once decimal typing, overflow, and rescale policy
  are named explicitly

Still deferred even after the family is fixed:

- mixed signed and unsigned success semantics beyond already-documented exact
  matches
- temporal or interval add spellings that engines may also lower with `+`
- non-numeric overloads or operator aliases that do not share numeric-add
  semantics cleanly

## Required Follow-On Boundaries

Before the family can be completed coherently, the active epic must settle:

1. `#413` for the TiDB-to-Arrow mapping required by admitted numeric add
   overloads
2. `#412` for `tipb` and `kvproto` enum reuse or explicit `tiforth`-only ID
   fallback for the same family

Only after those boundaries land should a later issue claim full family
completion through generic-first overload reuse.

## Result

`tiforth` now has one exact first complete function-family target: the scalar
numeric `add/plus` family. Future family work should stay inside that family
until its mapping, protocol identity, and completion boundaries are resolved.
