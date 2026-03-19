# Milestone-1 Nested And Decimal/Temporal Metadata Boundary

Status: issue #127 design checkpoint

Verified: 2026-03-19

Related issues:

- #74 `spec: define milestone-1 int32 type-system boundary`
- #90 `design: define dictionary-encoding boundary in shared data contract`
- #127 `design: define milestone-1 nested and decimal/temporal metadata boundary`
- #174 `design: define first temporal semantic slice boundary`
- #189 `design: define first decimal semantic slice boundary`

## Question

What nested-type and decimal/temporal metadata support is required for
milestone 1 across the shared data and type-system contracts?

## Inputs Considered

- `docs/contracts/data.md`
- `docs/spec/type-system.md`
- `docs/spec/milestone-1-expression-projection.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/differential/first-expression-slice.md`
- `adapters/first-expression-slice.md`
- `docs/design/first-temporal-semantic-slice.md`
- `docs/design/first-decimal-semantic-slice.md`
- issue #127
- issue #174
- issue #189

## Design Summary

Milestone 1 keeps the shared boundary narrow:

- nested logical families are out of scope for the current shared handoff and
  executable slice
- decimal and temporal logical families are also out of scope for milestone-1
  execution and differential evidence
- milestone 1 does not require shared interpretation of decimal precision or
  scale metadata, or temporal unit or timezone metadata
- adapters and local execution paths should fail unsupported family requests
  explicitly instead of guessing coercions

This keeps milestone-1 contracts aligned with the currently executable `int32`
projection slice while preserving room for later typed expansions.

## Milestone-1 Boundary

### Nested Families

- milestone-1 shared stage handoff does not require direct support for nested
  arrays (`list`, `large_list`, `fixed_size_list`, `struct`, `map`, `union`,
  or nested combinations)
- first-slice docs, fixtures, and differential artifacts should not add
  nested-family cases in milestone 1
- if an adapter or source encounters nested-backed data for a milestone-1 case,
  it should normalize to the current supported logical slice before handoff
  when possible; otherwise it should surface an unsupported outcome
  (`adapter_unavailable` in the differential adapter contract)

### Decimal And Temporal Families

- milestone-1 shared expression and projection semantics remain limited to the
  currently documented `int32` slice
- decimal and temporal logical families are not required inputs or outputs for
  milestone-1 executable or differential checkpoints
- milestone-1 shared contracts do not require interpreting or comparing decimal
  precision and scale metadata, or temporal unit and timezone metadata
- local execution paths should report unsupported-type execution errors rather
  than applying implicit casts into the milestone-1 `int32` arithmetic path

## Why This Boundary

- it removes ambiguity from milestone-1 contract reading by making unsupported
  families explicit rather than implied by TODOs
- it prevents accidental scope creep in adapter and harness work while the
  first executable differential slice is still intentionally narrow
- it keeps future family expansion explicit: adding nested, decimal, or
  temporal support requires a follow-on issue with semantics plus coverage

## Follow-On Boundary

Post-milestone follow-on checkpoints are now explicit:

- `docs/design/first-temporal-semantic-slice.md` fixes the first temporal
  semantic checkpoint as narrow `date32` passthrough plus
  `is_not_null(column(index))`
- `docs/design/first-decimal-semantic-slice.md` fixes the first decimal
  semantic checkpoint as narrow `decimal128` passthrough plus
  `is_not_null(column(index))`

Later issues may extend this checkpoint to define:

- the first shared slice that allows broader nested-family arrays across stage
  boundaries, including ownership and claim behavior for nested buffers
- temporal semantics beyond the first `date32` checkpoint, including
  timezone-sensitive normalization and ordering rules
- decimal semantics beyond the first `decimal128` checkpoint, including
  arithmetic, cast/coercion, and rounding or rescale policy

## Result

Milestone 1 explicitly treats nested, decimal, and temporal families as out of
scope for shared execution and differential evidence, with explicit unsupported
outcomes instead of implicit coercion. First post-milestone temporal and
decimal checkpoints are now fixed in dedicated follow-on design docs.
