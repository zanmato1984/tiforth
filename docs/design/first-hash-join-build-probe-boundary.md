# First Hash Join Build/Probe Boundary

Status: issue #438 design checkpoint

Verified: 2026-03-22

Related issues:

- #410 `epic: hash join build/probe program`
- #415 `program: focus-driven execution control`
- #438 `design: define first hash join build/probe boundary`

## Question

What first durable docs-first boundary should `tiforth` set for a complete hash
join program that splits execution into build and probe stages, adapts to the
adopted `broken-pipeline-rs` runtime, and uses TiFlash as donor evidence rather
than as source-of-truth code?

## Inputs Considered

- `docs/architecture.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/spec/operators/README.md`
- `docs/spec/type-system.md`
- `docs/design/kernel-expansion-acceptance.md`
- `docs/design/operator-expression-runtime-attachment.md`
- `docs/design/exchange-runtime-mapping.md`
- `docs/design/first-in-contract-exchange-slice.md`
- `docs/design/first-collation-string-slice.md`
- issue #410
- issue #438

## Design Summary

Treat hash join as one long-horizon operator program, not as one issue-sized
implementation drop.

The durable boundary for that program is:

- semantic operator family: one shared `hash_join` family with an explicit
  join-kind matrix
- runtime shape: separate build-side and probe-side operator boundaries that
  attach to adopted upstream runtime traits without inventing new shared
  runtime states
- implementation bias: generic key-path and row-state reuse first, specialized
  fast paths second, instead of one disconnected implementation per join kind
- evidence stance: TiFlash join behavior and implementation structure are donor
  evidence for inventory and design review, not code to copy by default

## Why A Separate Design Checkpoint Is Required

Current operator docs intentionally stop before multi-input families, and no
existing accepted design doc names:

- the build/probe split
- the join-kind matrix
- the required hash-table boundary
- collation-sensitive join-key handling
- the adopted-runtime mapping for blocked build completion, probe progress, and
  teardown

Hash join therefore needs its own design checkpoint before any executable
shared-kernel work claims it.

## Planned Semantic Scope

Before implementation starts, follow-on docs should make these boundaries
explicit:

- join kinds to classify in the first complete family:
  `inner`, `left_outer`, `right_outer`, `full_outer`, `left_semi`,
  `left_anti`, `right_semi`, and `right_anti`
- any `mark`, `existence`, or null-aware variants must be either specified
  explicitly or deferred explicitly; they must not remain implicit
- key semantics must cover duplicate keys, null join-key handling, output
  column ordering, unmatched-row materialization, and build-side versus
  probe-side ownership of reused columns
- residual non-equi predicates should remain a separate named boundary unless a
  follow-on spec proves they are required for the same first complete family

## Runtime And Ownership Scope

The first hash-join program should define build/probe attachment on the adopted
runtime contract without renaming upstream states:

- build-side state may own hash-table memory, row-reference storage, and any
  normalized key buffers, all under reserve-before-allocate admission
- probe-side state may read build-side state through explicit ownership and
  teardown rules rather than hidden global lifetime assumptions
- build completion, probe blocking, downstream backpressure, cancellation, and
  drain behavior must map onto adopted `TaskStatus` and `OpOutput` meanings
  only
- spill or repartition strategy, if needed, must extend the existing
  reserve-first contract rather than introducing an untracked memory path

## Collation And Type-System Scope

Hash join cannot stay independent of type and collation rules.

Before code lands, follow-on docs should name:

- which logical type families are admitted as hash-join keys in the first
  checkpoint
- which families require exact native equality and hash behavior
- how collation-tagged `utf8` keys normalize and compare when the join family
  admits them
- whether mixed-type keys are rejected, coerced, or normalized through a
  separate pre-join rule

The initial expectation should be to reuse already-documented collation
identifiers and type-system vocabulary wherever possible rather than inventing
join-only variants.

## Required Follow-On Docs Before Implementation

Any executable hash-join issue should update or create, or explicitly mark
unchanged, all of these surfaces first:

1. one semantic operator spec under `docs/spec/operators/`
2. `docs/spec/type-system.md` for key compatibility, null semantics, and any
   coercion or comparison rules
3. `docs/contracts/data.md` for build-state ownership units, forwarded-column
   claim behavior, and any new batch-envelope expectations
4. `docs/contracts/runtime.md` for build/probe blocked, resumed, cancelled, and
   teardown behavior where shared runtime meaning changes
5. conformance checkpoints under `tests/conformance/`
6. differential checkpoints and artifact carriers under
   `tests/differential/`
7. adapter boundary docs under `adapters/`
8. donor and engine inventory artifacts that review TiFlash structure,
   supported join kinds, hash-table choices, and collation behavior without
   promoting donor code to source of truth

## Program Decomposition

Because `docs/design/kernel-expansion-acceptance.md` requires thin,
reviewable kernel growth, this long-horizon task should be split into multiple
issue-sized checkpoints even if the end goal is one complete hash-join family.

Recommended decomposition:

1. inventory and semantic design for the join-kind matrix and output-shape
   rules
2. build-side ownership, admission, and hash-table boundary
3. probe-side match emission for the first equi-join subset
4. outer, semi, and anti follow-ons that reuse the same build/probe framework
5. collation-sensitive key normalization and comparison follow-on
6. adapter and harness execution plus checked-in evidence

## Out Of Scope For This Checkpoint

- merge join, nested-loop join, or general multi-way join planning
- distributed repartition exchange or network transport policy
- adaptive runtime re-optimization
- freezing one permanent public operator-construction API
- assuming TiFlash join behavior is automatically the shared contract without
  `tiforth` docs and harness evidence

## Result

`tiforth` now has one explicit planning boundary for a future complete
hash-join program: docs-first, split into build and probe operators, tied to
the adopted runtime and current admission rules, and decomposed into smaller
issue-sized checkpoints before implementation begins.
