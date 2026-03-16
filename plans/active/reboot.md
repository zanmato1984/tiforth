# Reboot Plan

## Scope

Stand up a harness-first reboot skeleton for `tiforth` without importing legacy implementation code.

## Phase 0: Repository Skeleton

- create docs-first layout
- define initial contracts and architecture boundaries
- define plan, inventory, test, and adapter directories

Exit criteria:

- repository structure is visible in git
- root navigation is clear
- no concrete operator or function code exists

## Phase 1: Donor Extraction And Inventory

- inventory relevant concepts from the legacy repository
- separate reusable ideas from rejected implementations
- extract candidate catalogs for functions, operators, and type behaviors
- record semantic drift and unknowns

Exit criteria:

- inventory artifacts exist under `inventory/`
- donor-derived concepts are documented without pulling donor code forward
- first-pass drift report identifies major engine mismatches

## Phase 2: Harness Foundations

- define fixture formats and result formats
- choose the first conformance slice
- choose the first differential slice across engines
- define the minimum runtime observability needed for tests

Exit criteria:

- harness contracts are documented
- tests directories have concrete harness plans
- first executable harness milestone is scoped, but not overdesigned

## Phase 3: Spec Expansion

- draft type-system details needed by the first harness slices
- add function and operator spec stubs based on inventory priorities
- resolve only the semantics needed to test meaningful cases

Exit criteria:

- prioritized spec surface exists
- unresolved semantics are called out explicitly
- harness cases can point to spec text instead of assumptions

## Phase 4: Kernel Introduction

- introduce the smallest shared kernel boundary that the harnesses require
- keep adapters thin and semantics spec-driven
- measure behavior against harness expectations from day one

Exit criteria:

- first kernel work is justified by harness needs
- no implementation lands without spec and test coverage targets
- adapter responsibilities remain explicit

## Current Risks

- overcommitting to runtime mechanics too early
- under-specifying the type system and creating hidden drift later
- letting adapters absorb semantics that belong in shared specs

## TODOs

- decide the first donor extraction targets
- decide the first engine pair or trio for differential testing
- decide the minimum artifact schema for inventory outputs
- complete the shared-kernel language decision task tracked in `plans/active/language-decision.md`
