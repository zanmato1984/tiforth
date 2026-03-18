# Vision

## Goals

- Unify relational operators and expression or function behavior across TiDB, TiFlash, and TiKV.
- Establish clear data and runtime contracts before implementation work.
- Build harnesses that can validate semantics, surface drift, and measure performance across engines.
- Reuse donor ideas that still help: Arrow for data interchange and broken-pipeline for runtime direction.

## Non-Goals

- Porting the legacy repository as-is.
- Carrying forward operator or function implementations that are already considered poor fits.
- Locking every unresolved semantic choice before inventory and harness work starts.
- Choosing a permanent language toolchain in this skeleton commit.

## Why Harness-First

Harness-first forces the repository to answer a practical question early: how will correctness and drift be measured? That matters more than shipping placeholder kernels.

It also keeps the reboot honest:

- specs can be tested against real engines
- disagreements can be recorded before they become code paths
- later implementation work can target a stable contract instead of a moving guess

## Immediate Priorities

- inventory donor and engine behavior without inheriting donor code
- define minimal data and runtime contracts
- stand up conformance, differential, and performance harness boundaries

## Current Checkpoints

- the first thin end-to-end slice is the local source -> projection -> sink path documented in `docs/spec/milestone-1-expression-projection.md`
- the first accepted kernel boundary is the milestone-1 expression-projection slice backed by the data and runtime contracts plus local conformance coverage
- the first documented differential checkpoint is the TiDB-versus-TiFlash expression slice in `tests/differential/first-expression-slice.md`
- the first executable differential checkpoint now exists through `crates/tiforth-harness-differential` and checked-in paired artifacts under `inventory/`
- the first post-gate shared-kernel expansion candidate is now fixed in `docs/design/first-post-gate-kernel-boundary.md`

## Next Checkpoint

- the next checkpoint is a docs-first filter-slice prep issue that defines the first filter semantics, harness cases, and adapter boundary updates before kernel code
- that follow-on checkpoint keeps layer-3 growth thin by limiting the first filter boundary to one predicate family: `is_not_null(column(index))`

## Kernel Expansion Gate

- widening the shared kernel beyond the current milestone-1 slice now requires the acceptance gate in `docs/design/kernel-expansion-acceptance.md`
- that gate requires executable differential evidence from the next thin slice plus docs-first scope, named harness coverage, and concrete completion evidence for any later kernel expansion

## First Inventory Wave

- the first inventory wave is documented in `docs/design/first-inventory-wave.md`
- that wave stays aligned with the milestone-1 projection semantic core: the projection operator plus `column`, `literal<int32>`, and `add<int32>` expression families
- donor catalogs, engine compatibility notes, and any first coverage-gap inventory should anchor to the stable refs already defined for `tests/differential/first-expression-slice.md`
