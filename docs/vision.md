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

## TODOs

- Decide which function and operator families are inventoried first.
- Define the first thin end-to-end harness slice.
- Define acceptance criteria for introducing the first kernel component.
