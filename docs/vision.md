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
- the first machine-readable drift-report sidecars now exist for both milestone-1 differential slices under `inventory/` alongside their required Markdown drift reports
- the first checked-in per-engine compatibility notes now cover both first-expression and first-filter slices under `inventory/` for TiDB and TiFlash
- the first post-gate shared-kernel expansion candidate is now fixed in `docs/design/first-post-gate-kernel-boundary.md`
- the docs-first filter prep checkpoint now covers semantics, conformance, differential slice shape, and adapter boundary through `docs/spec/first-filter-is-not-null.md`, `tests/conformance/first-filter-is-not-null-slice.md`, `tests/differential/first-filter-is-not-null-slice.md`, and `adapters/first-filter-is-not-null-slice.md`
- the first temporal date32 checkpoint now has docs and local executable conformance coverage through `docs/design/first-temporal-semantic-slice.md`, `tests/conformance/first-temporal-date32-slice.md`, and `crates/tiforth-kernel/tests/temporal_date32_slice.rs`, with differential and adapter anchors in `tests/differential/first-temporal-date32-slice.md` and `adapters/first-temporal-date32-slice.md`
- the first decimal `decimal128` checkpoint is now fixed as a docs-first semantic and adapter boundary through `docs/design/first-decimal-semantic-slice.md`, `tests/conformance/first-decimal128-slice.md`, `tests/differential/first-decimal128-slice.md`, and `adapters/first-decimal128-slice.md`
- the first executable differential temporal date32 checkpoint now exists through `crates/tiforth-harness-differential` and checked-in paired first-temporal artifacts under `inventory/`
- the first executable post-gate filter checkpoint now exists through `crates/tiforth-kernel` via `FilterPipe` and executable coverage in `crates/tiforth-kernel/tests/filter_is_not_null.rs`
- the first executable differential filter checkpoint now exists through `crates/tiforth-harness-differential` and checked-in paired first-filter artifacts under `inventory/`
- the first executable differential exchange-parity checkpoint now exists through `crates/tiforth-harness-differential/src/first_exchange_slice.rs` over the existing `first-expression-slice` and `first-filter-is-not-null-slice` case IDs
- the first live-runner orchestration path for `first-filter-is-not-null-slice` now exists through `crates/tiforth-harness-differential/src/first_filter_is_not_null_live.rs` and `crates/tiforth-harness-differential/src/bin/first_filter_is_not_null_live.rs`

## Next Checkpoint

- the next checkpoint is to refresh first-filter differential evidence from the live-runner path when TiDB and TiFlash environments are available, while preserving the same shared case IDs and normalized carriers
- that follow-on checkpoint should keep the current first-filter case IDs, `input_ref`s, and `filter_ref`s stable before broadening predicate families or type coverage

## Kernel Expansion Gate

- widening the shared kernel beyond the current milestone-1 slice now requires the acceptance gate in `docs/design/kernel-expansion-acceptance.md`
- that gate requires executable differential evidence from the next thin slice plus docs-first scope, named harness coverage, and concrete completion evidence for any later kernel expansion

## First Inventory Wave

- the first inventory wave is documented in `docs/design/first-inventory-wave.md`
- that wave stays aligned with the milestone-1 projection semantic core: the projection operator plus `column`, `literal<int32>`, and `add<int32>` expression families
- donor catalogs, engine compatibility notes, and any first coverage-gap inventory should anchor to the stable refs already defined for `tests/differential/first-expression-slice.md`
