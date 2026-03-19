# Architecture

The current proposal is intentionally layered so specs and harnesses can mature before kernels grow beyond the current milestone-1 slices.

## Proposed Layers

### 1. Spec

Defines names, signatures, semantics, edge cases, and compatibility notes for operators, functions, and types.

### 2. Data Contract

Defines the in-memory representation and batch boundaries. Directionally Arrow-native.

### 3. Kernel

Execution primitives that implement spec-defined behavior over the data contract. This layer is currently limited to narrow milestone-1 slices backed by accepted docs and local tests.

### 4. Runtime

Defines how kernels are scheduled, chained, canceled, instrumented, and backpressured. For the current milestone-1 slices, `tiforth` directly adopts the Arrow-bound runtime contract from `broken-pipeline-rs`; `tiforth`-owned operators and expressions attach on top of that contract rather than replacing it.

### 5. Adapters

Engine-facing translation layers for TiDB, TiFlash, and TiKV. Adapters should map engine concepts onto shared specs and contracts rather than owning semantics.

### 6. Harnesses

Conformance, differential, and performance harnesses that exercise the stack and report drift.

The first documented differential checkpoint is the TiDB-versus-TiFlash expression slice in `tests/differential/first-expression-slice.md`.

## Current Repository Bias

This reboot started in layers 1, 2, 4, 5, and 6. Layer 3 now enters only through minimal milestone-1 slices that are justified by docs and local tests.

The next end-to-end checkpoint should still grow layers 5 and 6 before it widens layer 3 further: `docs/design/next-thin-end-to-end-slice.md` fixes the follow-on slice as the first executable differential harness over the already-documented expression family.

`docs/design/adapter-milestone-breakdown.md` fixes how that differential slice should break into issue-scoped adapter and harness checkpoints.

`docs/design/kernel-expansion-acceptance.md` now defines the gate for any later layer-3 growth after that differential checkpoint exists.

`docs/design/first-post-gate-kernel-boundary.md` fixed the first post-gate layer-3 expansion as one narrow filter boundary, and issue #149 implements its first executable local kernel path.

`docs/design/first-in-contract-exchange-slice.md` now fixes the first post-milestone in-contract runtime exchange checkpoint as one narrow single-producer and single-consumer local queue boundary with required conformance and differential coverage, without widening current shared-kernel semantics.

## Current Minimal Kernel Boundaries

The currently useful shared-kernel boundaries under `crates/tiforth-kernel` are:

- the milestone-1 expression-projection slice
- the first post-gate filter slice for `is_not_null(column(index))`
- the first temporal `date32` executable conformance extension for `column(index)` passthrough and `is_not_null(column(index))`
- the first decimal `decimal128` executable conformance extension for `column(index)` passthrough and `is_not_null(column(index))`
- the first float64 NaN and infinity ordering executable conformance extension for `column(index)` passthrough and `is_not_null(column(index))`

Those boundaries are intentionally narrow:

- one static Arrow batch source, one projection or filter pipe, and one collecting sink for the local executable slices
- expression evaluation only for `column(index)`, `literal<int32>(value)`, and `add<int32>(lhs, rhs)`
- filter predicate evaluation for `is_not_null(column(index))` with `int32` predicate input in the first filter slice, `date32` predicate input in the first temporal slice, `decimal128` predicate input in the first decimal slice, and `float64` predicate input in the first float64 slice
- direct attachment to the adopted upstream `SourceOperator`, `PipeOperator`, and `SinkOperator` traits, with expressions and filter predicates kept as operator-local evaluators and schema helpers
- reserve-first admission around operator-owned output materialization
- governed-batch handoff and live-claim tracking through the source -> projection or filter -> sink paths
- local execution snapshots and checked-in fixtures that keep rows, errors, and ownership outcomes reviewable

These boundaries are justified by `docs/spec/milestone-1-expression-projection.md`, `docs/spec/first-filter-is-not-null.md`, `docs/spec/type-system.md`, `docs/contracts/data.md`, `docs/contracts/runtime.md`, `tests/conformance/expression-projection-slice.md`, `tests/conformance/first-filter-is-not-null-slice.md`, `docs/design/first-temporal-semantic-slice.md`, `tests/conformance/first-temporal-date32-slice.md`, `docs/design/first-decimal-semantic-slice.md`, `tests/conformance/first-decimal128-slice.md`, `docs/design/first-float64-ordering-slice.md`, and `tests/conformance/first-float64-ordering-slice.md`.

They are not yet a general shared kernel API. They exist to prove end-to-end paths where shared specs, admission rules, handoff ownership, and local harness evidence all line up before broader operators or adapter-driven execution are introduced.

## Architectural Rules

- Specs own semantics.
- Contracts own boundaries.
- Adapters should be thin.
- Harnesses should be able to test specs independently of future kernels.

## Kernel Expansion Gate

After the current milestone-1 slices, layer 3 should grow only when:

- the next thin end-to-end differential slice already exists as reviewable evidence
- the proposed kernel growth is one narrow boundary with docs and harness coverage already named
- the resulting issue can show which shared surfaces change and which stay out of scope

## TODOs

- Extend harness result and drift-report formats beyond the current first
  differential slices (`first-expression-slice` and
  `first-filter-is-not-null-slice`).
