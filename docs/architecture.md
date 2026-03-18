# Architecture

The current proposal is intentionally layered so specs and harnesses can mature before kernels grow beyond the current milestone-1 slice.

## Proposed Layers

### 1. Spec

Defines names, signatures, semantics, edge cases, and compatibility notes for operators, functions, and types.

### 2. Data Contract

Defines the in-memory representation and batch boundaries. Directionally Arrow-native.

### 3. Kernel

Execution primitives that implement spec-defined behavior over the data contract. This layer is currently limited to a narrow milestone-1 slice backed by accepted docs and local tests.

### 4. Runtime

Defines how kernels are scheduled, chained, canceled, instrumented, and backpressured. For the current milestone-1 slice, `tiforth` directly adopts the Arrow-bound runtime contract from `broken-pipeline-rs`; `tiforth`-owned operators and expressions attach on top of that contract rather than replacing it.

### 5. Adapters

Engine-facing translation layers for TiDB, TiFlash, and TiKV. Adapters should map engine concepts onto shared specs and contracts rather than owning semantics.

### 6. Harnesses

Conformance, differential, and performance harnesses that exercise the stack and report drift.

The first documented differential checkpoint is the TiDB-versus-TiFlash expression slice in `tests/differential/first-expression-slice.md`.

## Current Repository Bias

This reboot started in layers 1, 2, 4, 5, and 6. Layer 3 now enters only through minimal milestone-1 slices that are justified by docs and local tests.

The next end-to-end checkpoint should still grow layers 5 and 6 before it widens layer 3: `docs/design/next-thin-end-to-end-slice.md` fixes the follow-on slice as the first executable differential harness over the already-documented expression family.

`docs/design/kernel-expansion-acceptance.md` now defines the gate for any later layer-3 growth after that differential checkpoint exists.

## Current Minimal Kernel Boundary

The smallest currently useful kernel boundary is the milestone-1 expression-projection slice under `crates/tiforth-kernel`.

That boundary is intentionally narrow:

- one static Arrow batch source, one projection pipe, and one collecting sink for the local executable slice
- expression evaluation only for `column(index)`, `literal<int32>(value)`, and `add<int32>(lhs, rhs)`
- direct attachment to the adopted upstream `SourceOperator`, `PipeOperator`, and `SinkOperator` traits, with expressions kept as operator-local evaluators and field-derivation helpers
- reserve-first admission around computed output materialization
- governed-batch handoff and live-claim tracking through the source -> projection -> sink path
- local execution snapshots and checked-in fixtures that keep rows, errors, and ownership outcomes reviewable

This boundary is justified by `docs/spec/milestone-1-expression-projection.md`, `docs/spec/type-system.md`, `docs/contracts/data.md`, `docs/contracts/runtime.md`, and `tests/conformance/expression-projection-slice.md`.

It is not yet a general shared kernel API. It exists to prove one end-to-end path where shared specs, admission rules, handoff ownership, and local harness evidence all line up before broader operators or adapter-driven execution are introduced.

## Architectural Rules

- Specs own semantics.
- Contracts own boundaries.
- Adapters should be thin.
- Harnesses should be able to test specs independently of future kernels.

## Kernel Expansion Gate

After the current milestone-1 slice, layer 3 should grow only when:

- the next thin end-to-end differential slice already exists as reviewable evidence
- the proposed kernel growth is one narrow boundary with docs and harness coverage already named
- the resulting issue can show which shared surfaces change and which stay out of scope

## TODOs

- Choose the first post-gate kernel expansion boundary after the differential slice exists.
- Decide how adapter milestones should break down into tracked issues and harness checkpoints.
- Extend harness result and drift-report formats beyond the first differential expression slice.
