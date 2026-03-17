# Architecture

The current proposal is intentionally layered so specs and harnesses can mature before kernels exist.

## Proposed Layers

### 1. Spec

Defines names, signatures, semantics, edge cases, and compatibility notes for operators, functions, and types.

### 2. Data Contract

Defines the in-memory representation and batch boundaries. Directionally Arrow-native.

### 3. Kernel

Future execution primitives that implement spec-defined behavior over the data contract. This layer begins only with narrow milestone-1 slices backed by accepted docs and local tests.

### 4. Runtime

Defines how kernels are scheduled, chained, canceled, instrumented, and backpressured. Directionally inspired by broken-pipeline, but not committed to donor mechanics.

### 5. Adapters

Engine-facing translation layers for TiDB, TiFlash, and TiKV. Adapters should map engine concepts onto shared specs and contracts rather than owning semantics.

### 6. Harnesses

Conformance, differential, and performance harnesses that exercise the stack and report drift.

## Current Repository Bias

This reboot started in layers 1, 2, 4, 5, and 6. Layer 3 now enters only through minimal milestone-1 slices that are justified by docs and local tests.

## Architectural Rules

- Specs own semantics.
- Contracts own boundaries.
- Adapters should be thin.
- Harnesses should be able to test specs independently of future kernels.

## TODOs

- Define the smallest useful kernel boundary.
- Decide how plans map onto adapter milestones.
- Define artifact formats for harness results and drift reports.
