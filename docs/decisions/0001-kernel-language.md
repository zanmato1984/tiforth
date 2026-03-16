# 0001 - Kernel Language And Runtime Substrate

Status: proposed

Date: 2026-03-16

Related issues:

- #1 `design: choose tiforth shared-kernel language (C++ vs Rust)`
- #2 `investigate: rust-first memory accounting and spill blockers`
- #3 `design: broken-pipeline adaptation path for rust-first tiforth`
- #8 `design: host memory admission ABI for tiforth`
- #9 `design: tiforth dependency boundary over broken-pipeline-rs`
- #10 `milestone-1: first Arrow-bound operator and expression slice`

## Context

`tiforth` is being rebooted as a harness-first project that aims to unify operators and expressions across TiDB, TiFlash, and TiKV.

Two early design questions were treated as blockers for starting real implementation work:

1. What language should the shared kernel / runtime core use?
2. What runtime substrate should `tiforth` build on?

Issue #2 and issue #3 refined the picture substantially:

- milestone 1 does **not** require direct host-allocator routing for every internal Arrow allocation
- milestone 1 **does** require reserve-first host admission: reserve with the host first, fail on deny, then allocate internally
- `broken-pipeline-rs` now exists as a direct Rust substrate, so the runtime question is no longer about Rust plausibility; it is about adoption and dependency boundary
- `broken-pipeline-schedule` is explicitly treated as local test / harness support only, and this is not expected to change later

## Decision

### 1. Language

`tiforth` chooses **Rust** for the shared kernel / runtime core.

C++ remains relevant only as:

- historical lineage
- semantic reference when useful
- possible boundary interop material if a later narrow ABI requires it

C++ is **not** the primary implementation language for the shared `tiforth` kernel/runtime direction.

### 2. Runtime substrate

`tiforth` directly adopts the Arrow-bound runtime contract provided by `broken-pipeline-rs`.

`tiforth` does **not** invent an independent runtime contract for milestone 1.

Instead:

- `broken-pipeline-rs` provides the adopted runtime substrate
- `tiforth` provides operators and expressions on top of that substrate
- `tiforth` documentation records adoption scope, invariants, and project-specific constraints

### 3. Scheduler layer

`broken-pipeline-schedule` is **test / harness-only**.

It may be used for:

- local testing
- harness execution
- narrow validation of the first operator/expression slices

It is **not** the long-term production contract center for `tiforth`.

### 4. Memory model for milestone 1

`tiforth` adopts a **reserve-first host admission** model.

This means:

- before allocation or growth that must be governed, `tiforth` asks the host for memory admission
- if the host denies admission, `tiforth` fails instead of continuing
- after admission succeeds, `tiforth` may allocate internally using its own allocator strategy

Milestone 1 does **not** require every internal Arrow allocation path to directly route through the host allocator.

## Non-Goals

This decision does not mean:

- `tiforth` will define a new runtime framework separate from `broken-pipeline-rs`
- `tiforth` will use `broken-pipeline-schedule` as a permanent production scheduler dependency
- `tiforth` must preserve source-level compatibility with the original C++ `broken-pipeline`
- milestone 1 must solve direct host-allocator routing for every Arrow internal growth path

## Rationale

### Why Rust

- Rossi's default posture has consistently been Rust-first unless a real blocker appears
- no hard blocker remains after issue #2 and issue #3
- Rust fits the desired kernel/runtime direction, agent-friendly development style, and long-term correctness goals better than a C++-first reboot

### Why direct adoption of `broken-pipeline-rs`

- the runtime substrate now exists in Rust and no longer needs to be hypothesized
- adopting the existing Arrow-bound runtime contract is simpler and clearer than inventing a separate `tiforth` runtime contract prematurely
- it keeps `tiforth` focused on its real product surface: operators and expressions

### Why reserve-first instead of mandatory host-allocator routing

- reserve-first host admission captures the control requirement that matters for milestone 1
- it avoids making direct host-allocator routing a gating requirement for every Arrow internal path before the project has even shipped its first useful slice

## Consequences

### Positive

- the project can stop revisiting the Rust-vs-C++ choice as the default question
- runtime work can move from abstraction to concrete dependency-boundary design
- the first implementation slice can start with a narrower, more realistic memory-governance model

### Tradeoffs

- `tiforth` must now be explicit about where its own library boundary begins over `broken-pipeline-rs`
- reserve-first admission still requires careful operator and builder discipline
- some future host-allocator-routing work may still be valuable, but it is no longer the milestone-1 gate

## Immediate Follow-Up

The next implementation-facing work should focus on:

- #8 host memory admission ABI
- #9 dependency boundary over `broken-pipeline-rs`
- #10 first Arrow-bound operator/expression slice

## Revisit Conditions

This decision should be revisited only if one of the following happens:

- a concrete milestone-1 blocker appears that prevents `broken-pipeline-rs` adoption from working in practice
- host memory admission proves insufficient and direct allocator routing becomes mandatory earlier than expected
- upstream `broken-pipeline-rs` constraints make the adopted runtime contract unusable for the intended operator/expression layering
