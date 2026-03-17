# Broken-Pipeline Adoption And Layering For `tiforth`

Status: issue #3 design checkpoint

Verified: 2026-03-16

Related issues:

- #1 `design: choose tiforth shared-kernel language (C++ vs Rust)`
- #3 `design: broken-pipeline adaptation path for rust-first tiforth`

## Question

Given that `broken-pipeline-rs` now exists as a native Rust port, how should `tiforth` adopt and expose its Arrow-bound runtime contract directly while positioning `tiforth` itself as a library of operators and expressions on top?

This is no longer mainly a question about inventing a `tiforth`-owned runtime contract above an upstream dependency. The clarified direction is that `tiforth` should directly adopt the Arrow-bound runtime contract from `broken-pipeline-rs`, keep `broken-pipeline-schedule` strictly local to testing and harness work, and treat the original C++ Broken Pipeline repository as lineage only.

## Inputs Considered

- `README.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `plans/completed/language-decision.md`
- `inventory/language-constraints.md`
- `inventory/language-scorecard.md`
- issue #1 and issue #3
- PR #5
- public `broken-pipeline-rs` README and crate layout inspected on 2026-03-16

## Current Public Repo Facts

Observed from the public `broken-pipeline-rs` repo on 2026-03-16:

- `broken-pipeline-rs` is a public native Rust port of Broken Pipeline
- the workspace currently exposes three crates: `broken-pipeline`, `broken-pipeline-schedule`, and `broken-pipeline-c`
- `broken-pipeline` is the core protocol crate and includes pipeline compilation, a Rust `PipeExec` reference runtime, and Arrow-bound traits under `broken_pipeline::traits::arrow`
- `broken-pipeline-schedule` is an optional Arrow-bound schedule layer with ready-made scheduler front-ends
- `broken-pipeline-c` is a focused C API for task-group interop
- the current crate manifests are marked `publish = false`, so direct dependency currently implies a Git dependency or vendored source rather than crates.io packaging

## Clarified Adoption Stance

The runtime framing for `tiforth` should now be explicit:

- the primary shared runtime contract is the Arrow-bound runtime contract already provided by `broken-pipeline-rs`
- `tiforth` should adopt and expose that contract directly rather than inventing an independent runtime contract above it
- `tiforth` should be understood as a provided library of operators, expressions, and related semantics on top of that adopted contract
- `broken-pipeline-schedule` is for local testing and harness work only, and this is not expected to change later
- the original C++ Broken Pipeline repository matters as provenance and lineage, not as the primary design center

## What `tiforth` Adopts

For issue #3, the adopted contract should preserve and expose:

- the same small-step, resumable execution shape provided by the upstream Rust port
- the same observable blocked, yield, finish, cancel, and drain transitions, or close semantic equivalents already present in the adopted runtime
- the same stage-boundary and handoff behavior at the Arrow-bound runtime-contract level
- the same scheduler-agnostic split between semantic runtime rules and executor choice
- the same Arrow-bound batch vocabulary that the upstream Rust port already treats as the first-class runtime binding

## What `tiforth` Adds On Top

`tiforth` should contribute above the adopted contract, not around it:

- operator libraries
- expression libraries
- harness expectations and conformance cases for the adopted runtime behavior
- adapter-facing packaging and composition guidance that does not redefine the runtime substrate

## Boundary Rules

The intended boundaries are now straightforward:

- depend directly on the upstream Rust core and its Arrow-bound runtime contract as the primary shared substrate
- keep `broken-pipeline-schedule` out of the shared contract and use it only for local testing and harness execution
- reserve `broken-pipeline-c` for coarse adapter interop only if a later milestone needs it
- avoid reframing the original C++ headers or scheduler details as the target contract for `tiforth`

## Recommendation

`tiforth` should take a direct-adoption path:

- adopt the Arrow-bound runtime contract from `broken-pipeline-rs` as the shared runtime contract
- build `tiforth` as a library of operators and expressions on top of that contract
- treat `broken-pipeline-schedule` as harness-only support machinery
- treat `broken-pipeline-c` as a secondary interop option, not the core answer to issue #3
- keep references to the original C++ Broken Pipeline repository limited to provenance, lineage, and non-goals

## Impact On The Language Decision

Issue #3 now shows more than a plausible Rust adaptation path.

The more precise result is:

- `broken-pipeline-rs` already supplies the Arrow-bound runtime contract that `tiforth` should adopt directly
- `broken-pipeline` is not currently a blocker against a Rust-first kernel
- the remaining design work is about operator layering, dependency packaging, and keeping harness-only scheduler utilities out of the shared contract
- a blocker would appear only if `tiforth` discovers a concrete semantic mismatch in the upstream Rust runtime contract or a separate blocker from issue #2

## Immediate Follow-up Work

- refine `docs/contracts/runtime.md` so it clearly records the direct-adoption stance
- align `inventory/language-constraints.md` and `inventory/language-scorecard.md` with the operator-on-top framing
- if milestone work later depends directly on a specific upstream revision, record that dependency choice and why it remains acceptable
