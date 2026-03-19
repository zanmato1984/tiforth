# Milestone-1 Exchange Mapping On The Adopted Runtime Contract

Status: issue #125 design checkpoint

Verified: 2026-03-18

Related issues:

- #80 `design: define next thin end-to-end slice after milestone-1 projection`
- #86 `design: define milestone-1 operator and expression attachment to adopted runtime contract`
- #92 `design: define adapter-local runtime orchestration boundary`
- #123 `design: define milestone-1 spill and retry runtime mapping`
- #125 `design: define milestone-1 exchange runtime mapping boundary`

## Question

How should milestone-1 exchange behavior map onto the adopted `broken-pipeline-rs` runtime contract without inventing new shared runtime states?

## Inputs Considered

- `docs/architecture.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/adapter-runtime-orchestration-boundary.md`
- `docs/design/operator-expression-runtime-attachment.md`
- `docs/design/spill-retry-runtime-mapping.md`
- `docs/design/next-thin-end-to-end-slice.md`
- issue #125

## Design Summary

Milestone 1 keeps exchange out of the shared runtime surface. The accepted shared kernel path remains source -> projection or filter -> sink, and no exchange operator is introduced for this checkpoint.

When milestone-1 workflows still need fan-out or fan-in behavior (for example, adapter or harness orchestration), that behavior stays adapter-local and must not rename or extend adopted upstream runtime-state meanings.

## Milestone-1 Mapping Boundary

- milestone-1 shared runtime semantics do not define an in-contract exchange operator boundary yet
- any exchange-like orchestration outside the current shared kernel slice remains adapter-local and out of shared-contract scope
- adopted runtime states and outputs (`TaskStatus` and `OpOutput`) keep their existing meanings; no exchange-specific state names are added
- if an operator step performs buffering or transfer work, reserve-before-allocate admission still applies before resident memory growth
- any admitted resident bytes that remain reachable from live batches still use claim-carrying handoff and release rules from the data and runtime contracts

This fixes the current "how does exchange map?" answer for milestone 1: it maps as deferred shared-kernel scope plus adapter-local orchestration, not as a new shared runtime protocol.

## Why This Boundary

- it matches the current accepted kernel boundary and executable coverage, which does not yet include exchange operators
- it prevents premature protocol growth while preserving the adopted upstream runtime vocabulary
- it keeps memory-admission and ownership semantics consistent even when local orchestration stages buffer or transfer data
- it preserves room for a later narrow exchange issue that can define one concrete in-contract slice with harness coverage

## Follow-On Requirements

Any later issue that introduces an in-contract exchange slice should explicitly define:

- the first affected slice or operator boundary
- stage-level send or receive behavior and backpressure mapping on adopted runtime states
- cancellation, drain, and error propagation behavior through existing runtime outcomes
- ownership-claim behavior for buffered, forwarded, and released batches
- conformance or differential evidence that exercises the new exchange boundary

## Deferred Boundary

This checkpoint does **not** define:

- an exchange algorithm, partitioning strategy, or transport implementation
- adapter-local network, timeout, retry, or scheduling policy
- a new callback or streaming event surface beyond existing fixture translation boundaries
