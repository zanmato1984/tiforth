# Milestone-1 Spill And Retry Mapping On The Adopted Runtime Contract

Status: issue #123 design checkpoint

Verified: 2026-03-18

Related issues:

- #8 `design: host memory admission ABI for tiforth`
- #19 `design: define milestone-1 Arrow batch handoff and memory-ownership contract`
- #86 `design: define milestone-1 operator and expression attachment to adopted runtime contract`
- #123 `design: define milestone-1 spill and retry runtime mapping`

## Question

How should milestone-1 spill and retry behavior map onto the adopted `broken-pipeline-rs` runtime contract without introducing new shared runtime states?

## Inputs Considered

- `docs/contracts/runtime.md`
- `docs/contracts/data.md`
- `docs/design/host-memory-admission-abi.md`
- `docs/design/operator-expression-runtime-attachment.md`
- `crates/tiforth-kernel/src/admission.rs`
- `crates/tiforth-kernel/src/snapshot.rs`
- issue #123

## Design Summary

For milestone 1, spill and retry stay operator-local policy wrapped around reserve-first admission calls. They do not introduce new shared `TaskStatus` or `OpOutput` meanings.

The mapping is:

- reserve success continues on ordinary adopted runtime paths
- reserve denial records normal admission evidence and may trigger operator-local spill when that consumer is spillable
- retry is another reserve-before-allocate attempt under the same operator step policy
- unrecovered denial surfaces as an ordinary runtime error outcome (`memory_admission_denied`)

## Runtime-State Mapping Rules

- reserve denial by itself does **not** map to a new shared runtime state
- reserve denial by itself does **not** require `TaskStatus::Blocked`, `TaskStatus::Yield`, `OpOutput::Blocked`, or `OpOutput::PipeYield`
- if an operator chooses cooperative yielding while executing a long local spill loop, that yield remains scheduler cooperation only; it is not a new deny-specific semantic state
- unrecovered denial propagates through ordinary operator error paths and ends in the existing terminal `error` meaning
- successful retry returns to normal operator flow and can produce any already-adopted runtime output state that the operator would otherwise emit

## Spillable Versus Unspillable Consumer Paths

### Spillable Consumer

1. `try_reserve` is denied.
2. operator may spill resident state under local policy.
3. operator reports released resident bytes via `shrink` or `release`.
4. operator may retry `try_reserve` for the needed growth.
5. on admit, operator continues normal execution.
6. if retries are exhausted or spill cannot free enough memory, operator returns `memory_admission_denied`.

### Unspillable Consumer

1. `try_reserve` is denied.
2. operator fails that path immediately with `memory_admission_denied`.
3. no spill attempt is required or implied.

Retry loops must be bounded by operator or runtime policy; the shared contract does not imply infinite retries.

## Observable Milestone-1 Spill Outcomes

Milestone-1 observability keeps the existing contract event vocabulary:

- each denied attempt is observable as `reserve_denied`
- reclaiming resident bytes before retry is observable as `consumer_shrunk` or terminal `consumer_released`
- successful retry is observable as `reserve_admitted`
- unrecovered denial is observable through terminal `error` plus the `memory_admission_denied` classification

No additional spill-specific event names are required by the milestone-1 shared contract.

## Data-Contract Boundary For Spill

- canonical batch envelopes and `claims[]` describe resident governed bytes reachable from live batches
- bytes moved to disk by operator-managed spill are outside the live-batch claim surface while non-resident
- spill metadata or read buffers that remain resident stay governed memory and still require reserve-before-allocate accounting
- rehydrating spilled state is a fresh reserve-before-allocate step and may create new claims only for newly resident governed buffers

This keeps spill explicit and compatible with the milestone-1 claim-carrying handoff model.

## Deferred Boundary

This checkpoint does **not** define:

- exchange operator runtime semantics
- adapter-local timeout, retry, or transport policy
- spill algorithm details, file formats, or disk-manager interfaces
- callback-oriented event streaming beyond the current fixture translation boundary
