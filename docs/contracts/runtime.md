# Runtime Contract

Direction: `tiforth` adopts and exposes the Arrow-bound runtime contract from `broken-pipeline-rs` as its primary shared runtime contract.

`tiforth` does not define an independent runtime protocol for milestone 1. The shared execution vocabulary comes from the upstream `broken-pipeline` crate specialized to `broken_pipeline::traits::arrow::ArrowTypes`. `tiforth`'s role is to provide operators, expressions, host admission hooks, and higher-level composition on top of that adopted contract.

`broken-pipeline-schedule` sits outside the shared contract. Within `tiforth`, it is reserved for local testing and harness work only, and this is not expected to change. `broken-pipeline-c` also sits outside the milestone-1 runtime contract and matters only if a later coarse interop issue explicitly calls for it.

The original C++ Broken Pipeline repository remains relevant only as provenance and lineage for the adopted model. It is not the active design center for `tiforth`.

## Adopted Runtime Shape

- small-step, resumable task execution that a scheduler can re-enter
- explicit runtime control states for continue, blocked, yield, finished, cancelled, and drain or flush work, or close semantic equivalents already present in the adopted contract
- explicit stage boundaries with observable handoff points
- Arrow-bound batch handoff as the primary data-bearing runtime surface
- host-orchestration hooks around the main streaming path when preparation or finalization work needs different scheduling
- scheduler-agnostic correctness, so runtime semantics do not depend on one particular executor implementation
- host memory admission before operator-owned growth or output materialization

## Adopted Arrow-Bound Reference Surface

As inspected from the public `broken-pipeline-rs` repo on 2026-03-16, the Arrow-bound shared contract currently centers on:

- `broken_pipeline::traits::arrow::{ArrowTypes, Batch, Error, Result}`
- `broken_pipeline::{SourceOperator, PipeOperator, SinkOperator}`
- `broken_pipeline::OpOutput<Batch>`
- `broken_pipeline::{TaskContext, TaskStatus, Awaiter, Resumer, SharedAwaiter, SharedResumer, Task, TaskGroup, TaskHint, TaskHintType}`

These names refer to the Rust upstream surface from `broken-pipeline-rs`, not to the original C++ repository.

The runtime state names that `tiforth` adopts by name and meaning are currently:

- `TaskStatus::{Continue, Blocked, Yield, Finished, Cancelled}`
- `OpOutput::{PipeSinkNeedsMore, PipeEven, SourcePipeHasMore, Blocked, PipeYield, PipeYieldBack, Finished, Cancelled}`

## Boundary Policy

- public `tiforth` operator and expression interfaces should use the adopted upstream Arrow-bound types directly when they need runtime-facing signatures
- `tiforth` may add narrow convenience aliases or re-exports for the adopted Arrow-bound specialization, but it should not mirror the entire upstream crate or rename runtime states
- `tiforth` adds project-specific value around operator libraries, expression libraries, host admission hooks, adapter composition, and harness observability
- scheduler helpers, ready-made schedulers, and schedule-layer awaiter or resumer helpers stay outside the shared contract

## Shared Contract Surface

This adopted contract should remain precise about:

- Arrow-bound stage inputs and outputs
- pull, push, or mixed handoff semantics used by the adopted runtime
- lifecycle events and runtime control states
- cancellation, drain, yield, and error propagation behavior
- resource ownership and admission expectations at runtime boundaries
- metrics and tracing hooks required by harnesses and adapters
- the separation between upstream runtime protocol and adapter-specific orchestration

`broken-pipeline-schedule` is not part of this shared contract. Its ready-made schedulers and helpers are for local testing and harness use only. Detailed milestone-1 dependency guidance lives in `docs/design/broken-pipeline-boundary.md`.

## Milestone-1 Dependency Boundary

For milestone 1:

- `broken-pipeline` is the required upstream runtime crate for shared or production-facing contracts
- `broken-pipeline-schedule` is allowed only as a dev, test, or harness dependency
- `broken-pipeline-c` is not part of the milestone-1 dependency boundary
- because the upstream crate manifests currently set `publish = false`, the first implementation slice will need a pinned git dependency or vendored source rather than crates.io packaging

## Host Memory Admission Boundary

For milestone 1, runtime-visible memory governance follows the reserve-first design from issue #2 and issue #8.

- `tiforth` opens a host-visible memory consumer for each operator or runtime-owned memory domain that needs distinct attribution or spillability
- before governed allocation or reallocation, `tiforth` calls the host admission ABI to reserve the additional bytes required for the intended peak live growth
- if the host denies the request, `tiforth` does not allocate on that path; it either spills and retries within operator policy or returns an execution error
- after admission succeeds, `tiforth` may allocate internally through its own Rust and Arrow allocation paths
- when live bytes drop but the consumer remains active, `tiforth` reports the decrease via `shrink`
- when the consumer finishes, is cancelled, or errors out, `tiforth` `release`s all remaining admitted bytes
- the admission ABI is synchronous, callback-free, and does not add a new shared runtime state beyond success, spill-and-retry within the operator, or error propagation

Detailed admission semantics live in `docs/design/host-memory-admission-abi.md`.

## Non-Goals

- inventing a `tiforth`-only runtime contract above `broken-pipeline-rs`
- treating `broken-pipeline-schedule` as a production or shared-contract dependency
- centering the original C++ Broken Pipeline repository as the active design source
- locking the future scheduler design in this skeleton
- embedding engine-specific control flow into shared runtime contracts
- pretending the data contract and runtime contract can be designed independently
- requiring transparent spill inside Arrow allocation internals
- requiring direct host-allocator routing for every Arrow growth path in milestone 1

## Open Questions

- TODO: pin the exact upstream `broken-pipeline-rs` revision or vendored snapshot once implementation work starts
- TODO: decide whether `tiforth` needs a small convenience re-export module for Arrow-bound runtime aliases or whether direct upstream imports are sufficient
- TODO: define how `tiforth` operators and expressions attach to the adopted contract without renaming its runtime states
- TODO: define which runtime events must be observable in tests when exercising the adopted contract
- TODO: define error taxonomy and propagation guarantees expected by adapters, including memory-admission denial and internal allocation failure
- TODO: define memory ownership and accounting hooks between adopted runtime boundaries and `tiforth` operators
- TODO: define ownership transfer rules between runtime stages for already-admitted buffers and claimed batches
- TODO: define how exchange, spill, and retry behaviors map onto the adopted runtime contract
- TODO: decide what adapter-specific orchestration stays outside the shared contract

## Initial Boundary

For now, this contract exists to constrain harness, adapter, and operator design around the adopted `broken-pipeline-rs` Arrow-bound runtime surface and the reserve-first admission boundary. `tiforth` begins where operator, expression, admission, and adapter-layer semantics begin; the shared runtime protocol itself remains upstream-owned. It should become more precise by recording adopted upstream types, operator hooks, and observable memory-governance events before any real kernel scheduling code is introduced.
