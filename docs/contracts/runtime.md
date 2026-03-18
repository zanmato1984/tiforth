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
- the current `tiforth-kernel` crate-root re-export of `ArrowTypes` and `Batch` is the only milestone-1 convenience layer needed for the adopted Arrow-bound specialization
- `tiforth` may add narrow convenience aliases or re-exports for the adopted Arrow-bound specialization, but it should not add a dedicated runtime facade, mirror the entire upstream crate, or rename runtime states unless a later multi-crate or adapter-visible boundary proves that necessary
- `tiforth` adds project-specific value around operator libraries, expression libraries, host admission hooks, adapter composition, and harness observability
- scheduler helpers, ready-made schedulers, and schedule-layer awaiter or resumer helpers stay outside the shared contract

## Milestone-1 Operator And Expression Attachment

For milestone 1, the operator and expression attachment pattern is fixed by `docs/design/operator-expression-runtime-attachment.md`.

- runtime-entered `tiforth` kernels implement the upstream `SourceOperator<ArrowTypes>`, `PipeOperator<ArrowTypes>`, and `SinkOperator<ArrowTypes>` traits directly
- runtime-visible results stay `OpOutput<Batch>`, and upstream task-state names stay adopted by name and meaning
- expression nodes such as `Expr` and `ProjectionExpr` stay operator-internal evaluators and schema helpers instead of runtime participants
- `ProjectionRuntimeContext`, `GovernedBatch`, `BatchClaim`, and `LocalExecutionSnapshot` may add admission, ownership, and observability support around the adopted runtime payload, but they do not replace the shared runtime protocol

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

## Milestone-1 Adapter-Local Orchestration Boundary

Detailed milestone-1 guidance lives in `docs/design/adapter-runtime-orchestration-boundary.md`.

For milestone 1, the shared runtime and differential adapter docs do **not** freeze:

- connection, authentication, DSN, or environment provisioning details
- engine-native SQL or expression derivation, planner hints, or session-variable choices
- retry, backoff, failover, timeout, or cancellation transport behavior inside one adapter
- buffering, streaming, pagination, or result-materialization strategy inside one adapter
- engine-plan capture, adapter-local diagnostics, or local debug traces beyond the normalized response fields
- adapter-local concurrency helpers, schedulers, or harness wrappers

The shared boundary does still fix:

- stable `slice_id`, `case_id`, `input_ref`, `projection_ref`, and `spec_refs[]` meanings for the first differential slice
- normalized `case result` fields and first-slice error vocabulary
- shared meanings for `tiforth` runtime, admission, and ownership events where the local kernel path is involved

This keeps engine transport and orchestration out of the shared runtime contract while still requiring adapters to preserve the shared semantic carrier and observable outcome meanings.

## Milestone-1 Dependency Boundary

For milestone 1:

- `broken-pipeline` is the required upstream runtime crate for shared or production-facing contracts
- `broken-pipeline-schedule` is allowed only as a dev, test, or harness dependency
- `broken-pipeline-c` is not part of the milestone-1 dependency boundary
- because the upstream crate manifests currently set `publish = false`, milestone-1 consumption uses a pinned git dependency rather than crates.io packaging

The current milestone-1 implementation pin is the git revision already recorded in `crates/tiforth-kernel/Cargo.toml`:

- `broken-pipeline = 174e6cd07c41210158ae1d805b568968cf71f898`
- `broken-pipeline-schedule = 174e6cd07c41210158ae1d805b568968cf71f898`

That revision is the current reproducible upstream contract snapshot for milestone 1. If `tiforth` later bumps that revision or vendors an upstream snapshot, that change should be handled as its own issue because it changes the verified runtime baseline.

Milestone 1 should keep using that direct git pin while all of these stay true:

- the repo still depends on one reviewed upstream snapshot
- local builds and harness runs can consume that snapshot directly from git
- packaging, offline, or audit requirements do not yet require a vendored upstream source tree

If one of those assumptions stops holding, a separate follow-on issue should review either a revision bump or a vendored snapshot explicitly instead of changing the upstream dependency path incidentally.

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

## Milestone-1 Stage Handoff And Ownership

- every data-bearing handoff uses the canonical semantic envelope from `docs/contracts/data.md`: adopted upstream `Batch` payload plus batch identity, origin metadata, and live ownership claims
- a successful handoff transfers live batch claims from the producing task to the in-flight batch; it does not require a fresh host admission decision and it does not add a new shared runtime state
- downstream stages may forward incoming claims unchanged, add new claims for newly materialized buffers, or drop claims only when the referenced bytes are no longer reachable from their outgoing batch or retained state
- runtime cancellation, sink drop, or error teardown must release any remaining live batch claims before the query is considered fully finished

Milestone 1 therefore keeps ownership transfer as `tiforth`-owned policy layered on top of the adopted upstream batch runtime, not as a replacement for that runtime.

Detailed handoff rationale lives in `docs/design/arrow-batch-handoff-ownership.md`.

## Observable Milestone-1 Events

The shared contract must make the following events observable to local tests and adapter integrations through recorded fixtures or equivalent translated event capture:

- `consumer_opened`
- `reserve_admitted`
- `reserve_denied`
- `consumer_shrunk`
- `batch_emitted`
- `batch_handed_off`
- `batch_released`
- `consumer_released`
- terminal runtime outcome: `finished`, `cancelled`, or `error`

The minimum event payload should let a harness correlate query, stage, and operator identity plus `batch_id`, consumer identity, claimed bytes, and final outcome. Milestone 1 does not freeze one tracing API; it freezes the event meanings that tests and adapters must be able to observe.

## Milestone-1 Local Snapshot Shape

For milestone-1 local Rust tests and harness scaffolding, `tiforth-kernel` now freezes one concrete snapshot carrier:

- `LocalExecutionSnapshot`
- `admission_events[]`: ordered `consumer_opened`, `reserve_admitted`, `reserve_denied`, `consumer_shrunk`, and `consumer_released` observations from `RecordingAdmissionController`
- `runtime_events[]`: ordered `batch_emitted`, `batch_handed_off`, `batch_released`, and terminal outcome observations from `ProjectionRuntimeContext`

This snapshot is the local Rust-side harness carrier only. It does **not** freeze an adapter-facing callback surface, tracing sink, or FFI wire format.

Milestone 1 guarantees ordering within each event family captured above. It does **not** yet guarantee one merged cross-family total order or timestamp field.

Local executable coverage should derive fixture assertions from this snapshot shape rather than stitching together recorder internals ad hoc.

## Milestone-1 Local Fixture Export

`LocalExecutionSnapshot` now exports one local conformance fixture carrier:

- `LocalExecutionFixture`
- `admission_events[]`: contract-named event records with primitive payload fields for `consumer_opened`, `reserve_admitted`, `reserve_denied`, `consumer_shrunk`, and `consumer_released`
- `runtime_events[]`: contract-named event records with primitive payload fields for `batch_emitted`, `batch_handed_off`, `batch_released`, `finished`, `cancelled`, and `error`

Checked-in local conformance artifacts should serialize `LocalExecutionFixture` as JSON with top-level `admission_events` and `runtime_events` arrays. Event records should keep `event` plus only the primitive payload keys that apply to that event; non-applicable optional fields should be omitted rather than emitted as `null`.

This fixture export exists so local Rust tests and early harness scaffolding can assert one stable, reviewable carrier without depending directly on recorder internals or on the exact Rust enum layout used underneath.

The carrier still includes `cancelled` because the shared runtime contract needs that terminal meaning. Milestone-1 projection tests now cover that outcome through one local-only cancellation driver: the test harness steps the compiled `pipe_exec()` directly until sink handoff becomes observable, then stops before the later `finished` step and records cancelled teardown after the sink-owned batches are dropped. This keeps the cancelled checkpoint honest for the local source -> projection -> sink slice without changing the adopted shared runtime contract or promoting a scheduler helper into it.

This fixture remains the canonical milestone-1 serialized event carrier. It does **not** freeze a merged cross-family total order, timestamps, or full serialized `claims[]` payloads.

## Milestone-1 Adapter-Visible Event Carrier Boundary

Detailed guidance lives in `docs/design/adapter-visible-runtime-event-carrier.md`.

For milestone 1:

- `LocalExecutionSnapshot` remains an internal Rust-side carrier and should not be exposed directly through adapter-visible boundaries
- adapter-visible integrations that need to expose local runtime, admission, or ownership events should translate those observations into `LocalExecutionFixture`-shaped records with primitive payload fields
- first-slice adapter `case result` fields stay unchanged; event fixtures remain optional sidecar evidence rather than required request or response payload
- callback-oriented event streaming is not part of the milestone-1 shared adapter or runtime boundary

## Minimal Adapter-Visible Error Taxonomy

For milestone 1, adapters should be able to distinguish at least:

- `memory_admission_denied`: the host denied a reserve request, including a spill-and-retry path that still ends in denial
- `memory_allocation_failed`: local allocation or Arrow materialization failed after admission succeeded
- `ownership_contract_violation`: `tiforth` attempted an illegal claim lifecycle transition, such as emitting governed bytes without a claim, shrinking or releasing a live batch claim, or double-releasing a consumer

Operator-specific compute failures such as arithmetic overflow remain outside this ownership taxonomy and continue to surface as ordinary operator errors.

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

- TODO: define how exchange, spill, and retry behaviors map onto the adopted runtime contract
- TODO: decide whether a later milestone needs a shared callback or streaming event surface after fixture-style translation is no longer sufficient

## Initial Boundary

For milestone 1, this contract now fixes the observable handoff, ownership, and error meanings that sit around the adopted `broken-pipeline-rs` Arrow-bound runtime surface, plus the local Rust-side snapshot carrier used by current executable coverage and the adapter-visible fixture-translation boundary under `docs/design/adapter-visible-runtime-event-carrier.md`. `tiforth` begins where operator, expression, admission, ownership, and adapter-layer semantics begin; adapter-local orchestration stays outside the shared contract under `docs/design/adapter-runtime-orchestration-boundary.md`, and the shared upstream runtime protocol itself remains upstream-owned.
