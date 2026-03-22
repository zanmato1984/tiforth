# `tiforth` Dependency Boundary Over `broken-pipeline-rs`

Status: issue #9 design checkpoint, issue #62 revision pin

Verified: 2026-03-18

Related issues:

- #3 `design: broken-pipeline adaptation path for rust-first tiforth`
- #9 `design: tiforth dependency boundary over broken-pipeline-rs`
- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #62 `design: pin milestone-1 broken-pipeline-rs revision in runtime docs`

## Question

After issue #3 established that `tiforth` should directly adopt the Arrow-bound runtime contract from `broken-pipeline-rs`, what exact dependency boundary should `tiforth` keep for milestone 1?

The remaining question is not whether `tiforth` should invent a different runtime substrate. The remaining question is where the shared upstream contract stops and where `tiforth`-owned operator, expression, admission, and adapter surfaces begin.

## Inputs Considered

- `README.md`
- `docs/decisions/0001-kernel-language.md`
- `docs/contracts/runtime.md`
- `docs/design/broken-pipeline-adaptation.md`
- issue #9
- issue #3 and issue #10 for surrounding context
- public `zanmato1984/broken-pipeline-rs` README, workspace manifests, and selected public exports inspected on 2026-03-16

## Observed Upstream Facts

Observed from the public `broken-pipeline-rs` repo on 2026-03-16:

- the workspace currently exposes `broken-pipeline`, `broken-pipeline-schedule`, and `broken-pipeline-c`
- `broken-pipeline` is the core protocol crate and exports Arrow-bound types under `broken_pipeline::traits::arrow`
- the Arrow binding currently uses `Batch = Arc<RecordBatch>` and `Error = ArrowError`, while `PipelineTypes` now also carries a typed task-context payload
- the core crate publicly exports `SourceOperator`, `PipeOperator`, `SinkOperator`, `OpOutput`, `TaskContext`, `TaskStatus`, `Pipeline`, `PipelineChannel`, `compile`, and `PipeExec`
- the optional schedule crate is generic over `ScheduleTypes` and ships ready-made scheduler fronts plus detail awaiter or resumer helpers
- the current crate manifests set `publish = false`, so milestone-1 consumption currently implies a pinned git dependency or vendored source rather than crates.io packaging

Observed from the current `tiforth` manifests on 2026-03-17:

- `crates/tiforth-kernel/Cargo.toml` currently pins both `broken-pipeline` and `broken-pipeline-schedule` to git revision `caf70aacc384e62630ee0ae71e88c513a81b16dd`
- that revision is the current reproducible upstream contract snapshot for the milestone-1 implementation slice

## Boundary Decision

### 1. Required upstream crates

The milestone-1 dependency boundary is intentionally narrow:

- `broken-pipeline` is the required shared runtime dependency
- `broken-pipeline-schedule` is allowed only in local tests and harnesses
- `broken-pipeline-c` is outside the milestone-1 boundary and should not be pulled in by default

This keeps the production and shared contract centered on the upstream core crate rather than the optional scheduler or the C ABI layer.

### 1a. Milestone-1 revision pin

The milestone-1 dependency boundary now records the exact upstream revision used by the checked-in implementation:

- `broken-pipeline = caf70aacc384e62630ee0ae71e88c513a81b16dd`
- `broken-pipeline-schedule = caf70aacc384e62630ee0ae71e88c513a81b16dd`

That pin keeps the runtime evidence base reproducible while the upstream crates remain unpublished on crates.io. A later revision bump or vendored snapshot should be handled as a separate issue so the contract review and implementation change stay explicit.

Milestone 1 should continue using that pinned git snapshot directly while all of these stay true:

- the checked-in kernel slice still depends on one reviewed upstream snapshot
- local builds and harness runs can consume that snapshot directly from git
- packaging, offline, or audit requirements do not yet require a checked-in upstream source mirror

A later issue should switch to a vendored snapshot only when at least one of those assumptions breaks. Examples include an offline packaging requirement, a reproducibility review that needs the exact upstream source tree checked into this repo, or an upstream-availability risk that makes the direct git pin insufficient.

### 2. Directly adopted upstream contract surfaces

The following surfaces are upstream-owned and should be adopted directly instead of being redefined inside `tiforth`:

- runtime typing: `broken_pipeline::PipelineTypes` plus Arrow data payloads under `broken_pipeline::traits::arrow::{Batch, Error, Result}`
- operator attachment traits: `broken_pipeline::{SourceOperator, PipeOperator, SinkOperator}`
- operator step outputs: `broken_pipeline::OpOutput<TiforthTypes::Batch>`
- task and blocking control: `broken_pipeline::{TaskContext, TaskStatus, Awaiter, Resumer, SharedAwaiter, SharedResumer, TaskGroup}` specialized to `TiforthTypes`

These names refer to the Rust upstream surface from `broken-pipeline-rs`, not to the original C++ repository.

`tiforth` should keep the adopted runtime state names unchanged. In particular, it should not rename or shadow:

- `TaskStatus::{Continue, Blocked, Yield, Finished, Cancelled}`
- `OpOutput::{PipeSinkNeedsMore, PipeEven, SourcePipeHasMore, Blocked, PipeYield, PipeYieldBack, Finished, Cancelled}`

### 3. `tiforth`-owned surfaces

`tiforth` begins above the adopted runtime substrate. It owns:

- concrete operators and expression implementations
- operator and expression descriptors, registries, and composition helpers
- host admission and memory-governance hooks that sit around operator-owned allocation or growth
- adapter-facing packaging, composition guidance, and engine-specific glue kept outside the shared runtime contract
- conformance and harness expectations that assert how `tiforth` uses the adopted runtime contract

These are the places where `tiforth` adds project-specific value. They should attach to the upstream runtime contract rather than replace it.

### 4. Re-export and wrapping policy

The public boundary should stay transparent:

- `tiforth` public runtime-facing signatures may use the adopted upstream Arrow-bound types directly
- the current `tiforth-kernel` crate-root re-export of `ArrowTypes` and `Batch` is sufficient ergonomic surface for the milestone-1 kernel crate
- `tiforth` should not add a dedicated runtime-facade module unless a later multi-crate or adapter-visible boundary proves that the existing narrow re-export is no longer enough
- `tiforth` may add narrow convenience aliases or selective re-exports for ergonomics, but it should not publish a wide `pub use` mirror of the upstream crate
- wrappers are appropriate only when they add `tiforth`-specific policy such as host admission, operator factory wiring, expression binding, or harness observability
- wrappers must not create renamed copies of upstream runtime states or a second task or operator protocol

The intended result is that a contributor can read a `tiforth` runtime-facing signature and still understand that the underlying contract remains the upstream `broken-pipeline` contract.

### 5. Internal-only and harness-only surfaces

Some upstream surfaces should remain outside the shared public boundary:

- `broken-pipeline-schedule` is always test or harness-only, including its ready-made schedulers and detail awaiter or resumer helpers
- `broken-pipeline-schedule::Traits` may be convenient in local tests, but it should not become the production contract center or leak into adapter-facing docs as the required dependency
- `broken-pipeline-c` remains a later interop option only if a separate coarse ABI issue makes it necessary
- the original C++ Broken Pipeline repository remains lineage and provenance only

If a future production scheduler is added, it should target the adopted `broken-pipeline` core contract rather than making the schedule crate the permanent dependency center.

## Boundary Table

| Area | Upstream-owned surface | `tiforth` role |
| --- | --- | --- |
| Arrow runtime typing | `ArrowTypes`, `Batch`, `Error`, `Result` | use directly; do not define renamed copies |
| Operator protocol | `SourceOperator`, `PipeOperator`, `SinkOperator`, `OpOutput` | implement operators and expressions against the adopted traits and state machine |
| Task and blocking control | `TaskContext`, `TaskStatus`, `Awaiter`, `Resumer`, `TaskGroup` | supply context payloads, admission hooks, and observability around the adopted contract |
| Pipeline assembly and execution | `Pipeline`, `PipelineChannel`, `compile`, `PipeExec`, `PipelineExec` | build operator composition helpers and milestone slices on top |
| Scheduler fronts | `broken-pipeline-schedule::*` | keep local to tests and harnesses only |
| C ABI | `broken-pipeline-c` | keep out of milestone 1 unless a later interop issue says otherwise |

## Practical Consequences

- milestone 1 can start without inventing a second runtime API inside `tiforth`
- issue #10 should build directly on `broken-pipeline` and use `broken-pipeline-schedule` only in local test or harness execution
- adapter-facing and host-facing work should target the adopted core runtime contract, not the optional schedule crate API
- dependency pinning matters because the upstream crates are not currently published on crates.io

## Exit Criteria Answers

Issue #9 asks three practical questions.

### Which upstream crates are required for milestone 1?

- required shared runtime crate: `broken-pipeline`
- allowed local test or harness helper crate: `broken-pipeline-schedule`
- not required for milestone 1: `broken-pipeline-c`

### Which upstream types are directly used versus wrapped?

- directly used or selectively re-exported: the Arrow-bound runtime types, operator traits, task control types, and pipeline execution types listed above
- wrapped: only `tiforth`-specific policy surfaces such as admission-aware helpers, operator or expression assembly helpers, and harness observability helpers
- kept internal: schedule-layer helpers, ready-made schedulers, and the C ABI layer

### Where does `tiforth` begin and `broken-pipeline-rs` end?

`broken-pipeline-rs` owns the shared runtime protocol and Arrow-bound execution vocabulary. `tiforth` begins where operator semantics, expression semantics, host admission policy, adapter composition, and harness expectations are layered on top of that adopted protocol.
