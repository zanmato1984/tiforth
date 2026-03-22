# First Go Host Off-Heap Interop Boundary

Status: issue #363 design checkpoint

Verified: 2026-03-21

Related issues:

- #8 `design: host memory admission ABI for tiforth`
- #9 `design: tiforth dependency boundary over broken-pipeline-rs`
- #19 `design: define milestone-1 Arrow batch handoff and memory-ownership contract`
- #282 `design: define first off-heap state ownership boundary`
- #363 `design: document first Go host off-heap interop boundary for tiforth`

## Question

What first durable boundary should `tiforth` set for Go-hosted off-heap Arrow interop while keeping shared data and runtime contracts intact?

## Inputs Considered

- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/design/host-memory-admission-abi.md`
- `docs/design/arrow-batch-handoff-ownership.md`
- `docs/design/first-off-heap-state-ownership-boundary.md`
- `docs/design/broken-pipeline-boundary.md`
- issue #363

## Design Summary

- the first interop checkpoint is host-driven and reserve-first: the host controls compile and execution stepping while preserving the existing admission contract
- host-owned off-heap input Arrow batches are borrowed read-only by `tiforth`; host-provided buffers do not transfer ownership to `tiforth`
- output batches materialized by `tiforth` remain governed `tiforth`-owned bytes until the host explicitly releases or drops the corresponding ownership references
- this remains a semantic boundary only: no frozen C ABI, no universal foreign-buffer bridge, and no new shared runtime states

## Host Control Flow Boundary

1. the host assembles operators and compiles execution using the adopted upstream runtime (`compile` + `pipe_exec`)
2. the host drives `pipe_exec` through explicit step calls until one terminal outcome: `finished`, `cancelled`, or `error`
3. step calls may produce emitted batches; batching, forwarding, or buffering decisions stay host-local orchestration
4. on cancellation or error, the host still drives teardown so outstanding governed ownership is released

## Input Ownership Boundary

- the Go host owns imported off-heap input buffers and outer batch carriers
- `tiforth` consumes those inputs through borrowed runtime payloads and may forward them zero-copy when lifetime rules allow
- `tiforth` must not free, mutate, or re-parent host-owned input buffers
- when `tiforth` needs writable or longer-lived state than host input borrows permit, it allocates governed bytes under its own admission consumers

## Output Ownership Boundary

- buffers materialized by `tiforth` remain admitted governed bytes under `tiforth` consumer ownership
- emitted output batches must carry ownership metadata sufficient for later host-triggered release
- the host may either copy outputs into host-owned memory or hold read-only borrowed views
- while the host keeps borrowed output views alive, related governed claims remain live
- once the host no longer needs an output view, it must drive release or drop so `tiforth` can `shrink` or `release` admitted bytes

## Deferred Boundary

This checkpoint does not define:

- a concrete C ABI or concrete Go binding API
- a frozen cross-language Arrow foreign-buffer bridge for all embeddings
- asynchronous host callbacks or scheduler replacement
- adapter-local session, connection, or retry orchestration policy

## Planned Validation Slice

The next Go-host checkpoint should remain narrow and executable:

- validate one minimal host-facing FFI slice for `compile` -> `pipe_exec` ->
  repeated `step` -> explicit release or drop
- prove host-owned input borrowing, `tiforth`-owned output release, and
  terminal teardown on `finished`, `cancelled`, and `error`
- record memory-management expectations for who allocates, who may borrow, and
  who must trigger final release on every buffer domain that crosses the FFI
  surface
- add benchmark evidence for direct Rust stepping versus Go-through-`cgo`
  stepping so the repo can judge the control-loop overhead explicitly

That follow-on should stay a validation slice, not a permanent public-ABI
commitment. It should not be treated as permission to freeze one universal C
ABI, async callback model, or general Arrow foreign-buffer bridge for every
embedding.

## Result

`tiforth` now has a first durable Go-host off-heap interop checkpoint: host-driven compile and step control flow, host-owned input borrowing, and explicit host release responsibilities for `tiforth`-created output batches.
