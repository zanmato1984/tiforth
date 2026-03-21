# Host Memory Admission ABI For `tiforth`

Status: issue #8 design checkpoint

Verified: 2026-03-16

Related issues:

- #2 `investigate: rust-first memory accounting and spill blockers`
- #8 `design: host memory admission ABI for tiforth`
- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #19 `design: define milestone-1 Arrow batch handoff and memory-ownership contract`
- #123 `design: define milestone-1 spill and retry runtime mapping`

## Question

What host memory admission ABI should `tiforth` expose for milestone 1 if the governing rule is reserve with the host first, fail on deny, then allocate internally?

The answer needs to stay compatible with direct adoption of the `broken-pipeline-rs` runtime contract and with hosts implemented in Go, C++, or Rust.

## Inputs Considered

- `README.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/contracts/runtime.md`
- `docs/contracts/data.md`
- `docs/decisions/0001-kernel-language.md`
- `inventory/memory-accounting-blocker.md`
- issue #8
- issue #2
- issue #10

## Design Summary

The milestone-1 contract should be a consumer-scoped, reserve-first admission ABI with four operations:

- `open_consumer(descriptor) -> consumer_handle`
- `try_reserve(consumer_handle, delta_bytes) -> admitted | denied`
- `shrink(consumer_handle, delta_bytes)`
- `release(consumer_handle)`

The key rules are:

- `try_reserve` is the only fallible hot-path admission call
- `shrink` and `release` return previously admitted bytes and must not be denyable policy decisions
- requests cover the intended peak additional live bytes for the next growth step, not merely the logical payload bytes written
- denial does not create a new shared runtime state; it leads to spill-and-retry inside operator policy or to normal error propagation
- operator-managed spill remains explicit and above Arrow internals

This is a semantic ABI definition, not yet a frozen wire or FFI layout. Rust traits, C ABIs, or Go wrappers may expose different concrete signatures so long as they preserve these semantics.

## Core Invariants

- governed live bytes for a consumer must never intentionally exceed that consumer's currently admitted bytes
- no governed allocation or reallocation may start before the required additional bytes have been admitted
- a deny leaves the consumer's current admitted balance unchanged
- admission is not retroactive accounting and not an async-revocable lease; once bytes are admitted, they stay admitted until `tiforth` shrinks or releases them, or the enclosing query is torn down through normal runtime cancellation
- `tiforth` may allocate internally after admission succeeds; milestone 1 does not require the host to own the resulting Arrow buffers or to provide the allocator used for ordinary builder growth

## Consumer Model

Admission is scoped to a host-visible memory consumer rather than to anonymous calls.

A consumer represents one operator-owned or runtime-owned memory domain with stable attribution and spillability.

At minimum, the consumer descriptor should carry equivalent information to:

- query identifier
- stage or pipeline identifier
- operator or runtime owner identifier
- consumer kind such as Arrow output, operator state, or scratch
- spillability classification
- an optional human-readable debug label

One consumer handle should not mix unrelated spillability classes or ownership domains. If one operator owns both spillable state and unspillable output buffers, those should be separate consumers.

## Canonical Operation Semantics

### 1. `open_consumer`

`open_consumer` creates the attribution and reservation domain used by later calls.

- it should happen before the first governed allocation for that domain
- it returns an opaque handle that `tiforth` passes back on later calls
- the handle may move across threads between calls, but `tiforth` should not issue concurrent calls against the same handle from multiple runtime tasks
- distinct consumer handles may be used concurrently

An embedding may fuse this step into constructor or registration machinery. The required part of the contract is the existence of a stable consumer identity with the semantics above.

### 2. `try_reserve`

`try_reserve` requests additional admitted bytes for one consumer.

- the request is delta-based: it asks for additional bytes beyond the consumer's current admitted balance
- the host responds all-or-nothing for milestone 1; partial grants are out of scope
- on success, the consumer's admitted balance increases by `delta_bytes`
- on deny, the admitted balance does not change and `tiforth` must not perform the governed allocation on that path
- a deny may carry a host reason code for logging or metrics, but the minimal semantic result is admit or deny

`delta_bytes` must represent a conservative upper bound for the next growth step's additional live resident bytes.

That estimate must include, when relevant:

- capacity rounding or amortized builder growth chosen by `tiforth`
- temporary overlap between old and new storage during reallocation
- scratch structures that are part of the same governed memory domain for that step

If a growth path cannot bound its next-step peak reasonably, that is a design bug in the operator or helper and should be fixed there rather than papered over by post-allocation accounting.

### 3. Growth And Reallocation After Admission

After `try_reserve` succeeds, `tiforth` may allocate internally.

- ordinary Rust and Arrow allocation paths remain valid after admission
- if the operation later discovers it needs more bytes than were admitted, it must make another `try_reserve` call before continuing that growth
- `tiforth` must not deliberately overrun the admitted balance and reconcile afterward
- if the chosen growth algorithm reserved for a conservative peak and the actual live resident bytes settle lower, `tiforth` returns the difference through `shrink`

This keeps the admission boundary explicit while staying compatible with ordinary Arrow builders and internal Rust-owned allocators.

### 4. `shrink`

`shrink` returns part of a consumer's admitted balance while keeping the consumer alive.

- use it when live resident bytes drop but the consumer still exists
- use it after over-reserving for a conservative growth estimate
- use it after compaction, spill, free, or other state reductions that lower resident bytes
- do not use it to reduce bytes that are still attached to a live batch claim after handoff
- the requested shrink amount must not exceed the consumer's current admitted balance

For milestone 1, `shrink` is not a policy decision point. It should be treated as an infallible accounting update from `tiforth` to the host.

### 5. `release`

`release` returns all remaining admitted bytes for the consumer and retires the handle.

- use it on normal completion
- use it on cancellation or error teardown
- do not call it while any live batch claim still references bytes under that consumer
- treat it as the terminal form of `shrink`

Conceptually, `release` is `shrink(all_remaining_bytes)` plus consumer close.

## Batch-Lifetime Claims And Handoff

Admission governs bytes, not only builder activity. If admitted bytes survive into an emitted Arrow batch, the admitted balance must survive with them.

For milestone 1, `tiforth` should model this with claim-carrying batch ownership:

- after materialization, the producing stage `shrink`s any conservative over-reservation down to the exact retained live bytes that will remain reachable from the emitted batch
- those retained bytes become one or more live claims attached to the canonical batch envelope from `docs/contracts/data.md`
- handoff to the next stage transfers responsibility for those claims from the producing task's local scope to the live batch envelope; this is runtime-local bookkeeping, not a fresh `try_reserve`
- forwarded zero-copy columns keep their incoming claims; reusing an incoming array does not justify a new reserve request by itself
- newly materialized governed buffers add new claims under the producing stage's own consumers
- local scratch or build-time state that does not survive into the emitted batch should be `shrink`ed or `release`d before the batch is sealed for handoff

The host ABI does not need a fifth `transfer` operation for milestone 1. The required semantic rule is that admitted bytes remain live until the last batch or retained state referencing those bytes is gone.

## Failure Semantics

The ABI needs distinct behavior for host denial and local allocation failure.

### Host Denial

If `try_reserve` is denied:

- `tiforth` must not perform the governed allocation first and reconcile later
- the consumer's currently admitted balance stays unchanged
- the operator may run an explicit spill path if that consumer is spillable
- if spill succeeds and releases enough resident bytes, the operator may issue a fresh `try_reserve` request
- if there is no spill path, or a retry is still denied, the execution path fails with a memory-admission denial error
- a denied request must not emit a batch or hand off new live claims for that denied growth step

Denial is a normal control result, not undefined behavior and not an instruction to transparently block inside Arrow allocation internals.

### Internal Allocation Failure After Admission

If the host admits bytes but the subsequent internal allocation still fails:

- `tiforth` must return any admitted bytes that did not become live resident state before surfacing the failure
- if a partially built structure is discarded, its admitted bytes must also be released during teardown
- the surfaced error should remain distinguishable from host denial
- if the failed path already attached claims to an emitted batch, runtime teardown must release those claims before final query completion

This preserves the difference between policy rejection and a local allocator or implementation failure.

## Minimal Adapter-Visible Outcomes

The surrounding runtime contract in `docs/contracts/runtime.md` should preserve at least these distinctions for adapters and harnesses:

- `memory_admission_denied`
- `memory_allocation_failed`
- `ownership_contract_violation`

Concrete wire names may vary across Rust, C++, or Go embeddings so long as these meanings remain distinguishable.

## Interaction With Operator-Managed Spill

Spill remains an operator and runtime concern above Arrow internals.

- the host admission ABI does not initiate spill and does not manage spill files
- spillability is a consumer property declared when the consumer is opened
- on denial for a spillable consumer, the operator may spill resident state, call `shrink` or `release` for the memory it no longer keeps resident, and then retry admission
- on denial for an unspillable consumer, `tiforth` should fail that path immediately
- spill retries must be bounded by operator or runtime policy; the ABI does not imply infinite retry loops
- bytes stored on disk are outside the host memory-admission budget, but any resident spill metadata or read buffers remain governed memory and must still be admitted
- rehydrating spilled state later is a fresh reserve-before-allocate event

This keeps spill explicit, observable, and compatible with the reserve-first design identified in issue #2.

## Runtime Mapping And `broken-pipeline-rs` Compatibility

This ABI is designed to fit around the adopted `broken-pipeline-rs` runtime contract rather than redefine it.

Detailed milestone-1 spill and retry state mapping guidance lives in `docs/design/spill-retry-runtime-mapping.md`.

- admission calls happen inside operator or runtime-owned execution steps as resource checks around local growth
- a successful reserve leaves execution on the normal adopted runtime path
- a deny may trigger operator-local spill and retry, but it does not require a new shared runtime control state
- an unrecovered deny becomes ordinary runtime error propagation
- the ABI does not require scheduler callbacks, host-driven wakeups, or a parallel async admission protocol

That keeps the admission boundary compatible with direct runtime adoption and with `broken-pipeline-schedule` remaining harness-only.

## Cross-Language Assumptions

The milestone-1 ABI should remain easy to express across Rust, C++, and Go hosts.

- hot-path calls should be representable with opaque handles, fixed-width integers, enums, and plain-old-data structs
- the host does not need to pass `tiforth` retained pointers to host-owned mutable Arrow buffers just to enforce admission
- the ABI should be synchronous and callback-free on the hot path
- host functions should not call back into `tiforth` while processing an admission request
- no thread affinity is guaranteed for a consumer handle, but `tiforth` should serialize calls per handle

These rules intentionally avoid turning milestone 1 into a foreign-allocator or retained-foreign-buffer problem.

## Non-Goals

This design does not require or define:

- direct host-allocator routing for every ordinary Arrow growth path
- transparent spill inside Arrow allocators
- partial admission grants
- host-initiated revocation of outstanding reservations as part of the admission ABI
- a full production error-code registry for every adapter language binding

## Follow-Up Work

- map the semantic operations here onto the concrete dependency boundary work in issue #9
- define the exact metrics fields and snapshot payloads used for admit, deny, handoff, release, spill, and allocation failure paths
- map the claim-carrying handoff rules here onto the eventual kernel implementation hooks and batch-drop teardown paths
- keep Go-host embedding work aligned with the first host interop checkpoint in `docs/design/first-go-host-off-heap-interop-boundary.md`
- decide whether a later milestone needs direct host-allocator-backed buffers or imported immutable buffer bridges beyond this reserve-first, claim-carrying contract
