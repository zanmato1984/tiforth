# Rust-First Memory Accounting / Host Admission / Spill Blocker Check

Status: issue #2 evidence memo

Verified: 2026-03-16

Related issues:

- #1 `design: choose tiforth shared-kernel language (C++ vs Rust)`
- #2 `investigate: rust-first memory accounting and spill blockers`

Related PR:

- #4 `docs: issue #2 evidence checkpoint for language decision`

## Question

Is a Rust-first `tiforth` kernel blocked by any of the following, once Rossi's revised milestone-1 requirement is applied?

1. memory accounting
2. host reservation / admission control before allocation
3. internal allocation after admission
4. operator-managed spill

## Refined Conclusion

Result: **Rust-first is not blocked for milestone 1. The required contract is reserve with host first, fail on deny, then allocate internally.**

Current evidence supports the following split result:

- memory accounting: viable in Rust today
- host reservation / admission control: viable in Rust today
- internal Arrow-backed allocation after admission: viable in Rust today with `tiforth`'s own allocator or ordinary Rust allocation paths
- operator-managed spill: viable in Rust today and should stay above Arrow internals
- direct host allocator routing for ordinary Arrow growth paths is still absent in stock `arrow-rs`, but it is no longer the milestone-1 requirement

This changes the meaning of the earlier checkpoint.

The old phrase, **conditionally viable with a narrow lower-level boundary**, over-weighted allocator routing for milestone 1. Rossi's updated instruction narrows the actual day-one contract to a host admission boundary:

- `tiforth` reports intended memory before allocating
- the host either admits or denies that reservation request
- denial is a hard stop for the pending allocation / execution path
- once admitted, `tiforth` may allocate internally and continue normally

That means:

- if milestone 1 required stock Arrow builders to allocate directly through a host allocator, current Rust Arrow would still be insufficient
- that is no longer the milestone-1 requirement
- the remaining work is to define the reserve-first admission ABI and accounting discipline, not to force the kernel into C++

## Milestone-1 Requirement From Rossi

New direction received on 2026-03-16:

- milestone 1 no longer requires `tiforth` internal Arrow allocations to directly use the host allocator
- `tiforth` may use its own allocator internally
- before allocating, `tiforth` must report or reserve the intended memory with the host
- if the host denies the reservation or admission request, `tiforth` must fail and must not proceed with the allocation or execution path
- spill remains explicitly operator-managed

This is important because it cleanly separates four concerns that were partially blurred together in the earlier checkpoint:

- correct accounting is not the same as admission control
- admission control is not the same as direct allocator routing
- internal allocation after admission is not the same as host-owned allocation
- operator-managed spill reduces pressure on Arrow internals, but does not need to be transparent inside Arrow allocation paths

## Category-By-Category Assessment

### 1. Memory Accounting

Current upstream `arrow-rs` source reports workspace version `58.0.0`.

Observed facts:

- `arrow-buffer` has an optional `pool` feature with `MemoryPool`, `MemoryReservation`, and `TrackingMemoryPool`
- `MutableBuffer`, internal `Bytes`, and `Array` expose `claim`-style APIs for registering memory with a pool
- `Array::claim` is recursive and documents exact-once accounting for shared backing buffers, including slices and nested children
- `RecordBatch::get_array_memory_size()` still documents possible overestimation when columns share buffers or slices

What this means:

- Rust Arrow is now good enough for ownership-aware Arrow memory accounting
- exact batch accounting still needs a helper that deduplicates shared buffers
- this part of the problem is **not** the remaining language blocker candidate

### 2. Host Reservation / Admission Control

Current upstream `datafusion` source reports workspace version `52.3.0`.

Observed facts:

- DataFusion has its own runtime-level `execution::memory_pool::MemoryPool`, separate from Arrow Rust's pool type
- that API exposes `grow`, `try_grow`, `shrink`, named `MemoryConsumer`s, and spillable versus unspillable reservations
- `FairSpillPool`, `GreedyMemoryPool`, and `TrackConsumersPool` show current Rust patterns for admission control and observability above Arrow internals
- `common::utils::proxy` provides accounting helpers for non-Arrow containers such as `Vec`- and hash-table-related growth

What this means:

- reservation and admission control can live in a Rust runtime layer above Arrow
- this is the actual milestone-1 boundary: ask the host first, then allocate only after approval
- host denial should be treated as a fail-fast pre-allocation control point rather than an allocate-first reconciliation step
- this part of the problem is **not** what forces a C++ kernel

### 3. Internal Allocation After Admission

Observed facts from current `arrow-rs`:

- `MutableBuffer::with_capacity` and `MutableBuffer::from_len_zeroed` allocate through `std::alloc`
- `MutableBuffer::reserve` grows by calling `reallocate`, which uses `std::alloc::alloc`, `std::alloc::realloc`, and `std::alloc::dealloc`
- `BufferBuilder<T>` wraps `MutableBuffer`, so normal buffer-builder growth follows that same allocation path
- representative higher-level builders are not centrally allocator-routed either; for example, `PrimitiveBuilder::with_capacity` uses `Vec::with_capacity` for values storage before Arrow materialization
- `Buffer::from_custom_allocation` can still adopt externally allocated memory behind an ownership object when future milestones need that shape

What this means:

- ordinary internal Arrow construction in Rust remains viable once host admission succeeds
- milestone 1 no longer depends on stock Arrow builders calling into a host allocator
- `tiforth` may allocate through its own internal allocator after successful host admission
- direct host allocator routing is now a future design option rather than the milestone-1 blocker

### 3a. Direct Host Allocator Routing Is Deferred, Not Proven

Observed facts:

- `Bytes::try_realloc` only works for `Deallocation::Standard`
- `MutableBuffer::from_bytes` rejects non-standard deallocation, so custom-owned buffers do not re-enter the normal mutable growth path
- `Buffer::from_custom_allocation` mainly helps with imported or finalized immutable buffers

What this means:

- direct host allocator routing for ordinary Arrow growth is still not something stock `arrow-rs` provides today
- allocator-aware `tiforth` builders, imported-buffer bridges, or Arrow patches remain viable future options if a later milestone needs them
- those options no longer decide milestone-1 viability or the Rust-versus-C++ language question

### 3b. Host-Origin Impact After The Reframe

The cross-language sharp edge is smaller after Rossi's updated requirement.

- a Rust-, C++-, or Go-origin host can expose a reservation or admission ABI without giving `tiforth` long-lived ownership of host-managed memory
- the boundary now mainly needs request metadata, attribution, and admit-or-deny semantics rather than allocator callbacks on every Arrow growth path
- official Go `cgo` pointer rules still matter for future retained host-owned buffers, but they matter much less for an admission-only milestone-1 contract

### 4. Operator-Managed Spill

Observed facts:

- DataFusion models spillability as a property of the consumer, not as a transparent property of Arrow allocation itself
- `FairSpillPool` and related reservation logic show how spillable and unspillable consumers can coexist under one runtime policy
- `execution::disk_manager::DiskManager` shows a current Rust pattern for explicit spill-file management, disabled mode, and size tracking

What this means:

- operator-managed spill is compatible with a Rust-first kernel
- it should be expressed in the runtime and operator contract, not delegated to transparent Arrow-internal spill behavior
- the lack of transparent spill inside Arrow allocation paths is therefore **not** the blocker here

## DataFusion As Evidence, Not Authority

### Where DataFusion Is Useful

DataFusion is a useful current reference for:

- runtime-level reservations and named consumers
- spillable versus unspillable memory consumers
- disk spill management above Arrow internals
- accounting helpers for non-Arrow structures
- deduplicated batch sizing helpers
- an Arrow-facing claim bridge (`execution::memory_pool::arrow::ArrowMemoryPool`)

### Where DataFusion Is Not Sufficient

DataFusion does **not** automatically define the exact milestone-1 `tiforth` contract.

Observed facts:

- its `ArrowMemoryPool` adapter implements Arrow's `arrow_buffer::MemoryPool`
- Arrow's `MemoryPool::reserve` is infallible
- the adapter therefore creates a DataFusion reservation and calls `grow`, not `try_grow`
- the adapter is useful for Arrow memory participation in DataFusion accounting, but it still sits on Arrow's tracking interface rather than replacing Arrow's normal allocation path

What this means:

- DataFusion is strong evidence for how to separate accounting, reservations, and spill
- it is still **not** evidence that stock Arrow Rust already routes ordinary builder growth through a host allocator
- that gap simply no longer controls milestone-1 viability, because milestone 1 is now reserve-first rather than allocator-routed

## What This Means For `tiforth`

`tiforth` should now treat the four concerns separately.

### Memory Accounting

Keep in Rust with:

- ownership-aware `claim` support for Arrow arrays and buffers
- a batch-memory helper that deduplicates shared buffers
- non-Arrow accounting helpers for operator scratch state

### Host Reservation / Admission Control

Keep in Rust with:

- a runtime-owned memory governor
- an explicit host reservation or admission request before internal allocation
- fail-on-deny semantics for the pending allocation / execution path
- spillable versus unspillable consumer tracking

### Internal Allocation

Allow in Rust after successful host admission:

- `tiforth` may allocate through its own internal allocator or ordinary Rust / Arrow growth paths
- direct host allocator routing is optional future work, not the day-one contract
- future imported-buffer or custom-builder work should be tracked separately if a later milestone needs it

### Spill

Keep it operator-managed:

- spill should react to runtime policy and operator state shape
- spill does not need to be transparent inside Arrow allocators
- admission control and spill should cooperate, but not be conflated

## Blocker Assessment After Refinement

- memory accounting: **not a Rust blocker**
- host reservation / admission control: **not a Rust blocker**
- internal allocation after admitted reservation: **not a Rust blocker**
- operator-managed spill: **not a Rust blocker**
- direct host allocator routing for ordinary Arrow growth paths: **not a milestone-1 blocker under Rossi's revised requirement**
- Go-origin host interaction: an admission-only ABI is viable; the pointer-lifetime sharp edge moves to later imported-buffer work

This is the refined issue #2 result:

- Rust-first is not blocked for milestone 1
- the required contract is reserve with host first, fail on deny, then allocate internally
- the earlier allocator-routing sharp edge is now future optional work, not the deciding blocker
- this does not justify an automatic retreat to a C++ kernel

## Decision Impact

- issue #2 should now be recorded as: **Rust-first is not blocked for milestone 1; the required contract is reserve with host first, fail on deny, then allocate internally**
- this strengthens the Rust-first conclusion relative to the earlier memo
- the next design work should define the admission ABI, intended-memory estimation rules, and release or shrink semantics
- if a later milestone reintroduces mandatory direct host allocator routing, that should be reviewed as a separate design question rather than carried as a hidden day-one assumption

## Notes On Method

- this was treated as a research / evidence task, not an implementation task
- the only external writes in this continuation were the required issue #2 progress comment and the branch update for PR #4
- no local `tiforth-legacy` checkout was present in this workspace, so donor code was not used for this memo
- DataFusion was used as evidence and reference, not as an authority

## Sources

- `tiforth` issue #1: <https://github.com/zanmato1984/tiforth/issues/1>
- `tiforth` issue #2: <https://github.com/zanmato1984/tiforth/issues/2>
- `tiforth` PR #4: <https://github.com/zanmato1984/tiforth/pull/4>
- `arrow-rs` workspace manifest: <https://github.com/apache/arrow-rs/blob/main/Cargo.toml>
- `arrow-rs` pool support: <https://github.com/apache/arrow-rs/blob/main/arrow-buffer/src/pool.rs>
- `arrow-rs` mutable buffer allocation and claim paths: <https://github.com/apache/arrow-rs/blob/main/arrow-buffer/src/buffer/mutable.rs>
- `arrow-rs` immutable buffer ownership bridge: <https://github.com/apache/arrow-rs/blob/main/arrow-buffer/src/buffer/immutable.rs>
- `arrow-rs` allocation ownership model: <https://github.com/apache/arrow-rs/blob/main/arrow-buffer/src/alloc/mod.rs>
- `arrow-rs` shared-bytes ownership and reallocation limits: <https://github.com/apache/arrow-rs/blob/main/arrow-buffer/src/bytes.rs>
- `arrow-rs` buffer builder paths: <https://github.com/apache/arrow-rs/blob/main/arrow-buffer/src/builder/mod.rs>
- `arrow-rs` primitive builder path: <https://github.com/apache/arrow-rs/blob/main/arrow-array/src/builder/primitive_builder.rs>
- `arrow-rs` array memory and `claim` API: <https://github.com/apache/arrow-rs/blob/main/arrow-array/src/array/mod.rs>
- `arrow-rs` record-batch memory sizing note: <https://github.com/apache/arrow-rs/blob/main/arrow-array/src/record_batch.rs>
- Apache Arrow C++ memory pool header: <https://github.com/apache/arrow/blob/main/cpp/src/arrow/memory_pool.h>
- `datafusion` workspace manifest: <https://github.com/apache/datafusion/blob/main/Cargo.toml>
- `datafusion` runtime memory pool API: <https://github.com/apache/datafusion/blob/main/datafusion/execution/src/memory_pool/mod.rs>
- `datafusion` Arrow memory adapter: <https://github.com/apache/datafusion/blob/main/datafusion/execution/src/memory_pool/arrow.rs>
- `datafusion` spill-pool implementations: <https://github.com/apache/datafusion/blob/main/datafusion/execution/src/memory_pool/pool.rs>
- `datafusion` disk spill manager: <https://github.com/apache/datafusion/blob/main/datafusion/execution/src/disk_manager.rs>
- `datafusion` allocation proxy helpers: <https://github.com/apache/datafusion/blob/main/datafusion/common/src/utils/proxy.rs>
- `datafusion` record-batch memory helper: <https://github.com/apache/datafusion/blob/main/datafusion/common/src/utils/memory.rs>
- Go `cgo` pointer-passing rules: <https://go.dev/cmd/cgo/#hdr-Passing_pointers>
