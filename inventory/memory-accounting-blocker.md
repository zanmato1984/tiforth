# Rust-First Memory Accounting / Allocator Routing / Spill Blocker Check

Status: issue #2 evidence memo

Verified: 2026-03-16

Related issues:

- #1 `design: choose tiforth shared-kernel language (C++ vs Rust)`
- #2 `investigate: rust-first memory accounting and spill blockers`

Related PR:

- #4 `docs: issue #2 evidence checkpoint for language decision`

## Question

Is a Rust-first `tiforth` kernel blocked by any of the following, once Rossi's sharper milestone-1 requirement is applied?

1. memory accounting
2. admission / reservation control
3. allocator routing for Arrow allocations
4. operator-managed spill

## Refined Conclusion

Result: **Rust-first remains conditionally viable, but milestone 1 now needs an explicit allocator-routing boundary for Arrow-backed operator output**.

Current evidence supports the following split result:

- memory accounting: viable in Rust today
- admission / reservation control: viable in Rust today
- allocator routing for ordinary Arrow growth paths: **not** provided by stock `arrow-rs`
- operator-managed spill: viable in Rust today and should stay above Arrow internals
- allocator origin matters: Rust and C++ fit this boundary more directly than Go-heap-backed memory

This changes the meaning of the earlier checkpoint.

The old phrase, **conditionally viable with a narrow lower-level boundary**, was directionally right but too vague for milestone 1. The required boundary is not just a generic memory governor. It must explicitly solve **host allocator routing into operator-level Arrow build paths**, while distinguishing between:

- `tiforth`-owned mutable builders that can grow under host allocator control
- imported immutable buffers that can be adopted after allocation is complete
- stock Arrow mutable growth paths that still allocate through ordinary Rust allocation machinery

That means:

- if milestone 1 assumes stock Arrow builders or `MutableBuffer` growth will automatically honor a host allocator, current Rust Arrow is insufficient
- if milestone 1 accepts an explicit allocator-aware builder / imported-buffer boundary, Rust-first remains viable
- this is a blocker for a particular milestone shape, not proof that the kernel must become C++

## Milestone-1 Requirement From Rossi

New direction received on 2026-03-16:

- the host memory allocator must be wired through to `tiforth` operators
- the routed host allocator may originate in Go, C++, or Rust
- spill is explicitly managed by operators
- transparent spill inside Arrow allocation paths should not be assumed or required

This is important because it separates four concerns that were partially blurred together in the earlier checkpoint:

- correct accounting is not the same as admission control
- admission control is not the same as allocator routing
- allocator routing is not the same as spill
- operator-managed spill reduces pressure on Arrow internals, but does not automatically solve allocator routing

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

### 2. Admission / Reservation Control

Current upstream `datafusion` source reports workspace version `52.3.0`.

Observed facts:

- DataFusion has its own runtime-level `execution::memory_pool::MemoryPool`, separate from Arrow Rust's pool type
- that API exposes `grow`, `try_grow`, `shrink`, named `MemoryConsumer`s, and spillable versus unspillable reservations
- `FairSpillPool`, `GreedyMemoryPool`, and `TrackConsumersPool` show current Rust patterns for admission control and observability above Arrow internals
- `common::utils::proxy` provides accounting helpers for non-Arrow containers such as `Vec`- and hash-table-related growth

What this means:

- reservation and admission control can live in a Rust runtime layer above Arrow
- this should be a `tiforth`-owned boundary, not an assumption that Arrow itself will reject growth pre-allocation
- this part of the problem is also **not** what forces a C++ kernel

### 3. Allocator Routing For Arrow Allocations

This is the sharp remaining edge.

Observed facts from current `arrow-rs`:

- `MutableBuffer::with_capacity` and `MutableBuffer::from_len_zeroed` allocate through `std::alloc`
- `MutableBuffer::reserve` grows by calling `reallocate`, which uses `std::alloc::alloc`, `std::alloc::realloc`, and `std::alloc::dealloc`
- `BufferBuilder<T>` wraps `MutableBuffer`, so normal buffer-builder growth follows that same allocation path
- representative higher-level builders are not centrally allocator-routed either; for example, `PrimitiveBuilder::with_capacity` uses `Vec::with_capacity` for values storage before Arrow materialization
- `Buffer::from_custom_allocation` can adopt externally allocated memory behind an ownership object
- `Bytes::try_realloc` only works for `Deallocation::Standard`
- `MutableBuffer::from_bytes` rejects non-standard deallocation, so custom-owned buffers do not re-enter the normal mutable growth path

What this means:

- externally allocated or custom-owned buffers are enough for **imported** or **finalized immutable** Arrow buffers
- they are **not** enough to make stock mutable Arrow growth paths honor a host allocator
- ordinary operator-level Arrow construction still escapes host allocator control unless `tiforth` either avoids those paths or changes them

### 3a. Viable `tiforth`-Owned Boundary Shapes

Current evidence supports two viable shapes that stay compatible with a Rust-first kernel.

#### Allocator-Aware `tiforth` Builders

`tiforth` can own its mutable build path instead of delegating ordinary growth to stock Arrow builders.

Shape:

- values, offsets, and validity buffers are allocated and reallocated through a routed host allocator handle
- the builder performs its own fallible reservation checks before calling that allocator
- once the operator output is finalized, the resulting memory is wrapped into immutable Arrow buffers through `Buffer::from_custom_allocation` or equivalent finalized ownership transfer

What this solves:

- milestone-1 host allocator routing for operator-managed growth
- exact control over allocate / reallocate / free boundaries
- clean separation between runtime reservation policy and Arrow array materialization

What it does **not** solve by itself:

- reuse of stock `MutableBuffer`, `BufferBuilder`, or higher-level builders such as `PrimitiveBuilder`

#### Imported-Buffer Bridge

`tiforth` can also accept memory that was allocated or finalized outside normal Arrow Rust growth paths.

Shape:

- a host-side or lower-level component allocates and possibly fills the final buffer
- `tiforth` imports the buffer as immutable Arrow storage with an ownership object that knows how to free it
- this can be used for host-built batches or for a `tiforth` custom builder that finalizes before Arrow wrapping

What this solves:

- adoption of externally allocated immutable buffers
- zero-copy handoff when the final memory layout is already Arrow-compatible

What it does **not** solve:

- incremental mutable growth through stock Arrow Rust buffer builders

### 3b. Host Allocator Origin Cases

The allocator-origin question now matters as much as the Arrow API question.

#### Rust-Origin Allocator

This is the cleanest case.

- a Rust-first `tiforth` kernel can hold the allocator handle natively
- allocator-aware `tiforth` builders are fully viable here
- an imported-buffer bridge is also viable for finalized immutable buffers

Constraint:

- stock Arrow Rust mutable builders still will not route through that allocator unless `tiforth` replaces or patches those growth paths

#### C++-Origin Allocator

This is viable through an explicit FFI boundary.

- the C++ side can expose a stable C ABI or thin shim around a host allocator such as Arrow C++ `MemoryPool`
- allocator-aware `tiforth` builders can call `allocate` / `reallocate` / `free` through that shim
- imported immutable buffers are viable if ownership is returned to the C++ side through a drop handle or owner object

Constraint:

- ordinary `arrow-rs` mutable growth still will not route through that C++ allocator without replacing or patching the Arrow Rust growth path

#### Go-Origin Allocator

This is the hardest case and needs the most careful boundary definition.

Observed fact from official `cgo` guidance:

- C code may not keep Go pointers after the call returns unless the pointed-to memory is pinned, and common Go container types such as slices and strings are not a safe general long-lived Arrow buffer boundary

What this means:

- a Go-heap-backed Arrow buffer ownership model is **not** a good general milestone-1 assumption
- a Go-controlled host allocator can still be viable if it routes allocations into non-Go memory exposed through a C ABI, or into carefully pinned/exported memory with strict lifetime rules
- imported-buffer bridging from Go is viable only when the underlying memory is safe to retain from Rust after the call boundary

Constraint:

- stock Arrow Rust mutable growth still does not become Go-allocator-routed automatically

### 3c. What Requires Patching Or Replacing Ordinary Arrow Growth Paths

Current evidence now supports a sharper distinction.

Viable without patching stock Arrow:

- `tiforth`-owned allocator-aware builders that materialize Arrow buffers only after growth is complete
- imported immutable buffers whose ownership is represented by `Buffer::from_custom_allocation`
- runtime reservation / spill policy above Arrow internals

Not viable without patching or replacing ordinary Arrow growth paths:

- requiring `MutableBuffer`, `BufferBuilder`, `PrimitiveBuilder`, or similar stock Arrow Rust builders to route all allocate / reallocate / free operations through a host allocator
- assuming imported custom allocations can flow back into `MutableBuffer::from_bytes` and then keep growing through ordinary Arrow paths
- claiming DataFusion's Arrow-facing accounting bridge is already a full allocator-routing solution

So the stronger milestone-1 requirement does change the blocker assessment:

- stock Rust Arrow is insufficient if the requirement is: **use ordinary Arrow mutable/build paths and still have every relevant allocation routed through the host allocator**
- Rust-first remains viable if `tiforth` accepts one of these explicit designs:
  - allocator-aware `tiforth` builders that allocate via the host allocator and hand finished buffers into Arrow
  - a local or upstream Arrow patch that threads allocator routing through `MutableBuffer`-based growth paths
  - a similarly explicit imported-buffer bridge that keeps host allocation outside stock Arrow growth

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

DataFusion does **not** remove the milestone-1 allocator-routing problem.

Observed facts:

- its `ArrowMemoryPool` adapter implements Arrow's `arrow_buffer::MemoryPool`
- Arrow's `MemoryPool::reserve` is infallible
- the adapter therefore creates a DataFusion reservation and calls `grow`, not `try_grow`
- the adapter is useful for Arrow memory participation in DataFusion accounting, but it still sits on Arrow's tracking interface rather than replacing Arrow's normal allocation path

What this means:

- DataFusion is strong evidence for how to separate accounting, reservations, and spill
- it is **not** evidence that stock Arrow Rust already routes ordinary builder growth through a host allocator
- it is therefore a reference for the upper layer, not a complete answer for allocator routing

## What This Means For `tiforth`

`tiforth` should now treat the four concerns separately.

### Memory Accounting

Keep in Rust with:

- ownership-aware `claim` support for Arrow arrays and buffers
- a batch-memory helper that deduplicates shared buffers
- non-Arrow accounting helpers for operator scratch state

### Admission / Reservation Control

Keep in Rust with:

- a runtime-owned memory governor
- fallible reservations for large mutable state
- spillable versus unspillable consumer tracking

### Allocator Routing

Make this an explicit milestone-1 design choice:

- do **not** assume stock Arrow mutable paths already solve it
- define when operators use `tiforth` allocator-aware builders versus an imported-buffer bridge
- treat imported immutable buffers and mutable builder growth as different cases
- accept that keeping stock Arrow mutable growth in the path will require Arrow patching or local replacement of those builders
- make the cross-language allocator ABI explicit when the host allocator originates outside Rust

### Spill

Keep it operator-managed:

- spill should react to runtime policy and operator state shape
- spill does not need to be transparent inside Arrow allocators
- allocator routing and spill should cooperate, but not be conflated

## Blocker Assessment After Refinement

- memory accounting: **not a Rust blocker**
- admission / reservation control: **not a Rust blocker**
- operator-managed spill: **not a Rust blocker**
- allocator routing for ordinary Arrow growth paths: **the real remaining milestone-1 blocker edge**
- Go-origin host allocation is only conditionally viable if the routed memory is not an arbitrary retained Go heap pointer

This is the refined issue #2 result:

- Rust-first is still viable
- the remaining sharp edge is narrower than "Rust cannot do memory governance"
- but it is stronger than the earlier memo implied if milestone 1 truly requires host allocator routing inside operator-level Arrow build paths
- the viable non-patching path is now clearer: `tiforth`-owned builders and imported immutable buffers, with extra caution if the allocator originates in Go

## Decision Impact

- issue #2 should now be recorded as: **Rust-first is conditionally viable, but milestone 1 requires an explicit Arrow allocator-routing boundary**
- this does **not** justify an automatic retreat to a C++ kernel
- it does mean the next design work must define the milestone-1 Arrow construction path, not just speak abstractly about memory pools
- that design work should explicitly say which path is used for Rust-, C++-, and Go-origin allocator control
- Arrow C++ still retains a real advantage on allocator-routing surface area because its `MemoryPool` participates directly in allocation, reallocation, and free paths

## Notes On Method

- this was treated as a research / evidence task, not an implementation task
- the only external write in this continuation was the required issue #2 progress comment on 2026-03-16
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
