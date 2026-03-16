# Rust-First Memory Accounting / Spill Blocker Check

Status: issue #2 evidence memo

Verified: 2026-03-15

Related issues:

- #1 `design: choose tiforth shared-kernel language (C++ vs Rust)`
- #2 `investigate: rust-first memory accounting and spill blockers`

## Question

Is a Rust-first `tiforth` kernel blocked by Arrow-related memory accounting, allocator / memory-pool control, and spill support?

## Conclusion

Result: **conditionally viable with a narrow lower-level boundary**.

Rust-first is not blocked in the strong sense of "the required behavior cannot be implemented in Rust." Current Rust Arrow and adjacent Apache Rust runtime work are enough to support:

- ownership-aware accounting for Arrow buffers and arrays
- query / stage / operator-level memory reservation above Arrow internals
- spill-aware runtime policy and disk-backed operator state

The remaining gap is narrower and more specific:

- Rust Arrow does **not** currently provide a C++-style allocator-driven memory-pool model for ordinary Arrow allocations
- its pool support is primarily **accounting after allocation**, not a fallible admission-control API

That means `tiforth` should not assume Arrow Rust will be the entire memory-governor story. A small explicit lower layer is still needed for reservation policy, batch accounting helpers, and optional host-managed allocations.

## Minimum Requirements That Matter For The Language Decision

These are the minimum requirements that matter for choosing Rust versus C++. They are intentionally narrower than a full production memory subsystem.

### Day-One Requirements

- query / stage / operator attribution for both Arrow and non-Arrow memory
- a reservation path for large mutable state so runtime policy can reject growth, backpressure, or spill instead of allocating blindly forever
- ownership-aware accounting for shared Arrow buffers across slices, nested arrays, and stage handoffs
- release-on-drop or equivalent lifetime-coupled accounting so memory naturally unwinds with pipeline progress
- a way to distinguish spillable from unspillable consumers
- a spill path for operator state that can move data to disk and reduce in-memory pressure

### Important But Not Day-One Language Blockers

- custom allocator injection for every Arrow allocation site
- perfect host-engine memory-pool unification before the first kernel exists
- transparent spill inside Arrow itself rather than in the runtime / operator layer
- a fully general off-heap policy for every future adapter from the first milestone

## Current Upstream Observations

### 1. Arrow Rust Has Real Accounting Hooks Now

Current upstream `arrow-rs` source reports workspace version `58.0.0`.

Relevant observations:

- `arrow-buffer` has an optional `pool` feature with `MemoryPool`, `MemoryReservation`, and `TrackingMemoryPool`
- `MutableBuffer`, internal `Bytes`, and `Array` all expose `claim`-style APIs for registering memory with a pool
- `Array::claim` is recursive and documents exact-once accounting for shared backing buffers, including slices and nested children
- `Buffer::from_custom_allocation` can adopt externally allocated memory behind an ownership object

What this is good enough for:

- exact ownership-aware accounting of Arrow-backed data
- imported or externally owned buffers
- preventing double counting when multiple arrays share one backing allocation

### 2. Arrow Rust Pool Support Is Not The Same Thing As Allocator Control

The current Arrow Rust pool layer is meaningfully weaker than Arrow C++'s memory-pool API if the requirement is allocator control.

Observed limits:

- `MutableBuffer::with_capacity` and related constructors allocate through `std::alloc`
- standard resize / reallocation paths also use the standard allocator path
- `arrow_buffer::MemoryPool::reserve` is infallible and does not act as a quota gate
- the pool is therefore an accounting object, not a general-purpose allocation policy object

This is the most important distinction from the blocker investigation:

- **Arrow Rust pool support helps with correct accounting**
- **it does not by itself solve pre-allocation admission control or host-pool enforcement**

### 3. Arrow Rust Still Needs External Batch-Level Size Hygiene

The current `RecordBatch::get_array_memory_size()` docs in `arrow-rs` still say the method may overestimate when multiple columns share buffers or slices.

That means `tiforth` should not treat the summed `RecordBatch` size helper as a precise accounting primitive.

This is not a kernel blocker, but it is a design note:

- exact batch accounting still belongs in a small helper or boundary layer that deduplicates shared buffers

### 4. Arrow C++ Still Has The Stronger Native Memory-Pool Surface

Current Apache Arrow C++ still exposes a direct `MemoryPool` abstraction with:

- `Allocate`, `Reallocate`, and `Free`
- explicit byte-tracking statistics
- proxy / logging pool wrappers
- capped-pool support

This remains a real advantage for C++ if `tiforth` eventually needs direct participation in a host-style allocator / memory-pool contract rather than a runtime-managed reservation layer.

### 5. The Rust Ecosystem Already Shows The Missing Upper Layer

Current upstream `datafusion` source reports workspace version `52.3.0`.

DataFusion is important here because it demonstrates the current Rust pattern for memory governance **above** Arrow internals.

Relevant observations:

- DataFusion has its own `execution::memory_pool::MemoryPool` abstraction, separate from Arrow Rust's pool type
- that runtime-level pool supports `try_grow`, `grow`, `shrink`, named `MemoryConsumer`s, and spillable versus unspillable reservations
- `FairSpillPool`, `GreedyMemoryPool`, and `TrackConsumersPool` show concrete admission-control and observability patterns
- `execution::disk_manager::DiskManager` provides spill-file creation, disk-usage tracking, disabled mode, and size limits
- `common::utils::proxy` includes accounting helpers for `Vec` and hash-table growth outside Arrow buffers
- `common::utils::memory::get_record_batch_memory_size` explicitly deduplicates shared Arrow buffers for batch accounting

This matters because it shows the missing pieces do **not** force a C++ kernel. They can live in a Rust runtime layer above Arrow.

## What This Means For `tiforth`

### What `tiforth` Can Keep In Rust

`tiforth` can remain Rust-first if it keeps the following responsibilities out of Arrow internals and in its own runtime / kernel support layers:

- query / stage / operator memory reservations
- spillability decisions and spill-trigger policy
- accounting for non-Arrow structures such as vectors, hash tables, and operator scratch state
- batch-level shared-buffer deduplication where precise sizing is required
- handoff-time claiming of Arrow arrays or imported buffers

### The Narrow Lower-Level Boundary That Still Seems Necessary

The evidence supports a narrow boundary, not a C++-first retreat.

The boundary should likely contain:

1. a runtime-owned memory governor with fallible reservations and observability
2. a small Arrow accounting helper that claims arrays / buffers and deduplicates shared batch memory
3. an optional imported-buffer bridge for host-managed or off-heap allocations

That boundary can stay primarily Rust. Nothing in this research requires the shared kernel itself to become C++.

## What Would Turn This Into A Real Blocker

The current result would change if `tiforth` later decides that any of the following are mandatory from day one:

- every Arrow allocation must be admitted or denied by a host memory pool **before** the allocation occurs, with no wrapper-controlled build path
- spill must happen transparently inside Arrow allocation machinery instead of at runtime / operator boundaries
- the first kernel milestone must already share one engine-native allocator / memory-pool ABI across TiDB, TiKV, and TiFlash

Those would be materially stronger requirements than the current repo contracts say today.

## Decision Impact

- issue #2 should currently be recorded as: **conditionally viable with a narrow lower-level boundary**
- the blocker candidate is real, but narrower than the original worry suggested
- this weakens the case for abandoning a Rust-first kernel on memory-accounting grounds alone
- the next design work should make the memory-governor boundary explicit instead of assuming Arrow will provide all allocator semantics internally

## Notes On Method

- this was treated as a research / evidence task, not an implementation task
- no external writes were performed
- no local `tiforth-legacy` checkout was present in this workspace, so donor code was not used for this memo

## Sources

- `tiforth` issue #1: <https://github.com/zanmato1984/tiforth/issues/1>
- `tiforth` issue #2: <https://github.com/zanmato1984/tiforth/issues/2>
- `arrow-rs` workspace manifest: <https://github.com/apache/arrow-rs/blob/main/Cargo.toml>
- `arrow-rs` buffer pool support: <https://github.com/apache/arrow-rs/blob/main/arrow-buffer/src/pool.rs>
- `arrow-rs` mutable buffer allocation and `claim`: <https://github.com/apache/arrow-rs/blob/main/arrow-buffer/src/buffer/mutable.rs>
- `arrow-rs` allocation ownership model: <https://github.com/apache/arrow-rs/blob/main/arrow-buffer/src/alloc/mod.rs>
- `arrow-rs` array memory and `claim` API: <https://github.com/apache/arrow-rs/blob/main/arrow-array/src/array/mod.rs>
- `arrow-rs` record-batch memory sizing note: <https://github.com/apache/arrow-rs/blob/main/arrow-array/src/record_batch.rs>
- Apache Arrow C++ memory pool header: <https://github.com/apache/arrow/blob/main/cpp/src/arrow/memory_pool.h>
- `datafusion` workspace manifest: <https://github.com/apache/datafusion/blob/main/Cargo.toml>
- `datafusion` runtime memory pool API: <https://github.com/apache/datafusion/blob/main/datafusion/execution/src/memory_pool/mod.rs>
- `datafusion` spill-pool implementations: <https://github.com/apache/datafusion/blob/main/datafusion/execution/src/memory_pool/pool.rs>
- `datafusion` disk spill manager: <https://github.com/apache/datafusion/blob/main/datafusion/execution/src/disk_manager.rs>
- `datafusion` allocation proxy helpers: <https://github.com/apache/datafusion/blob/main/datafusion/common/src/utils/proxy.rs>
- `datafusion` record-batch memory helper: <https://github.com/apache/datafusion/blob/main/datafusion/common/src/utils/memory.rs>
