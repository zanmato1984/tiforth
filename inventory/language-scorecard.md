# Language Scorecard

This file is the comparison rubric for **C++** versus **Rust**.

The current decision posture is **Rust by default unless blocked**.

That means the scorecard is not trying to erase preference; it is trying to test whether the preference survives contact with real constraints.

## Suggested Scoring Scale

- 1 = poor fit
- 2 = weak fit
- 3 = mixed / uncertain
- 4 = strong fit
- 5 = very strong fit

Scores are optional if narrative comparison is clearer, but both languages should be evaluated against the same dimensions.

## Mandatory Questions Before Any Final Recommendation

1. Can a Rust-first kernel satisfy tiforth's required memory-accounting and spill model?
2. If not directly, can that gap be isolated behind a narrow lower-level boundary without giving up a Rust-developed kernel?
3. Can broken-pipeline ideas be adapted cleanly enough for a Rust-first runtime path?
4. Are the remaining integration costs genuinely blockers, or just engineering work?

If these questions are not answered, the decision is not ready.

## Current Checkpoint From Issue #2

- result: Rust-first is **conditionally viable, but milestone 1 requires an explicit Arrow allocator-routing boundary**
- question 1: provisionally yes for accounting, reservations, and operator-managed spill; conditionally yes for allocator routing only if `tiforth` uses allocator-aware builders for mutable growth, an imported-buffer bridge for finalized immutable buffers, or changes Arrow's mutable growth path
- question 2: provisionally yes, but the required boundary is sharper than before and now explicitly includes allocator routing rather than only accounting and spill policy; Go-origin allocator control is the hardest version of that boundary
- detailed note: `inventory/memory-accounting-blocker.md`

## Dimensions

### TiFlash Donor Leverage

- weight: TBD
- C++: TBD
- Rust: TBD
- notes: TBD

### TiKV Integration Fit

- weight: TBD
- C++: TBD
- Rust: TBD
- notes: TBD

### TiDB Integration Fit

- weight: TBD
- C++: TBD
- Rust: TBD
- notes: TBD

### Arrow Data-Contract Fit

- weight: high
- C++: 5
- Rust: 4
- notes: C++ Arrow still has the stronger native allocator / memory-pool surface. Rust Arrow `58.0.0` now has claim-based exact accounting hooks and `Buffer::from_custom_allocation`, but normal `MutableBuffer` / builder growth still uses standard Rust allocation paths and some higher-level builders allocate through `Vec::with_capacity`. Rust remains viable only if milestone 1 makes allocator routing explicit: `tiforth` must either own mutable build paths itself, import finalized immutable buffers, or patch/replace Arrow growth.

### Runtime / Concurrency Fit

- weight: high
- C++: 3
- Rust: 5
- notes: Issue #2 still strengthens Rust here. Current DataFusion `52.3.0` shows Rust-side memory reservations, spillable consumers, fair spill pools, disk spill management, non-Arrow accounting helpers, and an Arrow claim bridge layered above Arrow. That lines up with tiforth's staged, backpressured runtime direction. The remaining sharp edge is allocator routing for stock Arrow build paths, not the runtime or spill model itself.

### FFI / Boundary Complexity

- weight: high
- C++: 4
- Rust: 3
- notes: C++ avoids some allocator-boundary work if tiforth needs direct host memory-pool participation in ordinary Arrow allocations. Rust likely pays for a runtime memory governor plus allocator-aware builders, an imported-buffer boundary, or an Arrow patch boundary. That cost is manageable for Rust- and C++-origin allocators, but a Go-origin allocator adds stricter pointer-lifetime rules and likely requires routing retained memory outside the Go heap. The cost is real, but current evidence still does not justify giving up a Rust-first kernel.

### Build / Debug / Tooling Complexity

- weight: TBD
- C++: TBD
- Rust: TBD
- notes: TBD

### Long-Term Maintenance / Correctness Risk

- weight: high
- C++: 3
- Rust: 5
- notes: The issue #2 gap is now more precisely stated: allocator routing is the sharp edge, while accounting, reservations, and operator-managed spill are already credible in Rust. If that allocator-routing boundary is kept explicit and narrow, Rust still preserves the project's preferred safety and maintenance advantages without requiring the kernel body to move into C++.

## Decision Rule

Before using this scorecard to make a recommendation, write down:

- which dimensions are mandatory versus merely nice to have
- which tradeoffs the project is willing to accept
- whether an unresolved tie should trigger thin spikes instead of a forced decision
