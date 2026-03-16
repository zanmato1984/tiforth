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

- result: Rust-first is **conditionally viable with a narrow lower-level boundary**
- question 1: provisionally yes, if memory reservation and spill policy live above Arrow allocation internals
- question 2: provisionally yes, the required boundary appears narrow enough to preserve a Rust-developed kernel
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
- notes: C++ Arrow still has the stronger native allocator / memory-pool surface. Rust Arrow `58.0.0` now has claim-based exact accounting hooks and externally owned buffer support, but normal Arrow allocations still do not route through a fallible quota-enforcing pool. Rust remains viable if tiforth keeps reservation policy and precise batch accounting in a narrow runtime-owned boundary.

### Runtime / Concurrency Fit

- weight: high
- C++: 3
- Rust: 5
- notes: Issue #2 strengthens Rust here. Current DataFusion `52.3.0` shows Rust-side memory reservations, spillable consumers, fair spill pools, disk spill management, and non-Arrow accounting helpers layered above Arrow. That lines up with tiforth's staged, backpressured runtime direction without requiring a C++-first kernel.

### FFI / Boundary Complexity

- weight: high
- C++: 4
- Rust: 3
- notes: C++ avoids some allocator-boundary work if tiforth later needs direct host memory-pool integration. Rust likely pays for a small memory-governor / imported-buffer boundary, but current evidence suggests that boundary can stay narrow and does not justify giving up a Rust-first kernel.

### Build / Debug / Tooling Complexity

- weight: TBD
- C++: TBD
- Rust: TBD
- notes: TBD

### Long-Term Maintenance / Correctness Risk

- weight: high
- C++: 3
- Rust: 5
- notes: The memory-accounting gap found in issue #2 is real but bounded. Once the sharp edge is isolated behind a small boundary, Rust keeps the project's preferred safety and maintenance advantages without requiring the kernel body to move into C++.

## Decision Rule

Before using this scorecard to make a recommendation, write down:

- which dimensions are mandatory versus merely nice to have
- which tradeoffs the project is willing to accept
- whether an unresolved tie should trigger thin spikes instead of a forced decision
