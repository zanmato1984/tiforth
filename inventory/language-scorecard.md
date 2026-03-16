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

- result: Rust-first is **not blocked for milestone 1; it needs an explicit host reservation or admission contract before internal allocation**
- question 1: yes for accounting, reservation or admission control, internal allocation after approval, and operator-managed spill
- question 2: not needed for milestone 1; a special allocator-routing boundary can be deferred unless a later milestone restores mandatory direct host allocator participation in ordinary Arrow growth
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
- notes: C++ Arrow still has the stronger native allocator / memory-pool surface. Rust Arrow `58.0.0` now has claim-based exact accounting hooks and `Buffer::from_custom_allocation`, while normal `MutableBuffer` / builder growth still uses standard Rust allocation paths. Under Rossi's revised milestone-1 contract, that limitation no longer blocks Rust because host memory control happens through reserve-first admission rather than direct allocator routing.

### Runtime / Concurrency Fit

- weight: high
- C++: 3
- Rust: 5
- notes: Issue #2 now strengthens Rust more directly. Current DataFusion `52.3.0` shows Rust-side memory reservations, spillable consumers, fair spill pools, disk spill management, non-Arrow accounting helpers, and an Arrow claim bridge layered above Arrow. That lines up with tiforth's staged, backpressured runtime direction. The required milestone-1 edge is now a reserve-first host admission contract, not allocator routing for stock Arrow build paths.

### FFI / Boundary Complexity

- weight: high
- C++: 4
- Rust: 4
- notes: Rossi's revised requirement materially narrows the boundary. Milestone 1 now needs a reserve-or-deny ABI plus attribution and release semantics, not direct host allocator routing or long-lived foreign-buffer ownership on every Arrow growth path. That keeps cross-language work real, but much smaller than the earlier allocator-routing memo implied, including for Go-facing hosts.

### Build / Debug / Tooling Complexity

- weight: TBD
- C++: TBD
- Rust: TBD
- notes: TBD

### Long-Term Maintenance / Correctness Risk

- weight: high
- C++: 3
- Rust: 5
- notes: The issue #2 gap is now more precisely stated: host admission before allocation is the milestone-1 contract, while direct allocator routing is deferred. That keeps Rust's safety and maintenance advantages intact without requiring the kernel body to move into C++.

## Decision Rule

Before using this scorecard to make a recommendation, write down:

- which dimensions are mandatory versus merely nice to have
- which tradeoffs the project is willing to accept
- whether an unresolved tie should trigger thin spikes instead of a forced decision
