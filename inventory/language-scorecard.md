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
- C++: TBD
- Rust: TBD
- notes: includes allocator / memory-pool support, accounting hooks, and spill implications

### Runtime / Concurrency Fit

- weight: high
- C++: TBD
- Rust: TBD
- notes: includes staged execution, cancellation, backpressure, observability, and the ability to keep the kernel primarily Rust even if lower-level escape hatches are needed

### FFI / Boundary Complexity

- weight: high
- C++: TBD
- Rust: TBD
- notes: especially important if the fallback path is Rust kernel + selective C++ lower layers

### Build / Debug / Tooling Complexity

- weight: TBD
- C++: TBD
- Rust: TBD
- notes: TBD

### Long-Term Maintenance / Correctness Risk

- weight: high
- C++: TBD
- Rust: TBD
- notes: this dimension should reflect the project's stated preference for Rust safety and agent-friendly development, not treat both languages as culturally neutral

## Decision Rule

Before using this scorecard to make a recommendation, write down:

- which dimensions are mandatory versus merely nice to have
- which tradeoffs the project is willing to accept
- whether an unresolved tie should trigger thin spikes instead of a forced decision
