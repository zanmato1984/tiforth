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

1. Can a Rust-first kernel satisfy `tiforth`'s required memory-accounting and spill model?
2. If not directly, can that gap be isolated behind a narrow lower-level boundary without giving up a Rust-developed kernel?
<<<<<<< HEAD
3. Can broken-pipeline ideas be adopted cleanly enough for a Rust-first runtime path?
=======
3. Can `tiforth` adopt `broken-pipeline-rs` cleanly enough for a Rust-first runtime path?
>>>>>>> b14e856 (docs: adopt broken-pipeline-rs runtime framing)
4. Are the remaining integration costs genuinely blockers, or just engineering work?

If these questions are not answered, the decision is not ready.

## Current Checkpoint From Issue #2

- result: Rust-first is **not blocked for milestone 1; it needs an explicit host reservation or admission contract before internal allocation**
- question 1: yes for accounting, reservation or admission control, internal allocation after approval, and operator-managed spill
- question 2: not needed for milestone 1; a special allocator-routing boundary can be deferred unless a later milestone restores mandatory direct host allocator participation in ordinary Arrow growth
- detailed note: `inventory/memory-accounting-blocker.md`

## Current Checkpoint From Issue #3

<<<<<<< HEAD
- result: `broken-pipeline-rs` already exists and is **not a Rust blocker; it is now the direct runtime substrate candidate**
- question 3: yes, provisionally; the recommended path is direct adoption of the Arrow-bound `broken-pipeline-rs` runtime contract, with `tiforth` acting as an operator/expression library layered on top
- question 4: the remaining costs look like dependency-boundary and project-surface work, not runtime-language viability work, as long as `broken-pipeline-schedule` stays test/harness-only and source-level C++ compatibility is not treated as a goal
=======
- result: `broken-pipeline` is **not currently a Rust blocker; `broken-pipeline-rs` already supplies the Arrow-bound runtime contract that `tiforth` should adopt and expose directly**
- question 3: yes, provisionally; the recommended path is direct adoption of the upstream Arrow-bound contract, with `tiforth` positioned as an operator and expression library on top and `broken-pipeline-schedule` reserved for local harness use
- question 4: the remaining costs look like dependency packaging, revision management, and keeping harness-only scheduler utilities out of the shared contract; they become an architectural blocker only if the upstream Rust runtime proves semantically insufficient for `tiforth`
>>>>>>> b14e856 (docs: adopt broken-pipeline-rs runtime framing)
- detailed note: `docs/design/broken-pipeline-adaptation.md`

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

### Broken-Pipeline Adaptation Fit

- weight: high
- C++: 2
- Rust: 5
<<<<<<< HEAD
- notes: C++ still has the advantage of historical lineage, but that is no longer Rust's main challenge because `broken-pipeline-rs` already exists as a native Rust port with a core protocol crate, an Arrow-bound contract layer, an optional schedule layer, and a focused C interop layer. Rust scores higher because tiforth now intends to directly adopt the Arrow-bound `broken-pipeline-rs` runtime contract, keep `broken-pipeline-schedule` test/harness-only, and avoid source-level compatibility goals with the original C++ implementation.
=======
- notes: the clarified direction now treats `broken-pipeline-rs` as the authoritative runtime-contract source for `tiforth`. Rust scores highest because `tiforth` can adopt the upstream Arrow-bound runtime contract directly and build operators and expressions on top of it, while reserving `broken-pipeline-schedule` for local harness use. C++ remains relevant only as historical lineage and provenance, not as the active design center or preferred compatibility surface.
>>>>>>> b14e856 (docs: adopt broken-pipeline-rs runtime framing)

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
- notes: The issue #2 gap is now more precisely stated: host admission before allocation is the milestone-1 contract, while direct allocator routing is deferred. Combined with direct adoption of `broken-pipeline-rs`, that keeps Rust's safety and maintenance advantages intact without requiring the kernel body to move into C++.

## Decision Rule

Before using this scorecard to make a recommendation, write down:

- which dimensions are mandatory versus merely nice to have
- which tradeoffs the project is willing to accept
- whether an unresolved tie should trigger thin spikes instead of a forced decision
