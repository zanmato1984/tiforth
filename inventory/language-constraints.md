# Language Constraints

This file records evidence and constraints relevant to the shared-kernel language decision.

It is not the place to declare the winner.

## Decision Scope

In scope:

- language for the shared `tiforth` kernel / runtime core
- implications for Arrow-native data handling
- implications for broken-pipeline-inspired runtime work
- implications for TiDB / TiFlash / TiKV integration

Out of scope:

- adapter language standardization
- repo-wide tooling standardization beyond what the core decision requires

## Priority Concerns From Rossi

### Default Posture

- Strong default preference: **Rust** for the shared kernel / runtime core.
- Main reasons: memory and concurrency safety, plus strong fit for agent-driven implementation work.
- This means the decision process should treat Rust as the default path unless a real blocker is identified.

### Fallback Posture If Rust Hits A Blocker

- Prefer a hybrid design over a full retreat to a C++ kernel.
- If needed, use C++ for lower-level pieces while keeping the kernel body primarily Rust.
- Use explicit FFI boundaries only where necessary.
- Preserve the ability to develop the kernel independently in Rust.

### Candidate Blockers

#### 1. Arrow Memory Pool Support In Rust

Concern:

- Rust Arrow may not yet provide the full memory-pool support tiforth needs.
- TiDB, TiKV, and TiFlash all require fine-grained memory tracking.
- Spill support is also expected, so allocation control and accounting are not cosmetic concerns.

Why this matters:

- if Rust cannot support the memory-accounting model tiforth actually needs, the core design may be blocked or forced into awkward workarounds
- this is a likely blocker candidate rather than a minor inconvenience

#### 2. broken-pipeline Compatibility

Concern:

- `broken-pipeline` is currently C++ and header-only
- tiforth wants runtime ideas from broken-pipeline, and may want some direct reuse or adaptation path

Important context:

- Rossi owns `broken-pipeline`
- this lowers the barrier to changing the protocol, introducing a Rust implementation, or reshaping the interface to support a Rust-first kernel

Implication:

- broken-pipeline being in C++ is a serious integration topic, but not automatically a blocker if the protocol can evolve

### Working Decision Bias

The current burden of proof is asymmetric:

- Rust does not need to prove that C++ is impossible
- C++ needs either a clear positive advantage or a concrete blocker against Rust

## Baseline Constraints To Investigate

### 1. Donor Leverage

Questions:

- Which parts of `tiforth-legacy` are easier to reinterpret if the new core is C++?
- Which donor concepts remain equally portable regardless of implementation language?
- Where would choosing C++ accidentally encourage carrying forward implementation debt?

### 2. Host Project Integration

Questions:

- What is the practical embedding cost for TiFlash?
- What is the practical embedding cost for TiKV?
- What is the practical embedding cost for TiDB?
- Which integration surfaces are likely to require stable C ABI boundaries either way?

### 3. Arrow And Data Contract Fit

Questions:

- How mature is the Arrow story needed by tiforth in each language?
- Where do ownership and buffer-lifetime rules become easier or harder?
- Does either choice make the shared data contract easier to keep engine-neutral?

### 4. Runtime Contract Fit

Questions:

- Which language better supports the runtime properties tiforth likely needs: staged execution, interruption, backpressure, cancellation, observability?
- Where does concurrency become safer versus more predictable versus more debuggable?
- How much scheduler freedom is realistically needed in the first milestones?

### 5. Build, Debug, And Tooling Cost

Questions:

- What does local development look like for contributors?
- What does cross-project integration testing look like?
- Which option creates more friction in build graph, symbol management, packaging, or debugging?

### 6. Long-Term Maintenance

Questions:

- Which language better protects the project from memory and concurrency bugs in the core?
- Which language better matches the maintenance burden tiforth is willing to carry?
- Which language will make later contributors faster or slower to make correct changes?

## Issue #2 Checkpoint: Memory Accounting, Host Admission, And Spill

Status on 2026-03-16, reframed for Rossi's revised milestone-1 requirement:

- Rust-first is now **not blocked for milestone 1**; the required contract is **host reservation or admission before internal allocation, then fail on deny**
- memory accounting, host reservation or admission control, internal allocation after approval, and operator-managed spill do not currently force a C++ kernel
- stock `arrow-rs` still does not route ordinary mutable Arrow growth paths through a host allocator, but that is no longer the milestone-1 requirement
- the earlier allocator-routing sharp edge is therefore demoted from day-one blocker to later optional design work
- cross-language complexity is materially smaller when the host boundary is admit-or-deny rather than long-lived foreign-buffer ownership

Minimum day-one requirements that matter for the language decision:

- query / stage / operator attribution for Arrow and non-Arrow memory
- fallible host reservation or admission-control points before internal allocation of large mutable state or Arrow-backed operator output
- explicit fail-on-deny semantics: no allocation and no continued execution on that path after host rejection
- ownership-aware accounting for shared Arrow buffers across slices and stage handoffs
- spillable versus unspillable consumer distinction
- an operator-managed spill path that can release memory pressure through runtime-managed disk use

Observed upstream facts from the issue #2 investigation:

- current `arrow-rs` (`58.0.0` workspace version) has claim-based accounting hooks, including recursive `Array::claim`, but its pool support remains tracking-oriented rather than a C++-style allocator / memory-pool policy surface
- current `arrow-rs` mutable and builder growth paths still allocate through `std::alloc` or ordinary Rust container allocation paths, and that is acceptable for milestone 1 so long as host admission happens before allocation rather than through direct allocator routing
- current `arrow-rs` `RecordBatch` sizing helpers still document possible overcounting for shared buffers, so precise batch accounting should live in a helper layer rather than rely on a naive summed size API
- current `datafusion` (`52.3.0` workspace version) demonstrates a Rust-native pattern for fallible reservations, spill-aware consumers, disk spill management, and container-accounting helpers layered above Arrow
- current `datafusion` also includes an Arrow-facing pool adapter, but that bridge still sits on Arrow's infallible tracking interface and does not by itself make stock Arrow builder growth host-allocator-routed
- official Go `cgo` pointer rules matter much less for an admission-only milestone-1 boundary, because the host no longer needs to hand `tiforth` retained Go-heap-backed Arrow buffers just to enforce day-one memory control

Implication:

- the broad blocker candidate is narrower than "Rust cannot do memory governance"
- the earlier allocator-routing sharp edge no longer controls the milestone-1 language choice
- the right response is an explicit memory-governor **plus host-admission** boundary, not an automatic retreat from a Rust-first kernel
- Rust-first is now better supported for day one, while direct host allocator routing can be revisited only if a later milestone makes it mandatory again

Detailed evidence:

- see `inventory/memory-accounting-blocker.md`

## Known Structural Tension

There is an obvious tension between:

- maximizing TiFlash donor leverage and immediate practicality
- maximizing reboot cleanliness and long-term kernel quality
- reusing existing C++ runtime ideas quickly versus keeping the kernel genuinely Rust-first

The decision process should make those tensions explicit instead of hiding them inside implementation momentum.

## Evidence Rules

- record observations before conclusions
- separate facts from preferences
- if a concern is speculative, mark it as speculative
- prefer repo-local notes over chat-only conclusions
