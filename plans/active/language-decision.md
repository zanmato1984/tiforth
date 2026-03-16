# Language Decision Plan

Status: active

Related issue: #1 (`design: choose tiforth shared-kernel language (C++ vs Rust)`)

## Question

What language should `tiforth` use for the shared kernel / runtime core: **C++** or **Rust**?

This plan exists to prevent the repository from making the choice accidentally through the first implementation path that happens to land.

## Why This Is A Project Task

The language choice affects:

- how donor material from `tiforth-legacy` can be reused in spirit
- how cleanly Arrow-native data contracts can be implemented
- how realistic a broken-pipeline-inspired runtime contract is
- how painful integration will be for TiDB, TiFlash, and TiKV
- what kind of safety, debugging, and build complexity the project absorbs long term

Because the impact is cross-cutting, the choice should be tracked as a first-class tiforth task instead of being left implicit.

## Current State

The decision question is now constrained by a clear default posture from Rossi:

- prefer **Rust** for the shared kernel / runtime core if there is no blocker
- if blockers exist, prefer a **hybrid path** where low-level pieces may remain or become C++, but the kernel stays primarily Rust and can be developed independently in Rust
- do not choose C++ for the kernel by inertia alone; it must be justified by concrete blockers

Current focus:

- identify whether any blocker is real, architectural, and near-term
- decide whether a Rust-first design can absorb those blockers through explicit boundaries
- avoid accidental commitment to a C++-first kernel just because donor material already exists there

## Non-Goals

This plan does not decide:

- the language of every adapter
- the full build, packaging, or CI design
- the exact production architecture of the first kernel
- whether all future tooling must follow the same language choice

## Deliverables

- `inventory/language-constraints.md`
- `inventory/language-scorecard.md`
- a final decision record under `docs/decisions/`
- optional thin spikes, if paper analysis alone is insufficient

## Current Recommendation

The current recommendation is now strong enough to draft as a decision record:

- use **Rust** for the shared kernel / runtime core
- directly adopt the Arrow-bound runtime contract from `broken-pipeline-rs`
- treat `broken-pipeline-schedule` as test / harness-only, not a permanent production contract center
- use reserve-first host memory admission for milestone 1 rather than mandatory direct host-allocator routing for every internal Arrow allocation path

## Immediate Subtasks

- #2 `investigate: rust-first memory accounting and spill blockers`
- #3 `design: broken-pipeline adaptation path for rust-first tiforth`

## Next Implementation-Facing Tasks

- #8 `design: host memory admission ABI for tiforth`
- #9 `design: tiforth dependency boundary over broken-pipeline-rs`
- #10 `milestone-1: first Arrow-bound operator and expression slice`

## Work Phases

### Phase 0: Capture Priority Concerns

Record Rossi's top decision criteria, anti-goals, and default preference.

Exit criteria:

- the Rust-first default posture is written down
- candidate blockers are explicitly listed
- obvious non-goals are explicit

### Phase 1: Inventory Constraints

Document baseline constraints that exist regardless of preference.

Focus areas:

- TiDB / TiFlash / TiKV integration friction
- Arrow ecosystem and memory model implications
- runtime / concurrency / cancellation implications
- build, debug, test, and FFI complexity
- donor leverage versus reboot cleanliness

Execution note:

- issue #2 should sharpen the memory-accounting / spill blocker question
- issue #3 should sharpen the broken-pipeline adaptation question
- these can run in parallel, but #2 has slightly higher priority because it is the more likely true blocker

Exit criteria:

- the constraint log is concrete enough to compare options
- unknowns are called out explicitly

### Phase 2: Define The Rubric

Turn the constraint inventory into a decision rubric.

Exit criteria:

- evaluation dimensions are stable
- a simple scoring or ranking method exists

### Phase 3: Compare C++ And Rust

Apply the rubric and document tradeoffs.

Exit criteria:

- both options are evaluated against the same dimensions
- major tradeoffs are written, not implied

### Phase 4: Decide Whether Thin Spikes Are Needed

If paper analysis leaves the choice ambiguous, define and run the smallest useful spikes.

Candidate spike shape:

- Arrow batch input
- minimal expression evaluation slice
- minimal runtime or stage boundary
- notes on integration and debugging friction

Exit criteria:

- either spikes are declared unnecessary, or their scope is tightly defined

### Phase 5: Write The Decision Record

Record the final recommendation and its consequences.

Exit criteria:

- the chosen language is explicit
- the rejected alternative is discussed fairly
- downstream implications and follow-up work are listed

## Open Risks

- optimizing for the wrong host project too early
- letting donor convenience outweigh long-term architecture quality
- overvaluing language aesthetics and undervaluing integration cost
- drifting into implementation before the decision inputs are clear
