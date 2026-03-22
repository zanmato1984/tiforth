# Post-Reorganization Shared Carrier Extraction Boundary

Status: issue #401 design checkpoint

Verified: 2026-03-21

Related issues:

- #396 `design: formalize project layout and top-level shared surfaces`
- #398 `refactor: organize differential harness crate around grouped modules`
- #399 `refactor: organize TiDB and TiFlash adapter crates`
- #400 `refactor: organize TiKV adapter crate around engine and slices`
- #401 `design: evaluate shared carrier extraction after harness and adapter reorganizations`

## Question

Now that issue #398 has grouped the differential harness crate and issues #399
and #400 have grouped the adapter crates around `engine/` plus `slices/`,
which repeated request, result, artifact, and support helpers are actually
narrow and stable enough to share, and does that justify a new shared crate?

## Inputs Considered

- `docs/architecture.md`
- `docs/design/formal-project-layout-and-top-level-shared-surfaces.md`
- `crates/tiforth-adapter-tidb/src/lib.rs`
- `crates/tiforth-adapter-tidb/src/engine/mod.rs`
- `crates/tiforth-adapter-tidb/src/slices/first_expression_slice.rs`
- `crates/tiforth-adapter-tiflash/src/lib.rs`
- `crates/tiforth-adapter-tiflash/src/engine/mod.rs`
- `crates/tiforth-adapter-tiflash/src/slices/first_expression_slice.rs`
- `crates/tiforth-adapter-tikv/src/lib.rs`
- `crates/tiforth-adapter-tikv/src/engine/mod.rs`
- `crates/tiforth-adapter-tikv/src/slices/first_expression_slice.rs`
- `crates/tiforth-harness-differential/src/lib.rs`
- `crates/tiforth-harness-differential/src/slices/mod.rs`
- `crates/tiforth-harness-differential/src/pairwise/mod.rs`
- `crates/tiforth-harness-differential/src/tikv/mod.rs`
- representative differential harness modules such as
  `crates/tiforth-harness-differential/src/first_expression_slice.rs`,
  `crates/tiforth-harness-differential/src/first_filter_is_not_null_slice.rs`,
  `crates/tiforth-harness-differential/src/first_expression_slice_tikv_pairwise.rs`,
  and `crates/tiforth-harness-differential/src/first_decimal128_slice_tikv_pairwise.rs`
- issue #401

## Design Summary

The post-reorganization code shape now makes repeated carriers and helpers easy
to identify, but it still does **not** justify a new cross-workspace shared
crate.

The stable extraction point that exists now is smaller:

- keep engine execution helpers shared only inside each adapter crate
- keep differential artifact and pairwise comparison helpers shared only inside
  `crates/tiforth-harness-differential`
- keep slice request carriers, slice result carriers, drift summaries, and live
  runner wiring local until a later issue proves one reusable Rust API instead
  of only similar-looking structs

In other words: the right next step is a shared module family inside an
existing crate, not a new workspace crate.

## Findings

### 1. Adapter support repetition is real, but still engine-shaped

The adapter reorganizations already exposed one legitimate shared helper
boundary per engine crate:

- TiDB and TiFlash now each have `src/engine/mod.rs`
- those two `engine` modules currently differ only by engine-name constants
- TiKV now has its own `src/engine/mod.rs` with the same result and error
  carrier shape, but a different execution-plan type and a smaller helper set

That means the right current sharing level for adapter support code is:

- shared within one adapter crate through `engine/`
- not yet shared across adapter crates through a new workspace crate

The reason is not that repetition is absent. The reason is that the stable Rust
API is still engine-shaped:

- TiDB and TiFlash use the same SQL-plan helper today, but that symmetry is
  only between those two crates
- TiKV already diverges on plan type and on which normalization helpers it
  actually needs
- slice-local request validation, SQL text, schema normalization, unsupported
  handling, and follow-up messages still vary materially by slice

So a cross-engine adapter support crate would freeze a broader dependency edge
before all current engines need the same implementation surface.

### 2. Differential harness repetition is now narrow enough to share, but only inside the harness crate

The differential harness crate now shows two repeated internal shapes clearly.

First, the TiDB-versus-TiFlash slice modules repeatedly define the same
artifact-carrier skeleton:

- canonical request fields
- normalized schema-field, case-outcome, and case-result carriers
- `CaseResultsArtifact`
- `DriftStatus`, `ComparisonDimension`, `DriftCase`, and `DriftReport`
- JSON sidecar rendering and Markdown report rendering skeletons

Second, the TiKV pairwise follow-ons repeatedly define the same comparison
workflow:

- verify TiKV canonical requests against the existing TiDB-versus-TiFlash
  shared requests
- execute the shared TiDB-versus-TiFlash slice plus one TiKV single-engine
  slice
- convert TiKV single-engine case results into comparable pairwise carriers
- build two pairwise drift reports from the same comparison pattern

That repetition is now stable enough to justify one future extraction target:
an internal harness module family for shared differential artifact carriers and
pairwise comparison helpers under `crates/tiforth-harness-differential`.

It still does **not** justify a separate workspace crate because:

- the repeated code lives inside one long-lived harness crate already
- conformance harnesses are not yet using the same implementation surface
- the request fields and drift-summary logic still vary enough by slice that the
  first useful centralization should stay close to the differential harness
  itself

### 3. Similar-looking slice carriers still remain slice-local

The reorganizations did not remove the slice-specific differences that still
matter for API shape:

- some requests use `projection_ref`, some use `filter_ref`, and other slices
  add ordering, collation, or nested-structure-specific fields
- some pairwise identity checks compare projection refs, others compare filter
  refs, and some slices carry optional fields rather than one uniform key set
- drift dimensions, unsupported-class rules, and reviewer-facing follow-up text
  still vary materially by slice family
- live runner environment inputs, CLI arguments, and refresh-entrypoint wiring
  are still slice-specific orchestration

These are good reasons to keep slice request and result structs local even when
their field lists look superficially similar.

## Boundary

This issue closes the post-reorganization extraction question with the following
boundary:

- do not add a new shared workspace crate for adapter or harness carrier code
  now
- adapter support extraction should continue to stop at each crate's `engine/`
  boundary unless a later issue shows one stable API used across all relevant
  engine crates
- the first justified implementation-sharing follow-on, if needed, is one
  internal module family inside `crates/tiforth-harness-differential` for
  shared differential artifact carriers and pairwise comparison helpers
- slice request carriers, slice result normalization, drift summaries, CLI
  wiring, and live-runner orchestration remain local until a later docs-first
  issue narrows them further

## Reconsideration Trigger

Re-open this decision only when all of these are true:

- more than one long-lived crate category needs the same Rust implementation
  surface, not just the same documented semantics
- the shared API is stable across more than one current slice family or engine
- moving the code would simplify dependency edges instead of creating a new
  framework layer ahead of stable behavior

Until then, the repository should keep using the existing workspace categories
from issue #396 and prefer intra-crate extraction over new crates.
