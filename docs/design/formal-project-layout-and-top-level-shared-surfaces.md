# Formal Project Layout And Top-Level Shared Surfaces

Status: issue #396 design checkpoint

Verified: 2026-03-21

Related issues:

- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #80 `design: define the next thin end-to-end slice after milestone-1 projection`
- #86 `design: define milestone-1 operator and expression attachment to adopted runtime contract`
- #92 `design: define adapter-local runtime orchestration boundary`
- #137 `design: choose first post-gate kernel expansion boundary`
- #396 `design: formalize project layout and top-level shared surfaces`

## Question

Now that `tiforth` has multiple executable slices across the shared kernel,
engine adapters, differential harnesses, and checked-in evidence, which
project-organization and top-level interface boundaries should be treated as
formal, and which should remain intentionally slice-local?

## Inputs Considered

- `README.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/contracts/data.md`
- `docs/contracts/runtime.md`
- `docs/spec/type-system.md`
- `docs/process/documentation-updates.md`
- `crates/tiforth-kernel/src/lib.rs`
- `crates/tiforth-kernel/src/batch.rs`
- `crates/tiforth-kernel/src/runtime.rs`
- `crates/tiforth-kernel/src/operators.rs`
- `crates/tiforth-adapter-tidb/src/lib.rs`
- `crates/tiforth-adapter-tiflash/src/lib.rs`
- `crates/tiforth-adapter-tikv/src/lib.rs`
- `crates/tiforth-harness-differential/src/lib.rs`
- representative first-slice adapter and harness modules
- issue #396

## Design Summary

`tiforth` now has enough executable surface area to formalize its top-level
project organization, but not enough stable shared behavior to justify a broad
new framework layer.

This checkpoint therefore fixes a thin structural boundary:

- the repository top level is now formally organized by durable concern
  (`docs/`, `tests/`, `inventory/`, `adapters/`, `crates/`, `scripts/`), not
  by slice
- the long-lived Rust workspace shape is currently one shared kernel crate,
  one adapter crate per engine, and one harness crate per harness family
- the shared top-level Rust interfaces that are mature enough to formalize now
  are the kernel's current data, runtime, admission, ownership, expression,
  predicate, operator, and local-snapshot surfaces
- adapter-local orchestration, slice-specific carriers, and most differential
  harness glue remain intentionally slice-local until a later issue proves a
  narrower shared extraction point

This keeps the project legible as slice count grows without pretending that
`tiforth` already has a settled planner API, general function registry, or one
shared adapter framework.

## Formal Top-Level Repository Shape

The repository top level is now treated as a stable, long-lived shape:

- `docs/`: source-of-truth architecture, contracts, specs, design checkpoints,
  and accepted decisions
- `tests/`: canonical conformance and differential checkpoint docs plus local
  fixture assets
- `inventory/`: checked-in reviewable evidence such as case-results, drift
  reports, and compatibility notes
- `adapters/`: adapter-facing boundary docs for shared slice carriers
- `crates/`: Rust implementation crates grouped by long-lived layer or engine
  boundary
- `scripts/`: repeatable local workflow, validation, and artifact-refresh
  helpers

This formalizes the top level as one directory per durable responsibility. New
slice work should fit inside those existing concerns instead of creating new
top-level directories for each semantic checkpoint.

## Formal Workspace-Crate Categories

The current long-lived crate categories are:

- `crates/tiforth-kernel`: shared execution surfaces and narrow executable
  kernel slices
- `crates/tiforth-adapter-<engine>`: engine-local translation and execution
  over shared slice contracts
- `crates/tiforth-harness-<kind>`: harness execution, normalization,
  comparison, and artifact rendering

This means:

- shared kernel growth belongs under `tiforth-kernel` unless a later accepted
  boundary proves that a separate shared crate is necessary
- engine-specific execution logic belongs under the corresponding
  `tiforth-adapter-<engine>` crate rather than in a cross-engine utility crate
- differential execution and artifact generation belong under harness crates
  rather than in adapters or in `inventory/`
- slice count alone is not a reason to add one new crate per slice

New workspace crates should appear only when a boundary is already documented,
is reused across more than one existing long-lived crate or workstream, and
would otherwise become a misleading or unstable dependency edge.

## Top-Level Shared Rust Interfaces To Formalize Now

The currently stable shared Rust surfaces are the ones already repeated across
the accepted kernel and runtime checkpoints:

- runtime specialization over the adopted upstream contract:
  `TiforthTypes`
- shared batch and ownership glue:
  `TiforthBatch`, `OwnershipToken`, `BatchOrigin`
- admission and ownership-governance surfaces:
  `AdmissionController`, `AdmissionConsumer`, `ConsumerSpec`, and the current
  admission-event recorders
- runtime observability surfaces:
  `RuntimeContext`, `RuntimeEvent`, `LocalExecutionSnapshot`, and
  `LocalExecutionFixture`
- current shared expression and predicate nodes:
  `Expr`, `ProjectionExpr`, and `FilterPredicate`
- current runtime-entered operator surfaces:
  `StaticRecordBatchSource`, `ProjectionPipe`, `FilterPipe`, `ExchangePipe`,
  and `CollectSink`

These are the top-level `tiforth` interfaces that other `tiforth` crates,
tests, and future docs-first checkpoints may treat as intentionally shared.

The policy from `docs/contracts/runtime.md` and
`docs/design/operator-expression-runtime-attachment.md` remains unchanged:
`tiforth` still adopts the upstream `broken-pipeline` runtime traits directly
instead of adding a second `tiforth` runtime facade above them.

## Shared Carrier Policy Above The Kernel

Repeated adapter and differential modules now show one real pattern: canonical
request fields, normalized result fields, schema-field carriers, error classes,
and artifact bundles recur across slices.

That repetition is enough to formalize one boundary, but not yet enough to
freeze one new shared implementation crate.

The formal boundary for now is:

- shared meanings for request, result, and artifact carriers live in docs under
  `adapters/` and `tests/differential/`
- code may continue to carry slice-local Rust structs that implement those
  documented meanings
- follow-on issues may centralize those carriers inside one crate only after a
  narrower shared carrier boundary is documented and reviewable on its own

This keeps semantics centralized before implementation sharing, which matches
the repository's docs-first rule.

## What Remains Intentionally Slice-Local

This checkpoint does **not** formalize all repeated code as shared surface.

The following still remain slice-local by default:

- exact `case_id` inventories and slice-specific canonical requests
- engine-native SQL text, execution plans, and normalization details inside one
  adapter
- slice-specific schema normalization rules and unsupported-type checks
- pairwise drift-report dimensions or summaries that are not yet reused across
  multiple slice families
- live-runner environment wiring and artifact-refresh entrypoints
- slice-specific CLI bins and helper scripts

When a contributor sees repetition in those areas, the default next step is to
extract within one crate first, or to open a docs-first issue, rather than to
immediately introduce a new cross-workspace abstraction.

## Internal Organization Bias For Follow-On Refactors

Top-level organization is now fixed by responsibility. Follow-on refactors
should make internal crate layout match that bias more clearly over time.

Recommended direction:

- keep crate roots small and let them expose durable shared entry points
- group engine-local shared helpers inside each adapter crate before proposing a
  cross-engine adapter support crate
- group differential harness shared helpers inside the harness crate before
  proposing a broader workspace extraction
- keep CLI `src/bin/` entrypoints thin wrappers over library modules

For adapter and harness crates specifically, future cleanup should prefer a
shape that separates common helpers from slice entrypoints instead of leaving an
ever-growing flat list of `first_*_slice.rs` modules at crate root.

This is a refactor direction, not an all-at-once migration requirement for
issue #396.

## Why This Boundary Now

- multiple slices now exercise the same kernel-owned runtime, batch, admission,
  and ownership surfaces
- the top-level repository shape has already stabilized in docs and practice
- adapter and harness code now shows enough repeated structure to justify a
  clearer organization policy
- the project still does not have enough settled shared behavior to justify a
  planner layer, one broad function catalog API, or a general adapter framework

This is therefore the right time to formalize responsibilities and shared entry
points, while still refusing premature framework growth.

## Deferred Questions

- whether adapter request and result carriers should later move into one shared
  implementation crate
- whether conformance and differential harnesses should later share one common
  harness-support crate
- whether a later milestone needs a broader operator-construction surface above
  the current kernel entry points
- whether planner, optimizer, or function-registry surfaces deserve their own
  top-level crates

## Result

`tiforth` now has a formal project-organization checkpoint:

- organize the repository by durable concern, not by slice
- treat `tiforth-kernel`, `tiforth-adapter-<engine>`, and
  `tiforth-harness-<kind>` as the current long-lived workspace categories
- keep the formal shared Rust top level thin and centered on the current kernel
  surfaces plus docs-defined carrier meanings
- keep slice-local orchestration and carrier implementations local until a later
  issue proves a narrower shared extraction point
