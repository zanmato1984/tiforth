# Kernel Expansion Acceptance Criteria

Status: issue #82 design checkpoint

Verified: 2026-03-18

Related issues:

- #10 `milestone-1: first Arrow-bound operator and expression slice`
- #68 `design: define first differential expression slice and drift report format`
- #72 `design: define first differential adapter request/response surface`
- #80 `design: define the next thin end-to-end slice after milestone-1 projection`
- #82 `design: define acceptance criteria for expanding the kernel beyond milestone-1`
- #137 `design: choose first post-gate kernel expansion boundary`

## Question

What must be true before a follow-on issue widens the shared kernel beyond the current milestone-1 expression-projection slice?

## Inputs Considered

- `README.md`
- `docs/vision.md`
- `docs/architecture.md`
- `docs/design/next-thin-end-to-end-slice.md`
- `docs/spec/milestone-1-expression-projection.md`
- `docs/spec/type-system.md`
- `tests/conformance/expression-projection-slice.md`
- `tests/differential/first-expression-slice.md`
- `adapters/first-expression-slice.md`
- `tests/differential/first-expression-slice-artifacts.md`
- issue #82

## Acceptance Summary

`tiforth` should widen the shared kernel only after the next thin end-to-end slice from `docs/design/next-thin-end-to-end-slice.md` exists as executable, reviewable evidence and a follow-on issue can satisfy all of these conditions together:

- the motivation is tied to one explicit semantic gap or one explicit next capability, not to general parity or convenience
- the affected spec, contract, type-system, adapter, and harness docs are already updated or explicitly unchanged
- the new coverage and evidence path is named before code lands
- the proposed kernel growth stays narrow enough to review as one checkpoint
- the issue can state exactly what docs, tests, fixtures, inventory artifacts, and kernel changes will prove completion

This keeps the repo harness-first: evidence and docs justify kernel growth rather than the kernel forcing the next round of evidence.

## Required Gate

Before a later issue proposes layer-3 growth, all of these should be true.

### 1. The Current Differential Checkpoint Exists

- the executable checkpoint from `docs/design/next-thin-end-to-end-slice.md` can run the documented `first-expression-slice` cases through the shared adapter request surface
- the checkpoint emits normalized `case result` records and at least one reviewable TiDB-versus-TiFlash drift report using the documented inventory carriers
- `adapter_unavailable` may still exist for some cases, but unsupported coverage must be explicit rather than silently absent

Until that checkpoint exists, the default rule is still to grow adapters, harnesses, and evidence around the current semantic core rather than widening the kernel.

### 2. The Motivation Is Evidence-Backed

- the proposal names one concrete semantic family, one type boundary, or one execution boundary that the current kernel does not yet cover
- the need for that boundary is grounded in current conformance or differential evidence, an accepted spec gap, or a clearly blocked follow-on slice
- broad arguments such as "future parity," "likely needed later," or "better abstraction" are not enough by themselves

The kernel should grow because the documented evidence path has exposed the next smallest justified boundary, not because broader capability feels directionally useful.

### 3. Source-Of-Truth Docs Lead The Change

Before implementation starts, the follow-on issue should already know which source-of-truth docs must move.

At minimum, update or explicitly bound the relevant parts of:

- `docs/spec/` for semantics and edge cases
- `docs/spec/type-system.md` for typing, nullability, overflow, or coercion behavior
- `docs/contracts/data.md` when the data contract changes
- `docs/contracts/runtime.md` when scheduling, ownership, cancellation, or admission boundaries change
- adapter and harness docs when the new capability changes request, response, fixture, or inventory expectations

If one of those surfaces stays unchanged, the issue should say so explicitly instead of leaving reviewers to infer it.

### 4. Harness Coverage Is Defined Up Front

Every proposed kernel expansion should name the review path before code lands.

That means defining:

- the conformance cases that prove the new behavior locally
- any differential cases needed to compare the new behavior across engines
- any fixture or inventory artifact updates that reviewers should expect to inspect later

Local debug traces are not sufficient by themselves. If durable evidence belongs in git, the artifact family and refresh rule should already be documented.

### 5. Scope Stays Thin

One follow-on issue should widen one dimension at a time.

Acceptable scope shapes include one of these at a time:

- one operator or expression family
- one additional type boundary within an already-documented family
- one execution boundary that is required to make a documented slice real

The issue should also carry explicit non-goals so it does not silently turn into a generalized shared-kernel redesign.

### 6. Completion Evidence Is Concrete

The issue should define a completion signal that reviewers can check without relying on chat history.

That signal should name:

- which docs become the accepted source of truth for the new boundary
- which tests, fixtures, or differential artifacts prove the boundary works
- which kernel surface actually grows and which adjacent surfaces remain out of scope

If the issue cannot state those outcomes concretely, it is not yet ready to widen the kernel.

## What Does Not Qualify

These do not justify shared-kernel expansion on their own:

- adapter-local inconvenience or missing adapter plumbing
- local runtime or ownership fixtures that do not change shared semantics
- a desire to introduce broader shared kernel APIs before one slice requires them
- bundling multiple unrelated operator, type, or execution-boundary changes into one checkpoint

Those may still justify adapter, harness, or docs work, but they do not by themselves clear the gate for layer-3 growth.

## Resulting Workflow

1. finish the next thin end-to-end differential slice and collect reviewable evidence
2. open a docs-first issue for one narrowly scoped kernel expansion candidate
3. update the relevant spec, contract, adapter, and harness docs before code
4. implement the minimum kernel change that satisfies those docs and cases
5. land the resulting tests, fixtures, and inventory updates that prove the new boundary

Issue #82 fixed only the acceptance gate.

## Follow-On Boundary

The first post-gate boundary is now selected in `docs/design/first-post-gate-kernel-boundary.md`.

Issue #149 now provides the first executable local-kernel implementation and conformance evidence for that selected filter boundary. Later expansion issues should treat the milestone-1 expression-projection slice plus the first `is_not_null(column(index))` filter slice as the current accepted shared-kernel implementation boundaries.
