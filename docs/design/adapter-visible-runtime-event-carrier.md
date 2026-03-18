# Milestone-1 Adapter-Visible Runtime Event Carrier

Status: issue #121 design checkpoint

Verified: 2026-03-18

Related issues:

- #92 `design: define adapter-local runtime orchestration boundary`
- #121 `design: define adapter-visible runtime event carrier boundary`
- #135 `design: define adoption gate for shared runtime event streaming`

## Question

When adapter-visible integrations need runtime, admission, or ownership observability, should milestone 1 expose internal `LocalExecutionSnapshot` state directly, translate it into another carrier, or standardize a callback-style streaming API?

## Inputs Considered

- `docs/contracts/runtime.md`
- `docs/design/adapter-runtime-orchestration-boundary.md`
- `tests/differential/first-expression-slice-artifacts.md`
- issue #121

## Design Summary

Milestone 1 chooses a translation-first boundary:

- keep `LocalExecutionSnapshot` internal to local Rust execution paths
- expose adapter-visible runtime, admission, and ownership observations only through translated `LocalExecutionFixture`-shaped records with primitive payload fields
- keep this event carrier optional and sidecar-only for milestone 1; do not add required event fields to the first differential adapter request or `case result` response
- gate any shared callback or streaming event API behind `docs/design/runtime-event-streaming-adoption-gate.md`

This keeps event meanings stable without leaking internal Rust enum layouts or freezing a transport protocol too early.

## Carrier Decision

### 1. Internal versus adapter-visible shapes

`LocalExecutionSnapshot` remains an internal local carrier for kernel tests and harness scaffolding.

When runtime events cross an adapter-visible boundary, they should be translated into the already documented `LocalExecutionFixture` event shape:

- top-level `admission_events[]` and `runtime_events[]`
- each record uses `event` plus only the primitive payload keys that apply
- non-applicable optional fields are omitted rather than emitted as `null`

### 2. Attachment boundary

For milestone 1, this translated fixture is optional sidecar evidence.

It does not alter the required first-slice adapter contract:

- shared request fields remain `slice_id`, `case_id`, `spec_refs[]`, `input_ref`, and `projection_ref`
- shared `case result` fields remain unchanged

### 3. Callback boundary

Milestone 1 does not define a shared callback or streaming event API for adapters.

Adapters may still collect local diagnostics however they need, but callback transport, lifecycle, ordering guarantees, and backpressure semantics are deferred until a later issue has a concrete cross-adapter need.
Any follow-on shared callback or streaming proposal should satisfy the adoption gate in `docs/design/runtime-event-streaming-adoption-gate.md`.

## Why This Boundary

- preserves one stable event vocabulary already fixed in the runtime contract
- avoids exposing Rust-specific internal snapshot structures through adapter-facing surfaces
- keeps first-slice adapter and differential artifacts focused on semantic case outcomes
- leaves room for a later streaming design without forcing premature protocol choices

## Follow-On Boundary

Later issues may extend this checkpoint to define:

- when fixture-style sidecar export is required instead of optional
- the first slice that satisfies `docs/design/runtime-event-streaming-adoption-gate.md` for shared callback or streaming
- how any future event stream should align with current `consumer_*`, `batch_*`, and terminal outcome meanings without redefining them
