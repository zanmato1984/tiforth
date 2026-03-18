# Runtime Event Streaming Adoption Gate

Status: issue #135 design checkpoint

Verified: 2026-03-18

Related issues:

- #121 `design: define adapter-visible runtime event carrier boundary`
- #135 `design: define adoption gate for shared runtime event streaming`

## Question

After milestone-1 fixture translation, when should `tiforth` add a shared callback or streaming runtime event surface?

## Inputs Considered

- `docs/contracts/runtime.md`
- `docs/design/adapter-visible-runtime-event-carrier.md`
- `tests/differential/first-expression-slice-artifacts.md`
- issue #135

## Design Summary

Milestone 1 keeps fixture-style translation as the only shared adapter-visible event carrier.

Any shared callback or streaming event surface is out of scope until a follow-on issue satisfies this adoption gate:

- a concrete cross-adapter or harness use case cannot be met by optional `LocalExecutionFixture` sidecar evidence
- that use case requires incremental delivery before query completion rather than post-run artifact capture
- the same requirement applies to more than one adapter or harness path, so a shared contract is justified

If those triggers are not met, event streaming remains adapter-local and out of shared-contract scope.

## Required Contract Checkpoints

Before a shared callback or streaming event surface can be accepted, a docs-first follow-on issue must define all of these:

1. event payload and stability rules:
   - which event families are in scope
   - required fields versus optional extension fields
   - versioning or compatibility expectations
2. ordering model:
   - per-stream ordering guarantees
   - whether any cross-family merged ordering is guaranteed
3. lifecycle semantics:
   - stream start and terminal conditions
   - completion versus cancellation versus error termination behavior
4. flow-control semantics:
   - backpressure behavior and limits
   - behavior when consumer callbacks are slow, absent, or fail
5. coverage expectations:
   - conformance or differential cases proving the new contract
   - fixture or artifact updates needed to keep review evidence stable

A proposal that omits any checkpoint above should not widen the shared runtime contract.

## Milestone-1 Boundary

For milestone 1:

- `LocalExecutionSnapshot` stays internal to local Rust execution
- adapter-visible event evidence stays `LocalExecutionFixture`-shaped translation
- event evidence remains optional sidecar output
- callback-oriented event streaming is not part of the shared runtime contract

## Follow-On Boundary

The first issue that satisfies this gate should:

- name the first slice that requires shared streaming
- define the contract checkpoints above in source-of-truth docs before implementation
- update `docs/contracts/runtime.md` and adapter-facing slice docs in the same change set
