# Milestone-1 Adapter-Local Runtime Orchestration Boundary

Status: issue #92 design checkpoint, issue #121 event-carrier boundary checkpoint

Verified: 2026-03-18

Related issues:

- #72 `design: define first differential adapter request/response surface`
- #80 `design: define the next thin end-to-end slice after milestone-1 projection`
- #86 `design: define milestone-1 operator and expression attachment to adopted runtime contract`
- #92 `design: define adapter-local runtime orchestration boundary`
- #121 `design: define adapter-visible runtime event carrier boundary`

## Question

Which runtime and orchestration concerns should remain adapter-local for milestone 1 instead of entering `tiforth`'s shared runtime contract or the first differential adapter carrier?

## Inputs Considered

- `docs/contracts/runtime.md`
- `adapters/first-expression-slice.md`
- `docs/design/next-thin-end-to-end-slice.md`
- `docs/design/adapter-milestone-breakdown.md`
- `adapters/tidb/README.md`
- `adapters/tiflash/README.md`
- `adapters/tikv/README.md`
- issue #92

## Design Summary

For milestone 1, shared docs stop at stable case identifiers, normalized request and response fields, shared meanings for `tiforth` runtime, admission, and ownership events, and a fixture-translation rule for adapter-visible event export.

Everything required to drive one engine invocation stays adapter-local: session setup, query derivation, timeout and retry policy, cancellation transport, concurrency choices, plan capture, and environment wiring.

Adapters may translate shared case refs into engine-native execution however they need, but they must return the documented normalized `case result` carrier, follow the fixture-translation boundary in `docs/design/adapter-visible-runtime-event-carrier.md` when exposing local runtime events, and must not redefine shared semantic IDs or error-class meanings.

## Shared Boundary Still Owns

- `slice_id`, `case_id`, `input_ref`, `projection_ref`, and `spec_refs[]`
- the semantic meaning of each documented first-slice case
- normalized `case result` fields and the first-slice error vocabulary
- the distinction between `adapter_unavailable`, `arithmetic_overflow`, and `engine_error`
- shared meanings for `tiforth` runtime, admission, and ownership events where the local kernel path is involved
- the milestone-1 rule that adapter-visible event export uses translated `LocalExecutionFixture`-shaped records instead of exposing internal `LocalExecutionSnapshot` state directly

## Adapter-Local Orchestration Owns

- connection, authentication, DSN, and environment provisioning details
- engine session setup, planner hints, and session-variable choices
- engine-native SQL or expression construction derived from stable shared refs
- request batching, concurrency, retry, backoff, and failover policy
- timeout policy, cancellation transport, interrupt delivery, and cleanup or reconnect logic
- buffering, streaming, pagination, or result-materialization strategy inside one adapter
- engine-plan capture, diagnostics gathering, and local debug traces
- translation from engine-native failures into optional `engine_code` and `engine_message` fields

## Boundary Rules

- adapter-local choices may change how one engine is contacted or observed, but they must not change the meaning of shared case definitions or normalized result fields
- shared docs may require an explicit `adapter_unavailable` result when an adapter cannot execute a documented case, but they do not require one transport, protocol, or session shape
- local scheduler helpers, thread pools, async runtimes, or test harness wrappers remain adapter implementation details until a later accepted issue promotes them into a shared boundary
- adapter-local runtime data may be captured for debugging, but the first differential checkpoint still checks in only normalized `case result` and `drift-report` artifacts
- when adapter-visible integrations expose local runtime, admission, or ownership events in milestone 1, they should publish translated fixture-style sidecar payloads rather than a shared callback stream

## Why This Boundary Holds For Milestone 1

- it keeps the first executable differential slice reviewable one adapter at a time
- it prevents shared docs from freezing deployment and environment details before any adapter path is executable
- it leaves room for TiDB, TiFlash, and later TiKV bridges to use different execution mechanisms while still comparing one shared semantic surface
- it matches the runtime-contract goal of adopting upstream task semantics without turning engine transport into a shared runtime protocol

## Deferred Questions

- whether later milestones need shared session profiles or capability advertisement
- whether timeout or cancellation semantics should become part of a shared adapter boundary
- whether engine-plan capture or execution traces deserve a normalized artifact family
- whether a later milestone needs one shared callback or streaming event API after the current fixture-style sidecar boundary

## Follow-On Boundary

Later issues may extend the shared adapter or runtime docs to cover:

- reusable session profiles or capability discovery
- adapter-visible plan or diagnostic artifacts
- shared timeout or cancellation expectations
- a shared callback or streaming event surface if fixture-style sidecar export becomes insufficient
- a TiKV-specific adapter boundary after the TiDB-versus-TiFlash pairwise checkpoint exists

Until then, adapter work should treat orchestration as adapter-local implementation space bounded only by the current request and response carrier plus the normalized evidence requirements.
